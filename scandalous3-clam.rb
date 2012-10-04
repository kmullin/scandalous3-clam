#!/usr/bin/env ruby

require 'bundler/setup'
require 'optparse'
require 'yaml'
require 'sqlite3'
require 'aws/s3'
require 'tempfile'
require 'clamav'
require 'thread'

class ClamS3

  attr_accessor :threads

  def initialize(options={})
    options[:conf_file] ||= 'config/settings.yml'
    yaml_file = YAML.load_file(options[:conf_file])
    options.merge!(yaml_file['amazon'])
    options.keys.each do |key|
      options[(key.to_sym rescue key) || key] = options.delete(key)
    end
    @dry_run = options[:dry_run] || false
    @verbose = options[:verbose] || false
    @debug = options[:debug] || false
    @max_threads = options[:max_threads] || 5+2 # workers + control
    database = yaml_file['database']
    log options
    unless @dry_run
      @clamav = ClamAV.instance
      log("Loading ClamAV Database... ", false)
      @clamav.loaddb
      log("done!")
      log("Loading sqlite3 database #{database}... ", false)
      @db = SQLite3::Database.new(database)
      @db.busy_timeout = 3 * 1000 # 3 seconds
      @db.execute <<-SQL
        create table if not exists amazon_assets (
          aws_key varchar(255),
          bucket varchar(255),
          size integer,
          md5 varchar(32),
          is_virus integer,
          scanned_date datetime,
          PRIMARY KEY (aws_key, bucket)
        );
      SQL
      log("done!")
      log("Establishing S3 connection... ", false)
      @S3 = AWS::S3.new(
        :access_key_id     => options[:access_key_id],
        :secret_access_key => options[:secret_access_key],
        :use_ssl => true
      )
      log("done!")
      log("Finding S3 bucket #{options[:bucket]}... ", false)
      @bucket = @S3.buckets[options[:bucket]]
      log("done!")
    end
    @queue = Queue.new
    @mutex = Mutex.new
    @files_scanned = 0
  end

  def inject!
    count = 0
    @bucket.objects.each(:next_token => {:marker => get_last_asset_key}) do |obj|
      count += 1
      log("# %06d: %s" % [count, obj.inspect])
      unless asset_exists?(obj)
        @db.execute("insert into amazon_assets (aws_key, bucket, size, md5) values ('%s', '%s', '%s', %s);" % [obj.key, obj.bucket.name, obj.content_length, obj.etag])
      end
    end
  end

  def start_scan!
    @threads = []
    trap('INT') { log("Scanned #{@files_scanned} files."); @threads.dup.each {|t| Thread.kill(t); t.join; @threads.delete(t) } }
    trap('HUP') { log("ClamAV Reloading"); @clamav.reload }
    @threads << Thread.new do
      loop do
        @queue.clear
        get_unscanned_assets.each do |aws_key|
          @queue.push(@bucket.objects[aws_key])
        end
        $0 = "ScandalouS3 [Q:#{@queue.size} T:#{@threads.size} S:#{@files_scanned}/#{total_scanned}]"
        sleep 5
      end
    end
    @threads << Thread.new do
      loop do
        if @queue.size < 100
          inject!
        else
          sleep 5
        end
      end
    end
    (@max_threads - @threads.size).times do |i|
      @threads << Thread.new do
        loop do
          s3_obj = @queue.pop
          log("----- # %02d SCANNING #{s3_obj.key} #{s3_obj.content_length}" % i)
          scan_file(s3_obj)
        end
      end
    end
  end

  def total_scanned
    rows = @db.execute("select count(*) from amazon_assets where is_virus is not null;")
    rows.empty? ? nil : rows[0][0]
  end

  private

  def scan_file(s3_obj)
    tempfile = Tempfile.open(File.basename(s3_obj.key))
    s3_obj.read do |chunk|
      tempfile.write(chunk) # chunk.size works (progress bar?)
    end
    tempfile.close
    result = @clamav.scanfile(tempfile.path)
    tempfile.unlink
    unless result.nil?
      @db.execute <<-SQL
        update amazon_assets
        set is_virus = #{result == 0 ? 0 : 1}, scanned_date = '#{Time.now.utc}'
        where (aws_key like '%#{File.basename(s3_obj.key)}' and bucket = '#{s3_obj.bucket.name}' and size = '#{s3_obj.content_length}' and md5 = #{s3_obj.etag});
      SQL
      @mutex.synchronize { @files_scanned += 1 }
    end
    ! result.nil?
  end

  def get_unscanned_assets
    rows = @db.execute("select aws_key from amazon_assets where (bucket = '#{@bucket.name}' and is_virus is null) group by md5;")
    rows.flatten
  end

  def get_last_asset_key
    rows = @db.execute("select max(aws_key) from amazon_assets where bucket = '#{@bucket.name}';")
    rows.empty? ? nil : rows[0][0]
  end

  def asset_exists?(s3_obj)
    rows = @db.execute <<-SQL
      select aws_key
      from amazon_assets
      where (aws_key = '#{s3_obj.key}' and bucket = '#{s3_obj.bucket.name}' and size = '#{s3_obj.content_length}' and md5 = #{s3_obj.etag});
    SQL
    ! rows.empty?
  end

  def log(msg, newline=true)
    return unless @verbose
    if newline
      puts msg
    else
      print msg
    end
  end

end

options = {}
OptionParser.new do |opt|

  opt.banner = "Usage: #{File.basename($0)} [options]"
  opt.separator ""
  opt.separator "Options:"

  opt.on('-v', '--verbose', 'Verbose') { options[:verbose] = true }
  opt.on('-d', '--debug', 'Debug') { options[:debug] = true }
  opt.on('-n', '--dry-run', 'Dry run') { options[:dry_run] = true }
  opt.on('-t', '--total-scanned', 'Print how many scanned') { puts "-t not implemented yet" } # do nothing for now
  opt.on('-b', '--bucket NAME', 'bucket name') { |name| options[:bucket] = name }
  opt.on('-m', '--max-threads NUM', Integer, 'max threads (def. 5 + 2)') { |num| options[:max_threads] = num + 2 }

end.parse!

c = ClamS3.new(options)
c.start_scan!
while c.threads.size > 0
  sleep 0.1
end
puts 'done'
