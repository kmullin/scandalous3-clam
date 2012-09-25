#!/usr/bin/env ruby

require 'bundler/setup'
require 'optparse'
require 'yaml'
require 'sqlite3'
require 'aws/s3'
require 'tempfile'
require 'clamav'

class ClamS3

  def initialize(options={})
    options[:conf_file] ||= 'config/settings.yml'
    options.merge!(YAML.load_file(options[:conf_file])['amazon'])
    options.keys.each do |key|
      options[(key.to_sym rescue key) || key] = options.delete(key)
    end
    @dry_run = options[:dry_run] || false
    @verbose = options[:verbose] || false
    @debug = options[:debug] || false
    database = YAML.load_file(options[:conf_file])['database']
    unless @dry_run
      @clamav = ClamAV.instance
      log("Loading ClamAV Database... ", false)
      @clamav.loaddb
      log("done!")
      log("Loading sqlite3 database #{database}... ", false)
      @db = SQLite3::Database.new(database)
      @db.execute <<-SQL
        create table if not exists amazon_assets (
          aws_key varchar(255),
          bucket varchar(255),
          size integer,
          md5 varchar(32),
          is_virus integer,
          checked_date datetime,
          PRIMARY KEY (aws_key, bucket)
        );
      SQL
      log("done!")
      log("Establishing S3 connection... ", false)
      AWS::S3::Base.establish_connection!(
        :access_key_id     => options[:access_key_id],
        :secret_access_key => options[:secret_access_key],
        :use_ssl => true
      )
      log("done!")
      log("Finding S3 bucket #{options[:bucket]}... ", false)
      @bucket = AWS::S3::Bucket.find(options[:bucket])
      log("done!")
    end

  end

  def inject!
    count = 0
    @bucket.objects(:marker => get_last_asset_key).each do |obj|
      count += 1
      log("# %06d: %s" % [count, obj.inspect], false)
      if asset_exists?(obj)
        log(' exists!', false)
      else
        @db.execute("insert into amazon_assets (aws_key, bucket, size, md5) values ('%s', '%s', '%s', '%s');" % [obj.key, obj.bucket.name, obj.size, obj.etag])
      end
      log('')
      break if count >= 100
    end
  end

  def start_scan!
    # start a thread to walk db keys
  end

  private

  def scan_file(s3_obj)
    tempfile = Tempfile.new(File.basename(s3_obj.key))
    AWS::S3::S3Object.stream(s3_obj.key, s3_obj.bucket.name) do |chunk|
      tempfile.write(chunk) # chunk.size works (progress bar?)
    end
    tempfile.close
    result = @clamav.scanfile(tempfile.path)
    tempfile.unlink
    is_virus = result == 0 ? false : true
    if is_virus
      @db.execute <<-SQL
        update amazon_assets
        set (is_virus = #{is_virus ? 0 : 1}, scanned_date = '#{Time.now.utc}')
        where (aws_key = '#{s3_obj.key}' and bucket = '#{s3_obj.bucket.name}' and size = '#{s3_obj.size}' and md5 = '#{s3_obj.etag}');
      SQL
    end
    is_virus
  end

  def get_last_scanned_asset
    rows = @db.execute("select min(aws_key) from amazon_assets where bucket = '#{@bucket.name}' and is_virus is null;")
    rows.empty? ? nil : rows[0][0]
  end

  def get_last_asset_key
    rows = @db.execute("select max(aws_key) from amazon_assets where bucket = '#{@bucket.name}';")
    rows.empty? ? nil : rows[0][0]
  end

  def asset_exists?(s3_obj)
    rows = @db.execute <<-SQL
      select aws_key, bucket, size, md5
      from amazon_assets
      where (aws_key = '#{s3_obj.key}' and bucket = '#{s3_obj.bucket.name}' and size = '#{s3_obj.size}' and md5 = '#{s3_obj.etag}');
    SQL
    ! rows.empty?
  end

  def asset_scanned?(s3_obj)
    rows = @db.execute <<-SQL
      select is_virus, checked_date
      from amazon_assets
      where (aws_key = '#{s3_obj.key}' and bucket = '#{s3_obj.bucket.name}' and size = '#{s3_obj.size}' and md5 = '#{s3_obj.etag}');
    SQL
    ! rows.first.compact.empty?
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
  opt.on('-b', '--bucket NAME', 'bucket name') { |name| options[:bucket] = name }

end.parse!

c = ClamS3.new(options)
c.inject!
puts 'done'
