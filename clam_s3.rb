#!/usr/bin/env ruby

require 'optparse'
require 'yaml'
require 'sqlite3'
require 'aws/s3'

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
    unless @dry_run
      @db = SQLite3::Database.new(YAML.load_file(options[:conf_file])['database'])
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
      AWS::S3::Base.establish_connection!(
        :access_key_id     => options[:access_key_id],
        :secret_access_key => options[:secret_access_key],
        :use_ssl => true
      )
      @bucket = AWS::S3::Bucket.find(options[:bucket])
    end

  end

  def inject!
    count = 0
    @bucket.each do |obj|
      count += 1
      log("# %06d: %s" % [count, obj.inspect], false)
      if asset_exists?(obj)
        log(' exists!', false)
      else
        @db.execute("INSERT INTO amazon_assets (aws_key, bucket, size, md5) VALUES ('%s', '%s', '%s', '%s');" % [obj.key, @bucket.name, obj.size, obj.etag])
      end
      log('')
      break if count >= 100
    end
  end

  private

  def asset_exists?(s3_obj)
    rows = @db.execute <<-SQL
      SELECT aws_key, bucket, size, md5
      FROM amazon_assets
      WHERE (aws_key = '#{s3_obj.key}' AND bucket = '#{@bucket.name}' AND size = '#{s3_obj.size}' AND md5 = '#{s3_obj.etag}');
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
  opt.on('-b', '--bucket NAME', 'bucket name') { |name| options[:bucket] = name }

end.parse!

c = ClamS3.new(options)
c.inject!
puts 'done'
