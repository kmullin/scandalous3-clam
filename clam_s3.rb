#!/usr/bin/env ruby

require 'optparse'
require 'yaml'
require 'aws/s3'

class ClamS3

  def initialize(options={})
    options.merge!(YAML.load_file('config/amazon.yml'))
    options.keys.each do |key|
      options[(key.to_sym rescue key) || key] = options.delete(key)
    end
    @dry_run = options[:dry_run] || false
    @verbose = options[:verbose] || false
    @debug = options[:debug] || false
    unless @dry_run
      AWS::S3::Base.establish_connection!(
        :access_key_id     => options[:access_key_id],
        :secret_access_key => options[:secret_access_key],
        :use_ssl => true
      )
      @bucket = AWS::S3::Bucket.find(options[:bucket])
    end
  end

  def size
    @bucket.size unless @dry_run
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
puts c.size
