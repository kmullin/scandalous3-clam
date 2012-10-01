require 'yaml'
require 'sqlite3'
require 'redis'

# Redis data model
# PREFIX scandalous3-clam
# hash "prefix:assets:bucket_id:md5:aws_key"
# string "prefix:buckets:name", 1
# list "prefix:unscanned_assets:bucket_id"
# list "prefix:
# string "scandalous3-clam:total_scanned", 0
# hash "scan
#

desc "testing"
task :migrate_to_redis do
  database = YAML.load_file(File.join(File.dirname(__FILE__), 'config/settings.yml'))['database']
  db = SQLite3::Database.new(database)
  redis = Redis.new(:host => '127.0.0.1')
  rows = db.execute("select * from amazon_assets");
  prefix = "scandalous3-clam"
  # create bucket_id
  redis.setnx([prefix, 'bucket_id'].join('|'), 0)
  redis.incr([prefix, 'bucket_id'].join('|'))
  count = 0
  f = open('redis.cmd', 'wb')
  rows.each do |row|
    count += 1
    aws_key = row[0]
    bucket = row[1]
    size = row[2]
    md5 = row[3]
    is_virus = row[4]
    scanned_date = row[5]

    bucket_id = redis.get([prefix, 'bucket', bucket].join('|'))
    if bucket_id.nil?
      # create new bucket id
      bucket_id = redis.incr([prefix, 'bucket_id'].join('|'))
      bucket_id = redis.set([prefix, 'bucket', bucket].join('|'), bucket_id)
    end
    asset_key = [prefix, 'assets', bucket_id, md5, aws_key].join('|')

    if is_virus.nil? and scanned_date.nil?
      statement = "LPUSH unscanned_assets #{asset_key}\n"
    else
      statement = "HMSET #{asset_key} size #{size} is_virus #{is_virus} scanned_date #{scanned_date}\n"
    end
    f.write(statement)
    puts count
  end
  f.close
end
