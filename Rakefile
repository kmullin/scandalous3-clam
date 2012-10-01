require 'yaml'
require 'sqlite3'
require 'redis'

# Redis data model
# string "scandalous3-clam:bucket_id", 1
# list "scandalous3-clam:unscanned_assets:bucket_id"
# string "scandalous3-clam:total_scanned", 0
#

desc "testing"
task :migrate_to_redis do
  database = YAML.load_file(File.join(File.dirname(__FILE__), 'config/settings.yml'))['database']
  db = SQLite3::Database.new(database)
  redis = Redis.new(:host => '127.0.0.1')
  rows = db.execute("select * from amazon_assets");
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
    redis_key = [bucket, md5, aws_key].join('|')

    if is_virus.nil? and scanned_date.nil?
      statement = "LPUSH unscanned_assets #{redis_key}\n"
    else
      statement = "HMSET #{redis_key} size #{size} is_virus #{is_virus} scanned_date #{scanned_date}\n"
    end
    f.write(statement)
    puts count
  end
  f.close
end
