from store import RedisCluster
from configuration_file import ErrorUrls, StartUrls

RedisA = RedisCluster()
members = RedisA.get_all_members(ErrorUrls)
for member in members:
    print(member)
    RedisA.rc.lpush(StartUrls, member)
RedisA.delete_key(ErrorUrls)
