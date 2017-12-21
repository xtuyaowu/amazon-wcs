# -*- coding: utf-8 -*-
import sys
import traceback
import pymysql
from rediscluster import StrictRedisCluster
from settings import MYSQL_CONFIG_LOCAL, MYSQL_CONFIG_SERVER, StartupNodesLocal, StartupNodesServer


class RedisCluster:

    def __init__(self):
        try:
            self.rc = StrictRedisCluster(startup_nodes=StartupNodesServer, decode_responses=True)
        except:
            traceback.print_exc()

    def count_keys(self):   # 查询当前库里有多少key
        return self.rc.dbsize()

    def exists_key(self, key):
        return self.rc.exists(key)

    def delete_key(self, key):
        self.rc.delete(key)

    def rename_key(self, key1, key2):
        self.rc.rename(key1, key2)

    # String操作
    def set_key_value(self, key, value):
        self.rc.set(key, value)

    def get_key_value(self, key):   # 没有对应key返回None
        return self.rc.get(key)

    # Hash操作
    def set_hash(self, key, mapping):   # mapping为字典, 已存在key会覆盖mapping
        self.rc.hmset(key, mapping)

    def delete_hash_field(self, key, field):   # 删除hash表中某个字段，无论字段是否存在
        self.rc.hdel(key, field)

    def exists_hash_field(self, key, field):   # 检查hash表中某个字段存在
        return self.rc.hexists(key, field)

    def get_hash_field(self, key, field):   # 获取hash表中指定字段的值, 没有返回None
        return self.rc.hget(key, field)

    def get_hash_all_field(self, key):   # 获取hash表中指定key所有字段和值,以字典形式，没有key返回空字典
        return self.rc.hgetall(key)

    def increase_hash_field(self, key, field, increment):   # 为hash表key某个字段的整数型值增加increment
        self.rc.hincrby(key, field, increment)

    # List操作
    def rpush_into_lst(self, key, value):  # url从头至尾入列
        self.rc.rpush(key, value)

    def lpush_into_lst(self, key, value):  # url从尾至头入列
        self.rc.lpush(key, value)

    def lpop_lst_item(self, key):  # 从头取出列表第一个元素，没有返回None
        return self.rc.lpop(key)

    def blpop_lst_item(self, key):  # 从头取出列表第一个元素(元组形式,值为元祖[1], 元祖[0]为key名)，并设置超时，超时返回None
        return self.rc.blpop(key, timeout=1)

    def rpop_lst_item(self, key):  # 从尾取出列表最后一个元素，没有返回None
        return self.rc.rpop(key)

    def brpop_lst_item(self, key):  # 从尾取出列表最后一个元素(元组形式,值为元祖[1], 元祖[0]为key名)，并设置超时，超时返回None
        return self.rc.brpop(key, timeout=1)

    # Set操作
    def add_set(self, key, value):
        self.rc.sadd(key, value)

    def is_member(self, key, value):
        return self.rc.sismember(key, value)

    def pop_member(self, key):  # 随机移除一个值并返回该值,没有返回None
        return self.rc.spop(key)

    def pop_members(self, key, num):  # 随机取出num个值（非移除），列表形式返回这些值，没有返回空列表
        return self.rc.srandmember(key, num)

    def remove_member(self, key, value):   # 移除集合中指定元素
        self.rc.srem(key, value)

    def get_all_members(self, key):   # 返回集合中全部元素,不删除
        return self.rc.smembers(key)

    def remove_into(self, key1, key2, value):   # 把集合key1中value元素移入集合key2中
        self.rc.smove(key1, key2, value)

    def count_members(self, key):   # 计算集合中成员数量
        return self.rc.scard(key)


class AmazonStorePro:
    def __init__(self):
        try:
            self.conn = pymysql.connect(**MYSQL_CONFIG_LOCAL)
            #self.conn = pymysql.connect(**MYSQL_CONFIG_SERVER)
        except:
            traceback.print_exc()
            sys.exit()

    def execute_sql(self, _sql, *args):
        if 'select' in _sql:
            cur = self.conn.cursor()
            cur.execute(_sql, args)
            return cur.fetchall()
        else:
            if 'insert' in _sql or 'where' in _sql:
                cur = self.conn.cursor()
                cur.execute(_sql, args)
                self.conn.commit()
            else:
                print('no where')

    def close(self):
        cursor = self.conn.cursor()
        cursor.close()
        self.conn.close()


if __name__ == '__main__':
    pass

