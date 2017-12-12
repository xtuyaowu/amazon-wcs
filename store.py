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
            #self.conn = pymysql.connect(**MYSQL_CONFIG_LOCAL)
            self.conn = pymysql.connect(**MYSQL_CONFIG_SERVER)
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


class AmazonStore:
    def __init__(self):
        try:
            #self.conn = pymysql.connect(**MYSQL_CONFIG_LOCAL)
            self.conn = pymysql.connect(**MYSQL_CONFIG_SERVER)
        except:
            traceback.print_exc()
            sys.exit()

    def insert_amazon_sku(self, table_name, _uuid, products_id, url_id, brand, product_url, _name, first_title,
                          second_title, original_price, price, max_price, discount, dispatch, shipping, currency,
                          attribute, version_urls, review_count, grade_count, sales_total, total_inventory, favornum,
                          image_url, extra_image_urls, description, category, category_url, tags, shop_name, shop_url,
                          generation_time, platform, platform_url, crawl_time, create_time, status, questions,
                          is_delete, reserve_field_1, reserve_field_2, reserve_field_3, reserve_field_4,
                          reserve_field_5, reserve_field_6, reserve_field_7):
        sql = (
            "insert into {}(scgs_uuid, scgs_products_id, scgs_url_id, scgs_brand, scgs_product_url, scgs_name,"
            "scgs_firstTitle, scgs_secondTitle, scgs_original_price, scgs_price, scgs_max_price, scgs_discount," 
            "scgs_dispatch, scgs_shipping, scgs_currency, scgs_attribute, scgs_version_urls, scgs_review_count," 
            "scgs_grade_count, scgs_sales_total, scgs_total_inventory, scgs_favornum, scgs_image_url," 
            "scgs_extra_image_urls, scgs_description, scgs_category, scgs_category_url, scgs_tags, scgs_shop_name, " 
            "scgs_shop_url, scgs_generation_time, scgs_platform, scgs_platform_url, scgs_crawl_time, scgs_create_time, " 
            "scgs_status, scgs_questions, scgs_is_delete, scgs_reserve_field_1, scgs_reserve_field_2," 
            "scgs_reserve_field_3, scgs_reserve_field_4, scgs_reserve_field_5, scgs_reserve_field_6,"
            "scgs_reserve_field_7)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(table_name)
        cur = self.conn.cursor()
        cur.execute(sql, (_uuid, products_id, url_id, brand, product_url, _name, first_title, second_title,
                          original_price, price, max_price, discount, dispatch, shipping, currency, attribute,
                          version_urls, review_count, grade_count, sales_total, total_inventory, favornum, image_url,
                          extra_image_urls, description, category, category_url, tags, shop_name, shop_url,
                          generation_time, platform, platform_url, crawl_time, create_time, status, questions,
                          is_delete, reserve_field_1, reserve_field_2, reserve_field_3, reserve_field_4,
                          reserve_field_5, reserve_field_6, reserve_field_7))
        self.conn.commit()

    def insert_wcs_task_relevance(self, sku_uuid, sku_rank, sku_url, task_id, task_type, task_info, platform):
        sql = (
            "insert into crawler_wcs_task_relevance(wtr_sku_uuid, wtr_sku_rank, wtr_sku_url, wtr_task_id,"
            "wtr_task_type, wtr_task_info, wtr_platform, wtr_crawl_time, wtr_create_time)values"
            "(%s, %s,%s,%s,%s,%s,%s,curdate(),now())")
        cur = self.conn.cursor()
        cur.execute(sql, (sku_uuid, sku_rank, sku_url, task_id, task_type, task_info, platform))
        self.conn.commit()

    def close(self):
        cursor = self.conn.cursor()
        cursor.close()
        self.conn.close()


if __name__ == '__main__':
    pass

