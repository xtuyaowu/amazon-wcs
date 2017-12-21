import datetime
import time
import uuid
import queue
from configuration_file import (
    StartUrls, OneTask, DetailTag, ListTag, KeyWordTag, BestSellersTag, NewReleasesTag, RedisSpace, WaitSec, SearchBox,
    Temp, ProxyIpFailTimes)
from store import RedisCluster, AmazonStorePro, AmazonStore
from post_data import PostData
from get_proxy_ip import get_proxy_ip

RedisA = RedisCluster()
Que = queue.Queue()


def update_proxy_ip(que):
    try:
        for n in range(que.qsize()):
            que.get_nowait()
    except queue.Empty:
        pass
    for ip_proxy in get_proxy_ip():
        que.put({'ip': ip_proxy, 'num': ProxyIpFailTimes})
    print('update proxy ip')


def get_last_id(table):
    store = AmazonStorePro()
    sql_select = "select scgs_id from {} order by scgs_id desc LIMIT 1".format(table)
    row = store.execute_sql(sql_select)
    if row:
        row_id = row[0]['scgs_id']
        print(row_id)
    store.close()


def change_status(status, task_id):
    if Temp:
        pd = PostData()
        pd.update(wtcPlatform="amazon", wtcStatus=status, wtcId=task_id)
    else:
        store = AmazonStorePro()
        sql_update_status = "update crawler_wcs_task_center set wtc_status=%s, wtc_crawl_time=now() where wtc_id=%s"
        store.execute_sql(sql_update_status, status, task_id)
        store.close()


def scan_database():
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    flag = False
    store = AmazonStorePro()
    pd = PostData()
    if Temp:
        rows_0 = pd.select(wtcPlatform="amazon", wtcStatus=0, limit=1)
    else:
        sql_select = (
            "select wtc_id, wtc_task_type, wtc_task_frequency, wtc_task_period, wtc_task_info,wtc_task_category," 
            "wtc_task_product_id, wtc_task_site from crawler_wcs_task_center where wtc_status=%s and wtc_platform=%s"
            "and wtc_is_delete=%s limit 1")
        rows_0 = store.execute_sql(sql_select, 0, 'amazon', 0)

    if rows_0:
        row_dct = rows_0[0]
        print(row_dct)
        if Temp:
            task_id = row_dct['wtcId']
            try:
                task_type = row_dct['wtcTaskType']
                task_frequency = row_dct['wtcTaskFrequency']
                task_period = row_dct['wtcTaskPeriod']
                task_info = row_dct['wtcTaskInfo']
                task_category = row_dct['wtcTaskCategory']
                task_asin = row_dct['wtcTaskProductId']
                task_site = row_dct['wtcTaskSite']
            except KeyError:
                print("KeyError")
                change_status(-1, task_id)
                return
        else:
            task_id = row_dct['wtc_id']
            task_type = row_dct['wtc_task_type']
            task_frequency = row_dct['wtc_task_frequency']
            task_period = row_dct['wtc_task_period']
            task_info = row_dct['wtc_task_info']
            task_category = row_dct['wtc_task_category']
            task_asin = row_dct['wtc_task_product_id']
            task_site = row_dct['wtc_task_site']

        if task_type == DetailTag:   # 1
            if not (task_asin and task_site):
                change_status(-1, task_id)
                return
            task_asin = task_asin.strip()
            task_site = task_site.strip()
            _uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, task_asin + task_site)).replace('-', '')
            product_url = 'https://www.amazon.{}/dp/{}'.format(task_site, task_asin)
            amazon_store = AmazonStore()
            amazon_store.insert_wcs_task_relevance(_uuid, 0, product_url, task_id, DetailTag, '', 'amazon')
            amazon_store.close()
            page_url = 'https://www.amazon.{}/dp/{}'.format(task_site, task_asin)
        elif task_type == KeyWordTag:   # 3
            if not (task_site and task_info) or (task_info.strip().startswith('http')):
                change_status(-1, task_id)
                return
            task_site = task_site.strip()
            task_info = task_info.strip()
            keyword = '+'.join(task_info.split())
            if task_category:
                task_category = task_category.strip()
                search_box = SearchBox.get(task_category, None)
                if search_box:
                    page_url = 'https://www.amazon.{}/s/?url=search-alias%3D{}&field-keywords={}'.format(task_site, search_box, keyword)
                else:
                    change_status(-1, task_id)
                    return
            else:
                page_url = 'https://www.amazon.{}/s/?url=search-alias%3Daps&field-keywords={}'.format(task_site, keyword)
        elif task_type in (ListTag, BestSellersTag, NewReleasesTag):   # 2,4,5
            if not task_info or (not task_info.strip().startswith('http')):
                change_status(-1, task_id)
                return
            if "keywords" in task_info:
                change_status(-1, task_id)
                return
            page_url = task_info.strip()
        else:
            change_status(-1, task_id)
            return
        mp = {'entry': task_type, 'page_url': page_url, 'task_id': task_id}
        if task_category:
            if task_type in (BestSellersTag, NewReleasesTag):
                mp['task_category'] = task_category.strip()
            if task_type == KeyWordTag:
                mp['search_box'] = task_category.strip()
        # 单次采集
        if task_frequency == task_period == 1:
            RedisA.set_hash(OneTask, {'is_track': 0, 'task_id': task_id})
        # 循环采集首次
        elif task_period > task_frequency and task_type in (BestSellersTag, NewReleasesTag):
            RedisA.set_hash(OneTask, {'is_track': 1, 'task_id': task_id})
            key = RedisSpace + str(task_id)
            now_time = time.strftime("%Y-%m-%d %H:%M:%S")
            RedisA.set_hash(key, {'start_track_time': now_time, 'last_track_time': now_time})
        else:
            change_status(-1, task_id)
            return
        change_status(1, task_id)
        RedisA.rc.rpush(StartUrls, mp)
        # 昊旻测试用
        RedisA.rc.rpush('amz_test', mp)
        update_proxy_ip(Que)
    else:
        # 循环采集
        if Temp:
            rows_1 = pd.select(wtcPlatform="amazon", wtcStatus=1, limit=1000)
        else:
            sql_select_track = (
                "select wtc_id, wtc_task_type, wtc_task_frequency, wtc_task_period, wtc_task_info,wtc_task_category,"
                "wtc_task_product_id, wtc_task_site from crawler_wcs_task_center where wtc_status=%s and "
                "wtc_platform=%s and wtc_is_delete=%s")
            rows_1 = store.execute_sql(sql_select_track, 1, 'amazon', 0)

        for row_1 in rows_1:
            row_dct = row_1

            if Temp:
                task_id = row_dct['wtcId']
                task_type = row_dct['wtcTaskType']
                task_frequency = row_dct['wtcTaskFrequency']
                task_period = row_dct['wtcTaskPeriod']
                task_info = row_dct['wtcTaskInfo']
                task_category = row_dct['wtcTaskCategory']
            else:
                task_id = row_dct['wtc_id']
                task_type = row_dct['wtc_task_type']
                task_frequency = row_dct['wtc_task_frequency']
                task_period = row_dct['wtc_task_period']
                task_info = row_dct['wtc_task_info']
                task_category = row_dct['wtc_task_category']

            key = RedisSpace + str(task_id)
            if RedisA.exists_key(key):
                start_track_time = RedisA.get_hash_field(key, 'start_track_time')
                last_track_time = RedisA.get_hash_field(key, 'last_track_time')
                if isinstance(start_track_time, bytes):
                    start_track_time = start_track_time.decode('utf-8')
                    last_track_time = last_track_time.decode('utf-8')
                start_track_time_dt = datetime.datetime.strptime(start_track_time, "%Y-%m-%d %H:%M:%S")
                last_track_time_dt = datetime.datetime.strptime(last_track_time, "%Y-%m-%d %H:%M:%S")
                end_track_time = start_track_time_dt + datetime.timedelta(days=task_period)
                next_track_time = last_track_time_dt + datetime.timedelta(days=task_frequency)
                now_time = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                if next_track_time > end_track_time:
                    change_status(2, task_id)
                    RedisA.delete_key(key)
                if now_time > next_track_time:
                    page_url = task_info.strip()
                    mp = {'entry': task_type, 'page_url': page_url, 'task_id': task_id}
                    if task_category:
                        mp['task_category'] = task_category.strip()
                    RedisA.set_hash(key, {'last_track_time': now_time})
                    change_status(1, task_id)
                    RedisA.rc.rpush(StartUrls, mp)
                    print('track: %s' % task_id)
                    update_proxy_ip(Que)
                    break
                print('not track time: %s' % task_id)
        else:
            flag = True
    store.close()
    if flag:
        print('no task, waiting for {} sec.'.format(WaitSec))
        time.sleep(WaitSec)


if __name__ == '__main__':
    pass






