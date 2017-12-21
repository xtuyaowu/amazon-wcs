import json
import time
import traceback
from store import AmazonStorePro, RedisCluster
from configuration_file import MysqlDataList, MysqlDataError, RelevanceTable


sql_sku = (
    "insert into {}(scgs_uuid, scgs_products_id, scgs_url_id, scgs_brand, scgs_product_url, scgs_name,"
    "scgs_firstTitle, scgs_secondTitle, scgs_original_price, scgs_price, scgs_max_price, scgs_discount,"
    "scgs_dispatch, scgs_shipping, scgs_currency, scgs_attribute, scgs_version_urls, scgs_review_count,"
    "scgs_grade_count, scgs_sales_total, scgs_total_inventory, scgs_favornum, scgs_image_url,"
    "scgs_extra_image_urls, scgs_description, scgs_category, scgs_category_url, scgs_tags, scgs_shop_name, "
    "scgs_shop_url, scgs_generation_time, scgs_platform, scgs_platform_url, scgs_crawl_time, scgs_create_time, "
    "scgs_status, scgs_questions, scgs_is_delete, scgs_reserve_field_1, scgs_reserve_field_2,"
    "scgs_reserve_field_3, scgs_reserve_field_4, scgs_reserve_field_5, scgs_reserve_field_6,"
    "scgs_reserve_field_7)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")


sql_relevance = (
    "insert into crawler_wcs_task_relevance(wtr_sku_uuid, wtr_sku_rank, wtr_sku_url, wtr_task_id,"
    "wtr_task_type, wtr_task_info, wtr_platform, wtr_crawl_time, wtr_create_time)values"
    "(%s,%s,%s,%s,%s,%s,%s,%s,%s)")


def data_insert(rds):
    if rds.exists_key(MysqlDataList):
        store = AmazonStorePro()
        while rds.exists_key(MysqlDataList):
            item = rds.rc.rpop(MysqlDataList)
            item_json = json.loads(item)
            table = item_json['table']
            print(table)
            data = item_json['data']
            try:
                if table == RelevanceTable:
                    store.execute_sql(sql_relevance,
                                      data['wtr_sku_uuid'],
                                      data['wtr_sku_rank'],
                                      data['wtr_sku_url'],
                                      data['wtr_task_id'],
                                      data['wtr_task_type'],
                                      data['wtr_task_info'],
                                      data['wtr_platform'],
                                      data['wtr_crawl_time'],
                                      data['wtr_create_time'])
                else:
                    store.execute_sql(sql_sku.format(table),
                                      data['scgs_uuid'],
                                      data['scgs_products_id'],
                                      data['scgs_url_id'],
                                      data['scgs_brand'],
                                      data['scgs_product_url'],
                                      data['scgs_name'],
                                      data['scgs_firstTitle'],
                                      data['scgs_secondTitle'],
                                      data['scgs_original_price'],
                                      data['scgs_price'],
                                      data['scgs_max_price'],
                                      data['scgs_discount'],
                                      data['scgs_dispatch'],
                                      data['scgs_shipping'],
                                      data['scgs_currency'],
                                      data['scgs_attribute'],
                                      data['scgs_version_urls'],
                                      data['scgs_review_count'],
                                      data['scgs_grade_count'],
                                      data['scgs_sales_total'],
                                      data['scgs_total_inventory'],
                                      data['scgs_favornum'],
                                      data['scgs_image_url'],
                                      data['scgs_extra_image_urls'],
                                      data['scgs_description'],
                                      data['scgs_category'],
                                      data['scgs_category_url'],
                                      data['scgs_tags'],
                                      data['scgs_shop_name'],
                                      data['scgs_shop_url'],
                                      data['scgs_generation_time'],
                                      data['scgs_platform'],
                                      data['scgs_platform_url'],
                                      data['scgs_crawl_time'],
                                      data['scgs_create_time'],
                                      data['scgs_status'],
                                      data['scgs_questions'],
                                      data['scgs_is_delete'],
                                      data['scgs_reserve_field_1'],
                                      data['scgs_reserve_field_2'],
                                      data['scgs_reserve_field_3'],
                                      data['scgs_reserve_field_4'],
                                      data['scgs_reserve_field_5'],
                                      data['scgs_reserve_field_6'],
                                      data['scgs_reserve_field_7'])
            except Exception as exp:
                traceback.print_exc()
                item_json['error'] = '{!r}'.format(exp)
                rds.rc.lpush(MysqlDataError, json.dumps(item_json))

        print('finished insert')
        store.close()
    else:
        print('no item')
        time.sleep(30)


if __name__ == '__main__':
    amazon_rds = RedisCluster()
    while True:
        data_insert(amazon_rds)

