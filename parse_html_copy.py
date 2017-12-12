# -*- coding: utf-8 -*-
import re
import json
import time
import traceback
import uuid
from lxml import etree
from pymysql.err import InterfaceError, OperationalError, DatabaseError
from configuration_file import (
    COUNT, Month, StartUrls, ReqUrls, CrawlUrls, ErrorUrls, AsinSite, ListTag, KeyWordTag, BestSellersTag,
    NewReleasesTag, RedisSpace, OneTask, SiteType)
from scan_task import change_status


def mark_fail_task(rds):
    if rds.exists_key(OneTask):
        task_id = rds.get_hash_field(OneTask, 'task_id')
        if isinstance(task_id, bytes):
            task_id = task_id.decode('utf-8')
        change_status(-1, int(task_id))
        rds.delete_key(OneTask)


def collect_error(mp, rds, **kwargs):
    mapping = eval(mp)
    mapping["time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    mapping.update(kwargs)
    rds.add_set(ErrorUrls, mapping)
    rds.remove_member(CrawlUrls, mp)


def exist_captcha(html):
    sel = etree.HTML(html)
    captcha = sel.xpath('//input[@id="captchacharacters"]')
    if captcha:
        return True
    return False


def choose_parse(html, mp, rds, store):
    mapping = eval(mp)
    entry = mapping['entry']
    category_url = mapping.get('category_url', None)
    sel = etree.HTML(html)
    if entry in (ListTag, KeyWordTag):
        items = sel.xpath('//ul[starts-with(@class, "s-result")]/li[@data-asin]')
        if items:
            parse_list(html, mp, rds, store)
        else:
            if not category_url:
                mark_fail_task(rds)
            collect_error(mp, rds, error='no_list_items')
    elif entry in (BestSellersTag, NewReleasesTag):
        items_1 = sel.xpath('//div[starts-with(@class, "zg_itemImmersion")]')
        items_2 = sel.xpath('//div[starts-with(@class, "zg_itemRow")]')  # 日站
        if items_1 or items_2:
            parse_top(html, mp, rds, store)
        else:
            if not category_url:
                mark_fail_task(rds)
            collect_error(mp, rds, error='no_top_items')
    else:
        parse_product(html, mp, rds, store)


def parse_list(html, mp, rds, store):
    mapping = eval(mp)
    page_url = mapping['page_url']
    entry = mapping['entry']
    task_id = mapping['task_id']
    search_box = mapping.get('search_box', None)
    category_url = mapping.get('category_url', None)
    if not category_url:
        category_url = page_url
        mapping['category_url'] = page_url
    amount = mapping.get('amount', COUNT)
    try:
        # 确定站点
        suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]  # com美，co.uk英，co.jp日
        domain = SiteType[suffix]
        sign = domain['sign']
        site = domain['site']
        currency = domain['currency']
        sel = etree.HTML(html)

        result_count_xp = sel.xpath('//*[@id="s-result-count"]/text()')
        if result_count_xp:
            result_count = re.findall(r'of (.+) results', result_count_xp[0])
            if result_count:
                result_count = result_count[0].replace(',', '')
            else:
                result_count = ''
        else:
            result_count = ''

        if entry == 2:
            category = '>'.join(sel.xpath('//*[@id="s-result-count"]/span/*/text()'))
            task_info = category
        else:
            keyword = re.findall(r'field-keywords=(.+$)', category_url)[0]
            task_info = ' '.join(keyword.split('+'))
        running_category_key = '{}{}'.format(RedisSpace, category_url)
        products_lst = sel.xpath('//ul[starts-with(@class, "s-result")]/li[@data-asin]')
        for pl in products_lst:
            count = rds.count_members(running_category_key)
            if COUNT and count >= amount:
                rds.delete_key(running_category_key)
                break
            asin = pl.xpath('./@data-asin')
            if asin:
                asin = asin[0].strip()
                if rds.is_member(running_category_key, asin):
                    continue
                # 插入关联表
                product_url = 'https://www.amazon.{}/dp/{}'.format(suffix, asin)
                rds.add_set(running_category_key, asin)
                rank = rds.count_members(running_category_key)
                _uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, asin + suffix)).replace('-', '')
                try:
                    store.insert_wcs_task_relevance(_uuid, rank, product_url, task_id, entry, task_info, 'amazon')
                except InterfaceError:
                    print('InterfaceError')
                    rds.add_set(ReqUrls, mp)
                    store.conn.ping()
                except:
                    traceback.print_exc()

                # 不去重
                # used_asin = '{}@{}'.format(asin, suffix)
                # if not rds.is_member(AsinSite, used_asin):
                try:
                    original_price = pl.xpath('.//span[contains(@aria-label, "Suggested Retail Price")]/text()')
                    if original_price:
                        original_price = ''.join(original_price[0]).replace('from', '').replace(sign, '').replace(',', '').replace(currency, '')
                    else:
                        original_price = 0
                    price_1 = pl.xpath('.//span[contains(@class, "a-size-small s-padding-right-micro")]/text()')
                    price_2 = pl.xpath('.//span[contains(@class, "sx-price sx-price-large")]/../@aria-label')
                    price_3 = pl.xpath(
                        './/span[contains(@class, "a-size-base a-color-price s-price a-text-bold")]/text()')
                    price_4 = pl.xpath('.//a[@class="a-link-normal a-text-normal"]/span[@class="a-offscreen"]/text()')
                    price_5 = pl.xpath('.//span[@class="sx-price sx-price-large"]')
                    if len(price_1) > 0:
                        price = price_1
                    elif len(price_2) > 0:
                        price = price_2[0]
                    elif len(price_3) > 0:
                        price = price_3
                    elif len(price_4) > 0:
                        price = price_4[0]
                    elif len(price_5) > 0:
                        price_whole = price_5[0].xpath('./span[@class="sx-price-whole"]/text()')
                        price_fractional = price_5[0].xpath('./sup[@class="sx-price-fractional"]/text()')
                        price = '{}.{}'.format(price_whole[0], price_fractional[0])
                    else:
                        price = 0
                    max_price = 0
                    if price != 0:
                        price = ''.join(price).replace('from', '').replace(sign, '').replace(',', '').replace(currency, '')
                        if '-' in price:
                            price, max_price = [p.strip() for p in price.split('-')]

                    original_price = float(original_price)
                    price = float(price)
                    max_price = float(max_price)
                except:
                    original_price = 0
                    price = 0
                    max_price = 0
                #rds.add_set(AsinSite, used_asin)
                new_mp = {'page_url': product_url, 'entry': 1, 'rank': rank, 'uuid': _uuid,
                          'price': price, 'max_price': max_price, 'original_price': original_price,
                          'category_url': category_url, 'category_entry': entry, 'category_info': task_info,
                          'result_count': result_count}
                if search_box:
                    new_mp["search_box"] = search_box
                rds.rc.lpush(StartUrls, new_mp)

        else:
            next_page = sel.xpath('//a[@id="pagnNextLink"]/@href')
            if next_page:
                next_page_url = site + next_page[0]
                mapping['page_url'] = next_page_url
                rds.rc.lpush(StartUrls, mapping)
            else:
                rds.delete_key(running_category_key)
        rds.remove_member(CrawlUrls, mp)
    except:
        rds.add_set(ReqUrls, mp)
        traceback.print_exc()


# BestSellers, NewReleases类型不去重，全部存入track表
def parse_top(html, mp, rds, store):
    mapping = eval(mp)
    page_url = mapping['page_url']
    entry = mapping['entry']
    task_id = mapping['task_id']
    category_url = mapping.get('category_url', None)
    task_category = mapping.get('task_category', None)
    if not category_url:
        category_url = page_url
        mapping['category_url'] = page_url
    try:
        suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]
        domain = SiteType[suffix]
        sign = domain['sign']
        currency = domain['currency']
        sel = etree.HTML(html)

        # first_page = sel.xpath('//li[contains(@class, "zg_selected" )and @id="zg_page1"]')
        # if first_page:
        #     others_page = sel.xpath('//ol[starts-with(@class, "zg_pagination")]/li[not(@id="zg_page1")]')
        #     for op in others_page:
        #         op_url = op.xpath('./a/@href')[0]
        #         mapping['page_url'] = op_url
        #         rds.rc.lpush(StartUrls, mapping)

        category = sel.xpath('//h1[@id="zg_listTitle"]/span/text()')
        if category:
            category = category[0].strip()
        else:
            category = ''
        if task_category:
            category = task_category
        products_lst_1 = sel.xpath('//div[starts-with(@class, "zg_itemImmersion")]')  # 美英法站
        products_lst_2 = sel.xpath('//div[starts-with(@class, "zg_itemRow")]')  # 日站
        products_lst = products_lst_1 if products_lst_1 else products_lst_2
        for pl in products_lst:
            # asin
            asin = pl.xpath('.//div[@data-p13n-asin-metadata]/@data-p13n-asin-metadata')
            if asin:
                asin = eval(asin[0])['asin']
                rank = pl.xpath('.//span[@class="zg_rankNumber"]/text()')
                if rank:
                    rank = rank[0].strip().replace('.', '')
                else:
                    rank = 0

                # 插入关联表
                product_url = 'https://www.amazon.{}/dp/{}'.format(suffix, asin)
                _uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, asin + suffix)).replace('-', '')
                store.insert_wcs_task_relevance(_uuid, rank, product_url, task_id, entry, category, 'amazon')
                try:
                    price_1 = pl.xpath('.//span[starts-with(@class, "a-size-base a-color-price")]/span/text()')
                    if len(price_1) > 0:
                        _price = ''.join(price_1).replace(sign, '').replace(',', '').replace(' ', '').replace(currency, '')
                        if '-' in _price:
                            price, max_price = [p.strip() for p in _price.split('-')]
                            price = ''.join(re.findall(r'\d+\.?\d*', price))
                            max_price = ''.join(re.findall(r'\d+\.?\d*', max_price))
                        else:
                            price = _price
                            price = ''.join(re.findall(r'\d+\.?\d*', price))
                            max_price = 0
                    else:
                        price = 0
                        max_price = 0

                    price = float(price)
                    max_price = float(max_price)
                except:
                    price = 0
                    max_price = 0
                new_mp = {'page_url': product_url, 'entry': 1, 'rank': rank, 'price': price, 'uuid': _uuid,
                          'max_price': max_price, 'category_info': category, 'category_url': category_url,
                          'category_entry': entry}
                rds.rc.lpush(StartUrls, new_mp)
        rds.remove_member(CrawlUrls, mp)
        current_page = sel.xpath('//ol[starts-with(@class, "zg_pagination")]/li[contains(@class, "zg_page zg_selected")]/@id')[0].strip()[-1]
    except:
        rds.add_set(ReqUrls, mp)
        traceback.print_exc()


def parse_product(html, mp, rds, store):
    mapping = eval(mp)   # eval()函数还原存入字典值的类型
    page_url = mapping['page_url']
    entry = mapping['entry']
    category_info = mapping.get('category_info', '')
    category_url = mapping.get('category_url', '')
    category_entry = mapping.get('category_entry', entry)
    list_rank = mapping.get('rank', 0)
    search_box = mapping.get('search_box', '')
    try:
        sel = etree.HTML(html)

        # first_title 商品详情页显示的分类
        page_category = sel.xpath('//div[starts-with(@id, "wayfinding-breadcrumbs_feature_div")]//li//a/text()')
        if page_category:
            first_title = '>'.join([item.strip() for item in page_category])
        else:
            first_title = ''

        # 确定站点
        suffix = re.findall(r'www.amazon.(.*?)/', page_url)[0]
        domain = SiteType[suffix]
        site = domain['site']
        currency = domain['currency']
        sign = domain['sign']

        # products_id
        products_id = re.findall(r'dp/(.+)', page_url)[0]

        # uuid  products_id和suffix构成
        this_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, products_id + suffix)).replace('-', '')
        _uuid = mapping.get('uuid', this_uuid)

        # product_url
        product_url = page_url

        # asin
        asin_1 = sel.xpath('//th[contains(text(), "ASIN:")]/../*[2]/text()')
        asin_2 = sel.xpath('//td[contains(text(), "ASIN:")]/../*[2]/text()')
        asin_3 = sel.xpath('//b[contains(text(), "ASIN:")]/../text()')
        asin_4 = sel.xpath('//span[contains(text(), "ASIN:")]/../*[2]/text()')
        if asin_1:
            asin = asin_1
        elif asin_2:
            asin = asin_2
        elif asin_3:
            asin = asin_3
        elif asin_4:
            asin = asin_4
        else:
            asin = ''
        if asin:
            asin = asin[0].strip()
        url_id = asin

        # brand
        brand_1 = sel.xpath('//a[@id="bylineInfo"]/text()')
        brand_2 = sel.xpath('//a[@id="brand"]/text()')  # 日站
        brand_3 = sel.xpath('//th[contains(text(), "Brand")]')
        if brand_1:
            brand = brand_1[0].strip()
        elif brand_2:
            brand = brand_2[0].strip()
        elif brand_3:
            brand = brand_3[0].xpath('./../td/text()')
            if brand:
                brand = brand[0].strip()
            else:
                brand = ''
        else:
            brand = ''

        # name
        title_1 = sel.xpath('//span[@id="productTitle"]/text()')
        title_2 = sel.xpath('//div[@id="title_feature_div"]/h1/text()')
        title_3 = sel.xpath('//div[@id="mnbaProductTitleAndYear"]/span/text()')
        title_4 = sel.xpath('//h1[starts-with(@class, "parseasinTitle")]/span/text()')
        title_5 = sel.xpath('//span[@id="ebooksProductTitle"]/text()')
        title_6 = sel.xpath('//div[contains(@id,"Title")]//h1/text()')
        if title_1:
            _name = title_1[0].strip()
        elif title_2:
            _name = title_2[0].strip()
        elif title_3:
            _name = ''.join(title_3)
        elif title_4:
            _name = title_4[0].strip()
        elif title_5:
            _name = title_5[0].strip()
        elif title_6:
            _name = title_6[0].strip()
        else:
            rds.remove_member(CrawlUrls, mp)
            collect_error(mp, rds, error='No product name')
            return

        # discount
        discount = sel.xpath('//*[contains(@id, "price_savings")]/*[2]/text()')
        if discount:
            discount_num = re.findall(r'(\d+)%', discount[0]) if '%' in discount[0] else re.findall(r'(\d+)',
                                                                                                    discount[0])
            if discount_num:
                discount_num = discount_num[0]
                discount = str(100 - int(discount_num)) + '%'
            else:
                discount = ''
        else:
            discount = ''

        # original_price
        original_price = sel.xpath('//div[@id="price"]//span[@class="a-text-strike"]/text()')
        if original_price:
            original_price = original_price[0].strip().replace(sign, '').replace(',', '').replace(' ', '').replace(currency, '')
        else:
            original_price = mapping.get('original_price', 0)

        # price & max_price
        price = sel.xpath('//span[contains(@id, "priceblock")]/text()')
        price_1 = sel.xpath('//div[@id="centerCol"]//span[@id="color_name_1_price"]/span/text()')
        price_2 = sel.xpath('//span[@id="actualPriceValue"]/text()')
        # price_3 = sel.xpath('//span[@class="olp-padding-right"]/span[@class="a-color-price"]/text()')
        if price:
            price = price[0].strip().replace(sign, '').replace(',', '').replace(' ', '').replace(currency, '')
            if '-' in price:
                price, max_price = [p.strip() for p in price.split('-')]
                price = re.findall(r'\d+\.?\d*', price)
                if price:
                    price = ''.join(price)
                else:
                    price = 0
                max_price = ''.join(re.findall(r'\d+\.?\d*', max_price))
                if max_price:
                    max_price = ''.join(max_price)
                else:
                    max_price = 0
            else:
                max_price = 0
        elif price_1:
            price = ''.join(price_1).replace(sign, '').replace(',', '').replace(' ', '').replace(currency, '')
            price = re.findall(r'\d+\.?\d*', price)
            if price:
                price = ''.join(price)
            else:
                price = 0
            max_price = 0
        elif price_2:
            price = price_2[0].strip().replace(sign, '').replace(currency, '')
            max_price = 0
        else:
            price = mapping.get('price', 0)
            max_price = mapping.get('max_price', 0)

        # grade_count
        _grade_count = sel.xpath('//span[@id="acrPopover"]/@title')
        if _grade_count:
            if site == 'https://www.amazon.co.jp':
                grade_count = _grade_count[0].split(' ')[-1]
            else:
                grade_count = _grade_count[0].split(' ')[0]
        else:
            grade_count = 0

        # review_count
        _review_count = sel.xpath('//span[@id="acrCustomerReviewText"]/text()')
        if _review_count:
            _review_count = _review_count[0].strip().replace(',', '')
            review_count = re.findall(r'(\d+)', _review_count)[0]
        else:
            review_count = 0

        # questions
        _questions = sel.xpath('//a[@id="askATFLink"]/span/text()')
        if _questions:
            _questions = _questions[0].strip().replace('+', '').replace(',', '')
            questions = re.findall(r'\d+', _questions)[0]
        else:
            questions = 0

        # attribute
        attribute = dict()
        size = sel.xpath('//div[@id="variation_size_name"]')
        if size:
            if size[0].xpath('//select[@id="native_dropdown_selected_size_name"]'):
                attribute['size'] = [item.strip() for item in
                                     size[0].xpath(
                                         '//select[@id="native_dropdown_selected_size_name"]/option/text()')[
                                     1:]]
            elif size[0].xpath('//span[@class="selection"]/text()'):
                attribute['size'] = size[0].xpath('//span[@class="selection"]/text()')[0].strip()
            elif size[0].xpath('div/text()'):
                attribute['size'] = ''.join([item.strip() for item in size[0].xpath('div/text()')])

        color = sel.xpath('//div[@id="variation_color_name"]')
        if color:
            if color[0].xpath(
                    '//select[@id="native_dropdown_selected_color_name"]//option/@data-a-html-content'):
                attribute['color'] = color[0].xpath(
                    '//select[@id="native_dropdown_selected_color_name"]//option/@data-a-html-content')
            elif color[0].xpath('ul//li/@title'):
                attribute['color'] = [color.replace('Click to select', '').strip() for color in
                                      color[0].xpath('ul//li/@title')]
            elif color[0].xpath('//span[@class="selection"]/text()'):
                attribute['color'] = ''.join(
                    [item.strip() for item in color[0].xpath('//span[@class="selection"]/text()')])
            elif color[0].xpath('div/text()'):
                attribute['color'] = ''.join([item.strip() for item in color[0].xpath('div/text()')])

        style = sel.xpath('//div[@id="variation_style_name"]')
        if style:
            style_option = sel.xpath(
                '//select[@id="native_dropdown_selected_style_name"]//option/@data-a-html-content')
            style_option_1 = style[0].xpath('.//span/text()')
            if style_option:
                attribute['style'] = style_option
            elif style_option_1:
                attribute['style'] = style_option_1[0].strip()
            else:
                styles = style[0].xpath('ul//li/@title')
                attribute['style'] = [' '.join(style.split(' ')[3:]) for style in styles]

        if attribute:
            attribute = json.dumps(attribute, ensure_ascii=False)
        else:
            attribute = ''

        # main_image_url
        image_url_1 = sel.xpath('//img[@id="landingImage"]/@data-old-hires')
        image_url_2 = sel.xpath('//img[@id="imgBlkFront"]/@src')
        if image_url_1:
            main_image_url = image_url_1[0]
        elif image_url_2:
            main_image_url = image_url_2[0]
        else:
            main_image_url = ''

        # extra_image_urls
        eiu = []
        _extra_image_urls = sel.xpath('//div[@id="altImages"]')
        if _extra_image_urls:
            _extra_image_urls = _extra_image_urls[0].xpath('ul/li//img/@src')
            for url in _extra_image_urls:
                if '.jpg' in url:
                    image_url = re.findall(r'(.+)\._?.+\.jpg', url)[0] + '.jpg'
                    eiu.append(image_url)
                elif '.png' in url:
                    image_url = re.findall(r'(.+)\._?.+\.png', url)[0] + '.png'
                    eiu.append(image_url)
                else:
                    pass
        if eiu:
            extra_image_urls = ','.join(eiu)
            if not main_image_url:
                main_image_url = eiu[0]
        else:
            extra_image_urls = ''

        # description
        _description = sel.xpath('//div[@id="productDescription"]/p/text()')
        if _description:
            description = ''.join([desc.strip() for desc in _description])
        else:
            description = ''

        # generation_time
        release_date = sel.xpath('//strong[contains(text(), "Original Release Date")]/../text()')
        if suffix in ('com', 'co.uk'):  # 美英站两种匹配规则
            ymd_com = sel.xpath('//*[contains(text(), "%s")]' % Month['com']['xpath_text'])
            ymd_uk = sel.xpath('//*[contains(text(), "%s")]' % Month['co.uk']['xpath_text'])
            ymd = ymd_com if ymd_com else ymd_uk
        else:
            ymd = sel.xpath('//*[contains(text(), "%s")]' % Month[suffix]['xpath_text'])
        if ymd or release_date:
            if ymd:
                ymd_1 = ymd[-1].xpath('./../*[2]/text()')
                ymd_2 = ymd[-1].xpath('./../text()')
                ymd_3 = ymd[-1].xpath('./text()')
                if ymd_1 and re.findall(r'\d{4}', ymd_1[0]):
                    ymd = ymd_1
                elif ymd_2 and re.findall(r'\d{4}', ymd_2[0]):
                    ymd = ymd_2
                elif ymd_3:
                    ymd_3_1 = re.findall(r':(.+\d+)', ymd_3[0].strip())
                    ymd_3_2 = re.findall(r'\|(\d{1,2}.+\d{4})\|', ymd_3[0].strip())
                    ymd = ymd_3_1 if ymd_3_1 else ymd_3_2
            else:
                ymd = release_date
            if suffix == 'co.jp':  # 日站
                details = ymd[0].strip().split('/')
                generation_time = '%s-%s-%s' % (details[0], details[1], details[2])
            else:
                ymd = ymd[0].strip().replace(',', '').replace('.', '')
                d = re.findall(r'\d{1,2}', ymd)[0]
                y = re.findall(r'\d{4}', ymd)[0]
                m = ymd.replace(y, '').replace(d, '').strip()
                if suffix == 'es':
                    m = m.replace('de', '').strip()
                month = format_month(m, suffix)
                generation_time = '%s-%s-%s' % (y, month, d)
        else:
            generation_time = '1900-01-01 00:00:00'

        # histogram_review_count
        histogram_review_count = dict()
        _histogram_review_count = sel.xpath('//div[starts-with(@id, "rev")]//tr[contains(@class, "histogram-row")]')
        if _histogram_review_count:
            for h in _histogram_review_count[:5]:
                star_1 = h.xpath('./td[1]/*[1]/text()')
                star_2 = h.xpath('./td[1]/text()')
                if star_1:
                    _star = star_1[0]
                else:
                    _star = star_2[0]
                star = re.findall(r'\d', _star)[0]
                _percent = h.xpath('.//div[contains(@class, "a-meter") and @aria-label]/@aria-label')
                if _percent:
                    percent = _percent[0]
                else:
                    percent = '0%'
                histogram_review_count[star] = percent
        if histogram_review_count:
            histogram_review_count = json.dumps(histogram_review_count, ensure_ascii=False)
        else:
            histogram_review_count = ''
        reserve_field_1 = histogram_review_count

        # all_rank
        all_rank_1 = sel.xpath('.//*[contains(text(), "Best Sellers Rank")]/../*[2]/span/span')
        all_rank_2 = sel.xpath('//li[@id="SalesRank"]')
        if all_rank_1:
            all_rank_list = []
            for rk in all_rank_1:
                rank = rk.xpath('./text()')
                cat = rk.xpath('.//a/text()')
                if len(cat) == 1:
                    all_rank_list.append(rank[0].replace('#', '') + cat[0] + ')')
                else:
                    all_rank_list.append(rank[0].replace('#', '') + '>'.join(cat))
            all_rank_list = json.dumps(all_rank_list)
        elif all_rank_2:
            all_rank_list = []
            top_rank = all_rank_2[0].xpath('./text()')
            top_name = all_rank_2[0].xpath('./a/text()')
            if top_name and top_rank:
                all_rank_list.append(
                    ''.join([tr.strip() for tr in top_rank]).replace('#', '').replace(')', '') + top_name[0] + ')')
            category_rank = all_rank_2[0].xpath('./ul/li')
            if category_rank:
                for cr in category_rank:
                    category_rank_num = cr.xpath('./span[1]/text()')
                    category_rank_name = cr.xpath('./span[2]/a//text()')
                    all_rank_list.append(
                        category_rank_num[0].replace('#', '') + ' in ' + '>'.join(category_rank_name))
            all_rank_list = json.dumps(all_rank_list, ensure_ascii=False)
        else:
            all_rank_list = ''
        reserve_field_2 = all_rank_list

        # tech
        tech_detail = dict()
        tech = sel.xpath('//table[@id="productDetails_techSpec_section_1"]//tr')
        if tech:
            for tt in tech:
                k = tt.xpath('./th/text()')[0].strip()
                v = tt.xpath('./td/text()')[0].strip()
                if k:
                    tech_detail[k] = v
        if tech_detail:
            tech_detail = json.dumps(tech_detail, ensure_ascii=False)
        else:
            tech_detail = ''
        reserve_field_3 = tech_detail

        # reserve_field_4
        reserve_field_4 = sel.xpath('.//*[@id="merchant-info"]//text()')
        if reserve_field_4:
            reserve_field_4 = ''.join([rf.strip().replace('\n', '') for rf in reserve_field_4])
        else:
            reserve_field_4 = ''

        # reserve_field_5
        reserve_field_5 = sel.xpath('//div[contains(@id, "zeitgeistBadge")]/div/a/@title')
        if reserve_field_5:
            reserve_field_5 = reserve_field_5[0]
        else:
            reserve_field_5 = ''

        # reserve_field_6 & reserve_field_7 其他在售商家和最低价
        reserve_field_6_7 = sel.xpath('//div[contains(@id, "olp")]/div/span[1]/a/text()')
        if reserve_field_6_7:
            if suffix == 'co.jp':  # 日站
                reserve_field_6 = re.findall(r'\d+', reserve_field_6_7[0])[0]
                reserve_field_7 = sel.xpath('//div[contains(@id, "olp")]/div/span[1]/span/text()')
                reserve_field_7 = re.findall(r'\d+', reserve_field_7[0])[0]
            else:
                reserve_field_6_7 = reserve_field_6_7[0].strip()
                reserve_field_6_7 = re.findall(r'\d+,*\d*\.?\d*', reserve_field_6_7)  # '1,044.74'
                if len(reserve_field_6_7) > 1:
                    reserve_field_6 = reserve_field_6_7[0]
                    reserve_field_7 = reserve_field_6_7[1].replace(',', '')
                else:
                    reserve_field_6 = reserve_field_6_7[0]
                    reserve_field_7 = 0
        else:
            reserve_field_6 = 0
            reserve_field_7 = 0

        create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))

        # shop_name & shop_url
        shop = sel.xpath('//div[@id="merchant-info"]/a')  # 日站
        if shop:
            shop_name = sel.xpath('//div[@id="merchant-info"]/a/text()')[0]
            shop_url = site + sel.xpath('//div[@id="merchant-info"]/a/@href')[0]
        else:
            shop_name = ''
            shop_url = ''

        second_title = ''
        if category_entry == 1:
            category = first_title
        elif category_entry == 3:
            second_title = category_info
            if search_box:
                category = search_box
            else:
                category = first_title
        else:
            category = category_info
        dispatch = ''
        shipping = ''
        version_urls = ''
        sales_total = mapping.get('result_count', '')
        total_inventory = ''
        favornum = list_rank
        if category_entry == 2:
            tags = 'List'
        elif category_entry == 3:
            tags = 'KeyWord'
        elif category_entry == 4:
            tags = 'BestSellers'
        elif category_entry == 5:
            tags = 'NewReleases'
        else:
            tags = 'Detail'
        platform = 'amazon'
        platform_url = suffix
        status = 0
        is_delete = 0

        if category_entry in (BestSellersTag, NewReleasesTag):
            table_name = 'crawler_wcs_amazon_sku_track'
        else:
            table_name = 'crawler_wcs_amazon_sku'
        store.insert_amazon_sku(table_name, _uuid, products_id, url_id, brand, product_url, _name, first_title, second_title,
                                original_price, price, max_price, discount, dispatch, shipping, currency,
                                attribute, version_urls, review_count, grade_count, sales_total, total_inventory,
                                favornum, main_image_url, extra_image_urls, description, category, category_url, tags,
                                shop_name, shop_url, generation_time, platform, platform_url,
                                crawl_time, create_time, status, questions, is_delete, reserve_field_1,
                                reserve_field_2, reserve_field_3, reserve_field_4, reserve_field_5, reserve_field_6,
                                reserve_field_7)

        rds.remove_member(CrawlUrls, mp)
    except IndexError:
        if tags == 'Detail':
            mark_fail_task(rds)
        collect_error(mp, rds, error='IndexError')
    except InterfaceError:
        print('InterfaceError')
        rds.add_set(ReqUrls, mp)
        store.conn.ping()
    except OperationalError:
        print('OperationalError')
        rds.add_set(ReqUrls, mp)
        store.conn.ping()
    except DatabaseError:
        if tags == 'Detail':
            mark_fail_task(rds)
        traceback.print_exc()
        collect_error(mp, rds, error='DatabaseError')
    except:
        rds.add_set(ReqUrls, mp)
        traceback.print_exc()


def format_month(m, suffix):
    month = Month[suffix]['month']
    for index, item in enumerate(month):
        if item.lower().startswith(m.lower()):
            return index+1
