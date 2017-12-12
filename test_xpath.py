from lxml import etree
from configuration_file import SiteType, Month
import re
import requests
from settings import HEADERS
import random
import json
from store import AmazonStore
import uuid
import traceback
from pymysql.err import InterfaceError

store = AmazonStore()


def get_html(url):
    headers = {'User-Agent': random.choice(HEADERS)}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        if not exist_captcha(resp.text):
            resp.encoding = 'utf-8'
            with open('test_xpath.html', 'w', encoding='utf-8') as f:
                f.write(resp.text)
        else:
            print('captcha')


def exist_captcha(html):
    sel = etree.HTML(html)
    captcha = sel.xpath('//input[@id="captchacharacters"]')
    if captcha:
        return True
    return False


def parse_html(url, entry):
    with open('test_xpath.html', 'r', encoding='utf-8') as f:
        html = f.read()
    if entry in('l', 'k'):
        parse_list(html, url)
    elif entry == 't':
        parse_top(html, url)
    elif entry == 'd':
        parse_product(html, url)
    else:
        print('no entry')


def parse_list(html, url):
    # 确定站点
    suffix = re.findall(r'www.amazon.(.*?)/', url)[0]  # com美，co.uk英，co.jp日
    domain = SiteType[suffix]
    sign = domain['sign']
    site = domain['site']
    sel = etree.HTML(html)
    # category
    category = sel.xpath('//*[@id="s-result-count"]/span/*/text()')
    if category:
        category = '>'.join(category)
    else:
        category = ''
    print(category)

    result_count_xp = sel.xpath('//*[@id="s-result-count"]/text()')
    if result_count_xp:
        result_count = re.findall(r'of (.+) results', result_count_xp[0])
        if result_count:
            result_count = result_count[0].replace(',', '')
        else:
            result_count = ''
    else:
        result_count = ''
    print(result_count)
    products_lst = sel.xpath('//ul[starts-with(@class, "s-result")]/li[@data-asin]')
    for pl in products_lst:
        asin = pl.xpath('./@data-asin')
        if asin:
            asin = asin[0].strip()
        else:
            asin = 0
        # comments
        comments_1 = pl.xpath(
            './/span[@name]/../a[starts-with(@class, "a-size-small a-link-normal")]/text()')
        if comments_1:
            _comments = comments_1[-1].replace(',', '')
            if _comments.isdigit():  # 判断是否为数字
                comments = _comments
            else:
                comments = 0
        else:
            comments = 0
        original_price = pl.xpath('.//span[contains(@aria-label, "Suggested Retail Price")]/text()')
        if original_price:
            original_price = ''.join(original_price[0]).replace('from', '').replace(sign, '').replace(',', '')
        else:
            original_price = 0
        price_1 = pl.xpath('.//span[contains(@class, "a-size-small s-padding-right-micro")]/text()')
        price_2 = pl.xpath('.//span[contains(@class, "sx-price sx-price-large")]/../@aria-label')
        price_3 = pl.xpath('.//span[contains(@class, "a-size-base a-color-price s-price a-text-bold")]/text()')
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
            price = ''.join(price).replace('from', '').replace(sign, '').replace(',', '')
            if '-' in price:
                price, max_price = [p.strip() for p in price.split('-')]
        try:
            price = float(price)
            max_price = float(max_price)
        except ValueError:
            print('ValueError')
        print(asin, comments, original_price, price, max_price)
    next_page = sel.xpath('//a[@id="pagnNextLink"]/@href')
    if next_page:
        next_page_url = site + next_page[0]
        print(next_page_url)


def parse_top(html, url):
    suffix = re.findall(r'www.amazon.(.*?)/', url)[0]
    domain = SiteType[suffix]
    sign = domain['sign']
    currency = domain['currency']

    sel = etree.HTML(html)
    category = sel.xpath('//h1[@id="zg_listTitle"]/span/text()')
    if category:
        category = category[0].strip()
    else:
        category = ''

    products_lst_1 = sel.xpath('//div[starts-with(@class, "zg_itemImmersion")]')  # 美英法站
    products_lst_2 = sel.xpath('//div[starts-with(@class, "zg_itemRow")]')  # 日站
    products_lst = products_lst_1 if products_lst_1 else products_lst_2
    task_id = 1
    entry = 4

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
            try:
                store.insert_wcs_task_relevance(_uuid, rank, product_url, task_id, entry, category, 'amazon')
            except InterfaceError:
                print('InterfaceError')
                store.conn.ping()
            except:
                traceback.print_exc()
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

    current_page = sel.xpath('//ol[starts-with(@class, "zg_pagination")]/li[contains(@class, "zg_page zg_selected")]/a/@page')
    if current_page:
        current_page_num = current_page[0].strip()
        if current_page_num.isdigit():
            next_page_id = int(current_page_num) + 1
            print('next_page_id:', next_page_id)
            next_page = sel.xpath('//ol[starts-with(@class, "zg_pagination")]/li/a[@page="%s"]/@href' % next_page_id)
            if next_page:
                next_page_url = next_page[0].strip()
                print(next_page_url)
        else:
            print("current_page_num is not digit")
    else:
        print("no current page")


def parse_product(html, url):
    sel = etree.HTML(html)

    # first_title 商品详情页显示的分类
    # first_title 商品详情页显示的分类
    page_category = sel.xpath('//div[starts-with(@id, "wayfinding-breadcrumbs_feature_div")]//li//a/text()')
    if page_category:
        first_title = '>'.join([item.strip() for item in page_category])
    else:
        first_title = ''

    # 确定站点
    suffix = re.findall(r'www.amazon.(.*?)/', url)[0]
    domain = SiteType[suffix]
    site = domain['site']
    currency = domain['currency']
    sign = domain['sign']

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
    brand_2 = sel.xpath('//a[@id="brand"]/text()')
    if brand_1:
        brand = brand_1[0].strip()
    elif brand_2:
        brand = brand_2[0].strip()
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
        _name = ''

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
        original_price = original_price[0].strip().replace(sign, '').replace(',', '').replace(' ', '')
    else:
        original_price = 0

    # price & max_price
    price = sel.xpath('//span[contains(@id, "priceblock")]/text()')
    price_1 = sel.xpath('//div[@id="centerCol"]//span[@id="color_name_1_price"]/span/text()')
    price_2 = sel.xpath('//span[@id="actualPriceValue"]/text()')
    # price_3 = sel.xpath('//span[@class="olp-padding-right"]/span[@class="a-color-price"]/text()')
    if price and price[0].strip().startswith(sign):
        price = price[0].strip().replace(sign, '').replace(',', '').replace(' ', '')
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
        price = ''.join(price_1).replace(sign, '').replace(',', '').replace(' ', '')
        price = re.findall(r'\d+\.?\d*', price)
        if price:
            price = ''.join(price)
        else:
            price = 0
        max_price = 0
    elif price_2:
        price = price_2[0].strip().replace(sign, '')
        max_price = 0
    else:
        price = 0
        max_price = 0

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
        print(_extra_image_urls)
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
    if suffix in('com', 'co.uk'):  # 美英站两种匹配规则
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
        generation_time = '0000-00-00'

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
    reserve_field_4 = sel.xpath('//*[@id="merchant-info"]//text()')
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

    # shop_name & shop_url
    shop = sel.xpath('//div[@id="merchant-info"]/a')  # 日站
    if shop:
        shop_name = sel.xpath('//div[@id="merchant-info"]/a/text()')[0]
        shop_url = site + sel.xpath('//div[@id="merchant-info"]/a/@href')[0]
    else:
        shop_name = ''
        shop_url = ''

    print('first_title: {}'.format(first_title))
    print('asin: {}'.format(url_id))
    print('brand: {}'.format(brand))
    print('_name: {}'.format(_name))
    print('discount: {}'.format(discount))
    print('original_price: {}'.format(original_price))
    print('price: {}'.format(price))
    print('max_price: {}'.format(max_price))
    print('grade_count: {}'.format(grade_count))
    print('review_count: {}'.format(review_count))
    print('questions: {}'.format(questions))
    print('attribute: {}'.format(attribute))
    print('image_url: {}'.format(main_image_url))
    print('extra_image_urls: {}'.format(extra_image_urls))
    print('description: {}'.format(description))
    print('generation_time: {}'.format(generation_time))
    print('reserve_field_1: {}'.format(reserve_field_1))
    print('reserve_field_2: {}'.format(reserve_field_2))
    print('reserve_field_3: {}'.format(reserve_field_3))
    print('reserve_field_4: {}'.format(reserve_field_4))
    print('reserve_field_5: {}'.format(reserve_field_5))
    print('reserve_field_6: {}'.format(reserve_field_6))
    print('reserve_field_7: {}'.format(reserve_field_7))
    print('shop_name: {}'.format(shop_name))
    print('shop_url: {}'.format(shop_url))


def format_month(m, suffix):
    month = Month[suffix]['month']
    for index, it in enumerate(month):
        if m.lower() == it.lower():
            return index+1


def main(url, entry, flag=1):   # flag确定是否重新下载
    if flag:
        get_html(url)
    parse_html(url, entry)


if __name__ == '__main__':
    l = 'https://www.amazon.com/gp/bestsellers/automotive/15737391/ref=pd_zg_hrsr_automotive_3_5_last'
    d = 'https://www.amazon.com/dp/B00SYHWUF4'
    main(d, 'd', 1)
