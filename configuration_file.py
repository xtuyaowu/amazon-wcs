# -*- coding: utf-8 -*-

# 采集任务数据库连接方式：1为接口，0为直连
Temp = 0

# 筛选条件
COUNT = 1000  # 数量筛选，针对List采集，None表示全量采集，数值如1000，表示采集前1K个产品, 默认1K,每个链接可单独设置

# ProxyIpFailTimes: 设置一个代理ip访问不成功的次数，超过则从代理ip队列删除
ProxyIpFailTimes = 3

# 访问超时设置
TimeOut = 40

# 最大并发数
ConcurrentNum = 50

# 任务拉取间隔时间
WaitSec = 60

# redis的key名字设置
RedisSpace = 'amazon:wcs:'
StartUrls = '{}starturls'.format(RedisSpace)
ReqUrls = '{}requrls'.format(RedisSpace)
CrawlUrls = '{}crawlurls'.format(RedisSpace)
ErrorUrls = '{}errorurls'.format(RedisSpace)
OneTask = '{}onetask'.format(RedisSpace)
MysqlDataList = "{}datalist".format(RedisSpace)
MysqlDataError = "{}dataerror".format(RedisSpace)

# MySQL表名
RelevanceTable = 'crawler_wcs_task_relevance'
SkuTable = 'crawler_wcs_amazon_sku'
TrackTable = 'crawler_wcs_amazon_sku_track'

# 采集链接分类标签
DetailTag = 1
ListTag = 2
KeyWordTag = 3
BestSellersTag = 4
NewReleasesTag = 5

# 采集站点：美、英、日、法、意、西、德、印、加站
SiteType = {
    'com': {'site': 'https://www.amazon.com', 'currency': 'USD', 'sign': '$'},
    'co.uk': {'site': 'https://www.amazon.co.uk', 'currency': 'GBP', 'sign': '£'},
    'co.jp': {'site': 'https://www.amazon.co.jp', 'currency': 'YEN', 'sign': '￥'},
    'fr': {'site': 'https://www.amazon.fr', 'currency': 'EUR', 'sign': 'EUR'},
    'it': {'site': 'https://www.amazon.it', 'currency': 'EUR', 'sign': 'EUR'},
    'es': {'site': 'https://www.amazon.es', 'currency': 'EUR', 'sign': 'EUR'},
    'de': {'site': 'https://www.amazon.de', 'currency': 'EUR', 'sign': 'EUR'},
    'in': {'site': 'https://www.amazon.in', 'currency': 'INR', 'sign': ''},
    'ca': {'site': 'https://www.amazon.ca', 'currency': 'CDN', 'sign': '$'},
}

# top排行分类
TopType = {
   'BestSellers': 'BestSellers', 'NewReleases': 'NewReleases',
   'Bestseller': 'BestSellers', 'HotNewReleases': 'NewReleases',
   '売れ筋ランキング': 'BestSellers', '新着ランキング': 'NewReleases',
   'Lesmeilleuresventes': 'BestSellers', 'Dernièresnouveautés': 'NewReleases',
   'Lenovitàpiùinteressanti': 'NewReleases',                                    # BestSellers同英站
   'Losmásvendidos': 'BestSellers', 'Últimasnovedades': 'NewReleases',
   'Neuerscheinungen': 'NewReleases'                                            # BestSellers同英站
}


# 上架日期
Month = {
    'com': {'month': ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December'],
            'xpath_text': 'Date first available'},
    'co.uk': {'month': ['January', 'February', 'March', 'April', 'May', 'June',
                        'July', 'August', 'September', 'October', 'November', 'December'],
              'xpath_text': 'Date First Available'},
    'co.jp': {'xpath_text': 'での取り扱い開始日'},         # 日站的时间格式是数字形式的
    'fr': {'month': ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin',
                     'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'],
           'xpath_text': 'Date de mise en ligne sur Amazon.fr'},
    'it': {'month': ['Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
                     'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre'],
           'xpath_text': 'Disponibile su Amazon.it a partire dal'},
    'es': {'month': ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                     'Julio', 'Agosto', 'Septiembre', 'Octubre', 'noviembre', 'Diciembre'],
           'xpath_text': 'Producto en Amazon.es desde'},
    'de': {'month': ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
                     'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember'],
           'xpath_text': 'Im Angebot von Amazon.de seit'},
    'in': {'month': ['January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December'],
           'xpath_text': 'Date first available'},
    'ca': {'month': ['January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December'],
           'xpath_text': 'Date first available'},
}

# 关键词按分类搜索
SearchBox = {
    "All": "aps",
    "Cyber Monday Deals Week": "cyber-monday",
    "Alexa Skills": "alexa-skills",
    "Amazon Devices": "amazon-devices",
    "Amazon Fresh": "amazonfresh",
    "Amazon Video": "instant-video",
    "Amazon Warehouse Deals": "warehouse-deals",
    "Appliances": "appliances",
    "Apps & Games": "mobile-apps",
    "Arts, Crafts & Sewing": "arts-crafts",
    "Automotive Parts & Accessories": "automotive",
    "Baby": "baby-products",
    "Beauty & Personal Care": "beauty",
    "Books": "stripbooks",
    "CDs & Vinyl": "popular",
    "Cell Phones & Accessories": "mobile",
    "Clothing, Shoes & Jewelry": "fashion",
    "Clothing, Shoes & Jewelry>Women": "fashion-womens",
    "Clothing, Shoes & Jewelry>Men": "fashion-mens",
    "Clothing, Shoes & Jewelry>Girls": "fashion-girls",
    "Clothing, Shoes & Jewelry>Boys": "fashion-boys",
    "Clothing, Shoes & Jewelry>Baby": "fashion-baby",
    "Collectibles & Fine Art": "collectibles",
    "Computers": "computers",
    "Courses": "courses",
    "Credit and Payment Cards": "financial",
    "Digital Music": "digital-music",
    "Electronics": "electronics",
    "Garden & Outdoor": "lawngarden",
    "Gift Cards": "gift-cards",
    "Grocery & Gourmet Food": "grocery",
    "Handmade": "handmade",
    "Health, Household & Baby Care": "hpc",
    "Home & Business Services": "local-services",
    "Home & Kitchen": "garden",
    "Industrial & Scientific": "industrial",
    "Kindle Store": "digital-text",
    "Luggage & Travel Gear": "fashion-luggage",
    "Luxury Beauty": "luxury-beauty",
    "Magazine Subscriptions": "magazines",
    "Movies & TV": "movies-tv",
    "Musical Instruments": "mi",
    "Office Products": "office-products",
    "Pet Supplies": "pets",
    "Prime Exclusive Savings": "prime-exclusive",
    "Prime Pantry": "pantry",
    "Software": "software",
    "Sports & Outdoors": "sporting",
    "Tools & Home Improvement": "tools",
    "Toys & Games": "toys-and-games",
    "Vehicles": "vehicles",
    "Video Games": "videogames",
    "Wine": "wine",
}
