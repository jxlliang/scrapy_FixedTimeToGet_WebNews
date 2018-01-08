# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class GetallfinancenewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    web_ = scrapy.Field()
    title_ = scrapy.Field()
    date_  = scrapy.Field()
    url_   = scrapy.Field()
    content_ = scrapy.Field()
    source_ = scrapy.Field()

