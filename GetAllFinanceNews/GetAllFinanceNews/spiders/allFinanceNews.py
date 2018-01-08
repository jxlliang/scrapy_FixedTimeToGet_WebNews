# encoding: utf-8
"""
author=fenglelanya
learn more
"""
from scrapy import Selector
from scrapy.http import HtmlResponse
from lxml import etree
import sys,datetime,time,pymssql,random
import pandas as pd
import numpy as np
import json as js
import scrapy,re
from scrapy.http import Request
from scrapy.spiders import CrawlSpider,Rule  # Spider，它是所有爬虫的基类，对于它的设计原则是只爬取start_url列表中的网页(CrawlSpider 是Spider的派生类，在网页中提取link并继续爬取的工作CrawSpider更适合)
from scrapy.selector import HtmlXPathSelector
from scrapy.linkextractors.sgml import SgmlLinkExtractor
from bs4 import BeautifulSoup as BS
from GetAllFinanceNews.items import GetallfinancenewsItem
from GetAllFinanceNews.settings import *
reload(sys)
sys.setdefaultencoding('utf-8')
class GetAllFinanceNews(CrawlSpider):
    name='deepNews'
    start_urls=[r'http://stock.hexun.com/',r'http://money.163.com/stock/',r'http://blog.sina.com.cn/lm/stock/',r'http://finance.sina.com.cn/stock/'
                ,r'http://stock.jrj.com.cn/',r'http://stock.stockstar.com/',r'http://finance.china.com.cn/stock/']# ,r'https://xueqiu.com/#/cn'
    Today_datetime=datetime.date.today()
    toDay=Today_datetime.strftime('%Y-%m-%d')
    toDay_string=Today_datetime.strftime('%Y%m%d')
    sinaDay = (Today_datetime + datetime.timedelta(days=-2)).strftime('%Y%m%d')  # 上一天
    year=toDay_string[:4]
    moth__=toDay_string[-4:-2]
    day__=toDay_string[-2:]
    moth_day=moth__+day__
    rules = [
        Rule(SgmlLinkExtractor(allow=(r'http://stock.hexun.com/{}/\d+.html'.format(toDay))),callback='parseHeXunContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://money.163.com/%s/%s/\d+/D7(.+).html')%(toDay_string[2:4],moth_day)),callback='parseMoneyContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/stock/jsy/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/stock/gujiayidong/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/stock/marketresearch/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/roll/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/stock/hyyj/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/stock/s/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.sina.com.cn/stock/asia/{}/doc-ifyq(.+).shtml'.format(toDay))),callback='parseSinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://blog.sina.com.cn/s/blog_(.+).html')),callback='parseSinaBlogContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.china.com.cn/stock/dp/{}/\d+.shtml'.format(sinaDay))),callback='parseFinanceChinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.china.com.cn/stock/{}/\d+.shtml'.format(sinaDay))),callback='parseFinanceChinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.china.com.cn/news/{}/\d+.shtml'.format(sinaDay))),callback='parseFinanceChinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://finance.china.com.cn/stock/xgdt/{}/\d+.shtml'.format(sinaDay))),callback='parseFinanceChinaContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://stock.stockstar.com/SS{}0(.+).shtml'.format(sinaDay))),callback='parseStockStarContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://stock.jrj.com.cn/{}/{}/{}(.+).shtml'.format(year,moth__,day__))),callback='parseJinRongJieContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://stock.jrj.com.cn/invest/{}/{}/{}(.+).shtml'.format(year, moth__, day__))),callback='parseJinRongJieContent'),
        Rule(SgmlLinkExtractor(allow=(r'http://stock.jrj.com.cn/hotstock/{}/{}/{}(.+).shtml'.format(year, moth__, day__))),callback='parseJinRongJieContent'),
        # http://stock.jrj.com.cn/hotstock/2018/01/07073123907445.shtml
    ]
    item = GetallfinancenewsItem()

    def parseJinRongJieContent(self, response):
        """金融界的新闻提取"""
        self.item['web_'] = 'JinRongJie'
        self.item['url_'] = response.url
        soup=BS(response.body,'lxml')
        try:
            title_=soup.findAll('div',{'class':'titmain'})[0].findAll('h1')[0].text.strip()
            print 'title_==', title_
        except:pass
        self.item['title_']=title_
        year_=str(datetime.date.today().year)
        dateSource=soup.findAll('p',{'class':'inftop'})[0].text.strip()
        dateSource=''.join(dateSource.replace(' ', '').split())
        date__=year_+re.findall(u'{}(.+)来源'.format(year_),dateSource)[0]
        self.item['date_'] = date__
        try:
            source_=re.findall(u'来源：(.+)开门红必抢金股',dateSource)[0]
        except:
            try:
                source_=re.findall(u'来源：(.+)你真的会炒股吗',dateSource)[0]
            except:pass
        author_=u'作者：'
        if author_ in source_:
            source_=re.sub(author_,'',source_)
        self.item['source_'] = source_
        newsData=soup.findAll('div',{'class':'texttit_m1'})[0].text.strip()
        newsData = ''.join(newsData.replace(' ', '').split())
        newsData = re.sub('\'', "\"", newsData)
        self.item['content_'] = newsData
        yield self.item

    def parseStockStarContent(self, response):
        """证券之星的新闻提取"""
        self.item['web_'] = 'StockStar'
        respon = response.body  # 返回的HTML
        self.item['url_']=response.url
        soup = BS(respon, 'lxml')
        title_=soup.select('#container-box > h1')[0].text # title_
        self.item['title_'] = title_
        date_ = soup.findAll('span', {'id': 'pubtime_baidu'})[0].text.strip()
        source_=soup.findAll('span',{'id':'source_baidu'})[0].text.strip(u'来源：').strip()
        self.item['source_'] = source_
        self.item['date_'] = date_
        dt__=soup.findAll('div',{'class':'article'})[0].text.strip()
        newsData=''.join(dt__.replace(' ','').split())
        newsData = re.sub('\'', "\"", newsData)
        self.item['content_'] = newsData
        yield self.item

    def parseHeXunContent(self,response):
        """和讯的新闻提取"""
        self.item['web_'] = 'HeXun'
        respon=response.body  # HTML
        self.item['url_'] = response.url
        soup=BS(respon,'lxml')
        title_=soup.select('body > div.layout.mg.articleName > h1')[0].text
        self.item['title_'] =title_
        date_=soup.findAll('span',{'class':'pr20'})[0].text
        self.item['date_'] = date_
        source=soup.select('body > div.layout.mg.articleName > div > div.tip.fl > a')[0].text
        self.item['source_'] = source
        newsData=soup.findAll('div',{'class':'art_contextBox'})[0].text.strip()
        newsData = ''.join(newsData.strip().replace(' ', '').split())
        newsData = re.sub('\'', "\"", newsData)
        self.item['content_'] = newsData
        yield self.item

    def parseMoneyContent(self,response):
        """网易的新闻提取"""
        self.item['web_']='163'
        url=response.url
        self.item['url_']=url
        #print u'网易的 url ==',url
        respon = response.body  # 返回的HTML
        soup=BS(respon,'lxml')
        title_=soup.select('#epContentLeft > h1')[0].text
        self.item['title_']=title_
        #print u'title_==',title_
        date_souce=soup.findAll('div',{'class':'post_time_source'})[0].text.strip()
        try:
            dt=re.findall(u'(.+)来源',date_souce)[0]
        except:
            dt=re.findall('\d+',date_souce)[0]
        self.item['date_']=dt
        #print 'date==', dt
        try:
            source=soup.findAll('a',{'id':'ne_article_source'})[0].text
        except:source='--'
        #print u'来源==',source
        self.item['source_']=source
        try:
            newsData=soup.findAll('div',{'class':'post_text'})[0].text
            newsData = re.sub('\'', "\"", newsData)
        except:
            if len(title_)>0:
                newsData=title_
            else:newsData=''
        newsData=''.join(newsData.strip().replace(' ','').split())
        #print 'newsDatas==',newsData
        self.item['content_']=newsData
        yield self.item

    def parseSinaContent(self, response):
        """新浪的即时新闻提取"""
        #print u'新浪新闻 url==',response.url
        self.item['web_']='sina'
        self.item['url_'] = response.url
        soup=BS(response.body,'lxml')
        title_=soup.select('#artibodyTitle')[0].text.strip() # 标题
        self.item['title_']=title_
        #print u'title_==',title_
        dateSource=soup.findAll('span',{'class':'time-source'})[0].text
        dateSource=''.join(dateSource.replace(' ', '').strip().split())
        dateIndigit=re.sub('\D','',dateSource) # '\D' 匹配非数字
        date__=datetime.datetime.strptime(dateIndigit,'%Y%m%d%H%S')
        self.item['date_']=date__
        #print u'date_==',date__
        source=re.sub('\d','',dateSource).strip().replace(u'年月日:','') # 匹配数字
        #print u'source==', source
        self.item['source_'] = source
        newsData=soup.findAll('div',{'class':'article article_16'})[0].text
        newsData = ''.join(newsData.strip().replace(' ', '').split())
        newsData = re.sub('\'', "\"", newsData)
        self.item['content_'] = newsData
        #print 'newsData==',newsData
        yield self.item
    """
    def parseSinaGuJiaYiDongContent(self, response):
        #新浪的估价异动新闻提取
        print u'新浪的估价异动新闻 url==',response.url
        soup = BS(response.body, 'lxml')
        title_ = soup.select('#artibodyTitle')[0].text.strip()  # 标题
        print u'title_==', title_
        dateSource = soup.findAll('span', {'class': 'time-source'})[0].text
        dateSource = ''.join(dateSource.replace(' ', '').strip().split())
        print u'dateSource==', dateSource
        newsData = soup.findAll('div', {'class': 'article article_16'})[0].text
        newsData = ''.join(newsData.strip().replace(' ', '').split())
        newsData = re.sub('\'', "\"", newsData)
        print 'newsData==', newsData
    """
    def parseSinaBlogContent(self,response):
        """新浪博客新闻提取"""
        self.item['url_']=response.url
        self.item['web_']='SinaBlog'
        soup=BS(response.body,'lxml')
        try:
            title_=soup.findAll('div',{'class':'atcbox'})[0]
            title_ = title_.findAll('h1')[0].text
            self.item['title_'] = title_
            date_ = soup.findAll('span', {'class': 'time'})[0].text
            self.item['date_'] = date_
            source = soup.findAll('a', {'class': 'btn'})
            source__ = ''
            if len(source) == 0:
                source__ = 'sina_blog financeNews'
            else:
                for i in np.arange(len(source)):
                    source__ = source__ + source[i].text
            self.item['source_'] = source__
            newsData = soup.findAll('div', {'class': 'articalContent'})[0].text.strip()
            newsData = ''.join(newsData.strip().replace(' ', '').split())
            newsData = re.sub('\'', "\"", newsData)
            self.item['content_'] = newsData
            yield self.item
        except:pass

    def parseFinanceChinaContent(self,response):
        """中国财经网新闻提取"""
        self.item['url_'] = response.url
        self.item['web_'] = 'FinanceChina'
        soup=BS(response.body,'lxml')
        title_=soup.select('body > div.wrap.c.top > h1')[0].text.strip()
        self.item['title_'] =title_
        dateSource=soup.findAll('span',{'class':'fl time2'})[0].text.strip()
        date_=re.sub('\D','',dateSource).strip()
        date__ = datetime.datetime.strptime(date_, '%Y%m%d%H%S')
        self.item['date_'] = date__
        source_=re.sub('\d','',dateSource).strip().replace(u'年月日:','').strip()
        self.item['source_']=source_
        newsData=soup.findAll('div',{'class':'navp c'})[0].text.strip()
        newsData = ''.join(newsData.strip().replace(' ', '').split())
        newsData = re.sub('\'', "\"", newsData)
        self.item['content_'] = newsData
        yield self.item