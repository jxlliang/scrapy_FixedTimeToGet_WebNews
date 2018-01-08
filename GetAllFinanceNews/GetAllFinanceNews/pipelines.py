# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json as js
import pymssql,sys
import settings
import chardet
reload(sys)
sys.setdefaultencoding("utf-8")

class GetallfinancenewsPipeline(object):
    """
    功能:保存item数据
    """
    def __init__(self):
        super(GetallfinancenewsPipeline, self).__init__()
        # 登陆数据库
        self.host = settings.SQL_SERVER_HOST
        self.database = settings.SQL_SERVER_DBNAME
        self.user = settings.SQL_SERVER_USER
        self.pwd = settings.SQL_SERVER_PWD

    def dbConnect(self):
        """lian接数据库"""
        if not self.database:
            raise (NameError, "No such database!")
        else:
            self.cnn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.database,
                                       charset="utf8")
            cur = self.cnn.cursor()
            if not cur:
                raise (NameError, "Can not connect database")
            else:
                return cur

    def execQuery(self, sql):
        """执行查询语句"""
        try:
            cur = self.dbConnect()
            if cur:
                cur.execute(sql)
                resList = cur.fetchall()
                # 查询完毕后关闭连接
                self.cnn.close()
                return resList
        except Exception, e:
            print u'sql语句==', sql
            print u'执行查询语句的时候报错了，e=', e

    def execNonQuery(self, sql):
        """执行非查询语句"""
        cur = self.dbConnect()
        try:
            if cur:
                cur.execute(sql)
                self.cnn.commit()  # 提交sql指令
                self.cnn.close()
        except Exception, e:
            print u'sql==', sql
            print u'执行非查询语句时出错了==>', e

    def process_item(self, item, spider):
        """保存数据"""
        title = item['title_']
        content = item['content_']
        web = item['web_']
        update = item['date_']
        url = item['url_']
        source = item['source_']
        sql = "insert into GetAllWebFinanceNew([souce],[newTitle],[newConten],[newUrl],[newWeb],[newUpdateTime]) values" \
              "('%s','%s','%s','%s','%s','%s')" % (source,title,content,url,web,update)
        sql_check = "SELECT * FROM [LiangJingJun].[dbo].[GetAllWebFinanceNew] where [souce]='%s' and [newTitle]='%s'" \
                    "and [newUrl]='%s' and [newWeb]='%s' and [newUpdateTime]='%s'" % (source,title,url,web,update)
        sql_return = self.execQuery(sql_check.encode('utf-8'))
        if sql_return:
            pass
        else:self.execNonQuery(sql.encode('utf-8'))  # 写入数据库
        return item
