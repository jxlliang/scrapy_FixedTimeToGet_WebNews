# encoding: utf-8
"""
author=fenglelanya
learn more
"""
import time,os,sys,sched,threading
from scrapy import cmdline
#初始化sched模块的scheduler类
#第一个参数是一个可以返回时间戳的函数，第二个参数可以在定时未到达之前阻塞。
schedule=sched.scheduler(time.time,time.sleep)
#被周期性调度触发的函数
class GetAllFinanceNews(object):
    def __init__(self):
        super(GetAllFinanceNews,self).__init__()
        self.main()

    def getLastNew(self):
        """获取新闻"""
        cmdline.execute("scrapy crawl deepNews".split())

    def lastNewTask(self):
        """获取lastNew的任务"""
        threading.Thread(target=self.getLastNew).start()

    def main(self, sec=300):
        """主函数"""
        # enter 四个参数分步为:事件间隔、优先级(由于同时间到达的两个事件同时执行时定序)、被调用出发的函数、给该触发函数的参数(tuple形式)
        # schedule.enter(sec,0,self.getLastNew,())())
        schedule.enter(sec, 0, self.lastNewTask, ())
        time.sleep(5)
        schedule.run()

if __name__ == '__main__':
    GetAllFinanceNews()