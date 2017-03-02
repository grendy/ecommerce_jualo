import scrapy
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from scrapy.http import TextResponse
# from tokorobet.items import tcategory
# from impala.dbapi import connect
from selenium.webdriver.common.proxy import *
import traceback
import MySQLdb
from pyvirtualdisplay import Display

import time


class ProductSpider(scrapy.Spider):
    name = "kategori"
    allowed_domains = ["https://jualo.com"]
    start_urls = ["https://www.jualo.com/categories"]

    def __init__(self,conn):
        self.conn = conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        self.driver = webdriver.PhantomJS()
    @classmethod
    def from_crawler(cls,crawler):
        conn=MySQLdb.connect(
            host=crawler.settings['MYSQL_HOST'],
            port=crawler.settings['MYSQL_PORT'],
            user=crawler.settings['MYSQL_USER'],
            passwd=crawler.settings['MYSQL_PASS'],
            db=crawler.settings['MYSQL_DB'])
        return cls(conn)

    def parse(self, response):
        cur = self.conn.cursor()
        url = 'https://www.jualo.com/categories'
        try:
            # import pdb;pdb.set_trace()
            self.driver.get(url)
        except:
            print traceback.print_exc()
        for tidur in range(0, 100):
            time.sleep(1)
            try:
                for kat in range(0,24):
                    response = TextResponse(url=response.url, body=self.driver.page_source, encoding='utf-8')
                    url = response.xpath('/ html / body / div[2] / div / div[2] / div['+str(kat+1)+'] / a[1]/@href').extract_first()
                    nama_kategori = response.xpath('/ html / body / div[2] / div / div[2] / div['+str(kat+1)+'] / a[1] / div / span/text()').extract_first()
                    time.sleep (2)
                    print "========================================"
                    print(nama_kategori)
                    print(url)
                    print "========================================"
                    sql = "select * from jualo_category where url = '{}' and nama_kategori = '{}'".format(url, nama_kategori)
                    cur.execute(sql)
                    results = cur.fetchall()
                    if len(results) == 0:
                        sql = "INSERT INTO jualo_category VALUES ('{}','{}')".format(url, nama_kategori)
                        print sql
                        cur.execute(sql)
                        self.conn.commit()
                        print "======================================"
                        print "[INFO] Mysql insert sukses : {}".format(sql)
                        print "======================================"
                    else:
                        print "======================================"
                        print "[ERROR] Mysql insert failure : {}".format(sql)
                        print "============s=========================="
            except:
                pass
        cur.close()
        try:
            self.driver.close()
        except:
            pass