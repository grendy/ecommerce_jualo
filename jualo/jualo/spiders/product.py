import scrapy
import json
import time
from scrapy.http import FormRequest
# from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from scrapy.http import TextResponse
import traceback
from pyvirtualdisplay import Display
from selenium.webdriver.common.keys import Keys
import MySQLdb
from selenium.webdriver.common.proxy import *
from scrapy.http import TextResponse
import time


class ProductSpider(scrapy.Spider):
    name = "coy"
    allowed_domains = ["https://jualo.com"]
    start_urls = ["https://www.jualo.com/"]

    def __init__(self,conn):
        self.conn = conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        self.driver = webdriver.PhantomJS()
        # display = Display(visible=0, size=(800,600))
        # display.start()
        # self.driver = webdriver.Firefox()

    @classmethod
    def from_crawler(cls, crawler):
        conn = MySQLdb.connect(
            host=crawler.settings['MYSQL_HOST'],
            port=crawler.settings['MYSQL_PORT'],
            user=crawler.settings['MYSQL_USER'],
            passwd=crawler.settings['MYSQL_PASS'],
            db=crawler.settings['MYSQL_DB'])
        return cls(conn)

    def parse(self, response):
        cursor = self.conn.cursor()
        try:
            a = 0
            for tidur in range(0, 100):
                time.sleep(1)
                try:
                    sql = "select url from jualo_category"
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    # import pdb;pdb.set_trace()
                    # results = "https://www.tokopedia.com/p/pakaian"
                    for ulang in range(0, 24):
                        a = results[ulang]
                        url = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                        print "====================================="
                        print(url)
                        print "====================================="
                        self.driver.get(url)
                        # import pdb;pdb.set_trace()
                        try:
                            for halaman in range(0, 500):
                                try:
                                    count = 0
                                    for i in range(0,20):
                                        response = TextResponse(url=response.url, body=self.driver.page_source,encoding='utf-8')
                                        product_url = response.xpath('/ html / body / div[2] / div / div[4] / div[2] / div / div / ul / li[' + str(i+1) + '] / div / div[1] / div / div[1] / a[2]/@href').extract_first()
                                        count +=1
                                        if count == 60:
                                            import pdb;pdb.set_trace()
                                        else:
                                            print count
                                            if product_url != None:
                                                try:
                                                    # import pdb;pdb.set_trace()
                                                    if count == 1 and product_url == None:
                                                        break
                                                    status = ""
                                                    product_url = product_url.encode('utf-8')
                                                    sql = "INSERT INTO `jualo_url`(`product_url`, `status`) VALUES ('{}','{}') ".format(product_url, status)
                                                    cursor.execute(sql)
                                                    self.conn.commit()
                                                    print "======================================="
                                                    print product_url
                                                    print "INSERT SUKSES"
                                                    print "======================================="
                                                except:
                                                    print "==============================================================================="
                                                    print "Data Duplicate"
                                                    print product_url
                                                    print "==============================================================================="
                                                    pass
                                            else:
                                                print "========+============"
                                                pass
                                                # conn.rollback()
                                            # break
                                except:
                                    pass
                                # import pdb;pdb.set_trace()
                                try:
                                    coy = url + "?page=" + str(halaman+1)
                                    self.driver.get(coy)
                                except:
                                    pass
                                if product_url == None:
                                    break
                        except:
                            pass
                except:
                        pass

        except:
            pass
        self.conn.close()
        try:
            self.driver.close()
        except:
            pass