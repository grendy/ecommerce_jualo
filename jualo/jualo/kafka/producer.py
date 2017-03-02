import scrapy
from logging import exception
import datetime
from kafka import KafkaProducer, KafkaConsumer
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from scrapy.http import TextResponse
from selenium.webdriver.common.proxy import *
from scrapy.http import TextResponse
import traceback
import MySQLdb
import sys
import json
import setting
from pyvirtualdisplay import Display
import demjson

import time


class producer:
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=setting.host,
            port=setting.port,
            user=setting.user,
            passwd=setting.passwd,
            db=setting.db)
        self.connect = self.conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        # self.driver = webdriver.Chrome()
        # driver.get("http://www.google.com")
        # service_args = [setting.proxy]
        # self.driver = webdriver.PhantomJS(service_args = service_args)
        myProxy = setting.firefox_proxy
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': myProxy,
            'ftpProxy': myProxy,
            'sslProxy': myProxy,
        })
        display = Display(visible=0, size=(800, 600))
        display.start()
        self.driver = webdriver.Firefox(proxy=proxy)

    def parse(self):
        cur = self.conn.cursor()
        cou = self.conn.cursor()
        try:
            # import pdb;pdb.set_trace()
            count = "select count(*)from jualo_url where status = ''"
            sql = "select product_url from jualo_url where status = ''"
            cur.execute(sql)
            cou.execute(count)
            results = cur.fetchall()
            b = cou.fetchall()
            terus = str(b).replace(",", "").replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("L", "")
            print (terus)
            terus = int(terus)
            print "============================================"
            print (terus)
            count = 0
            for ulang in range(0, terus):
                try:
                    print (ulang)
                    count += 1
                    a = results[ulang]
                    url = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                    try:
                        self.driver.get(url)
                        time.sleep(5)
                        response = TextResponse(url=url, body=self.driver.page_source, encoding='utf-8')
                        # import pdb;pdb.set_trace()
                        try:
                            # ambil detail product dan penjual
                            product_url = url
                            penjual_url = response.xpath('//*[contains(@class,"col-md-12 name-user")]/a/@href').extract_first().encode('utf-8')
                            penjual = response.xpath('//*[contains(@class,"col-md-12 name-user")]/a/text()').extract_first().encode('utf-8')
                            lokasi = response.xpath('//*[contains(@class,"top_location")]/text()').extract()
                            product = response.xpath('//*[contains(@class,"ad_show_title")]/text()').extract_first().encode('utf-8')
                            harga = response.xpath('//*[contains(@class,"real_price")]/text()').extract_first().encode('utf-8')
                            kondisi = response.xpath('//*[contains(@class,"second-hand")]/text()').extract()
                            # kategori = response.xpath('//*[contains(@class,"breadcrumbs__link")]/a/text()').extract()
                            dilihat = response.xpath('//*[contains(@id,"view_count")]/text()').extract_first().encode('utf-8')
                            update_terakhir = response.xpath('//*[contains(@class,"top_timer")]/text()').extract()
                            deskripsi = response.xpath('//*[contains(@class,"ad_show_detail")]/text()').extract()

                            # self.driver.save_screenshot('SCEEN1.png')

                            lokasi = ''.join(lokasi)
                            lokasi = lokasi.replace("\n","").encode('utf-8')
                            product = product.replace("\\n","").replace("\n","")
                            kondisi = ''.join(kondisi)
                            kondisi = kondisi.replace("\n", "").encode('utf-8')
                            deskripsi = ''.join(deskripsi)
                            deskripsi = deskripsi.replace("\n", "").encode('utf-8')
                            update_terakhir = ''.join(update_terakhir)
                            update_terakhir = update_terakhir.replace("\n", "").encode('utf-8')

                            harga = str(harga).replace("\n","").encode('utf-8')
                            harga = harga.replace(".", "").replace("Rp ", "")
                            harga = int(harga)
                            dilihat = int(dilihat.encode('utf-8'))


                            if "\n" in deskripsi:
                                deskripsi = None
                            # import pdb;pdb.set_trace()
                            # ganti yang hari lalu jadi tanggal terupdate
                            if "hari lalu" in update_terakhir:
                                now = datetime.datetime.now()
                                hari = update_terakhir.split(" ")
                                hari = ''.join(hari[0])
                                hari = hari.encode('utf-8')
                                coy = now.strftime("%Y-%m-%d %H:%M").split("-")
                                tanggal = coy[1]
                                tanggal_fix = int(tanggal) - int(hari)
                                tanggal_fix = coy[0] + "-" + str(abs(tanggal_fix)) + "-" + coy[2]
                                update_terakhir = tanggal_fix
                            elif "Hari ini," or "jam lalu" in update_terakhir:
                                try:
                                    hari = update_terakhir.split(",")
                                    hari = hari[1]
                                except:
                                    pass
                                now = datetime.datetime.now()
                                coy = now.strftime("%Y-%m-%d")
                                try:
                                    update_terakhir = coy + hari
                                except:
                                    update_terakhir = coy

                            print "=================================================="
                            # if count == 8 :
                            #     import pdb;pdb.set_trace()
                            akhir = json.dumps(
                                {'type': 'jualo', 'product_url': product_url, 'penjual_url': penjual_url,
                                 'product_name': product, 'harga': harga, 'update_terakhir': update_terakhir,
                                 'kondisi': kondisi, 'dilihat': dilihat, 'deskripsi': deskripsi,'penjual': penjual,'lokasi': lokasi})
                            try:
                                for kaf in range(0, 20):
                                    try:
                                        prod = KafkaProducer(bootstrap_servers=setting.broker)
                                        prod.send(setting.kafka_topic, b"{}".format(akhir))
                                        print "=================================================="
                                        print "SUKSES SEND TO KAFKA"
                                        print "=================================================="
                                        print akhir
                                        status = "done"
                                        sql = "UPDATE jualo_url SET status = '{}' WHERE product_url = '{}'".format(status, url)
                                        cur.execute(sql)
                                        self.conn.commit()
                                        kaf = 1
                                    except:
                                        pass
                                    if kaf == 1:
                                        break
                            except Exception, e:
                                print e
                        except Exception, e:
                            print e
                    except:
                        pass
                except Exception, e:
                    print e
        except Exception, e:
            print e
        cur.close()
        try:
            self.driver.close()
        except Exception, e:
            print e


if __name__ == '__main__':
    p = producer()
    p.parse()
