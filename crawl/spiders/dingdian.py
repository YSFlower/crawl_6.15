import re
import scrapy
from bs4 import BeautifulSoup
from scrapy.http import Request
from crawl.items import DingdianItem
from ..mysqlpipelines.sql import Sql

class MySpider(scrapy.Spider):
    name = 'crawl'
    allowed_domains = ['23us.com']
    bash_url = 'http://www.23us.com/class/'
    # 'http://www.23us.com/class/1_1.html'
    bashurl = '.html'

    def start_requests(self):
        for i in range(1,11):
            url = self.bash_url + str(i) + '_1' + self.bashurl
            yield Request(url,self.parse)

    def parse(self,response):
        max_num = BeautifulSoup(response.text,'lxml').find('div',class_="pagelink").find_all('a')[-1].get_text()
        bash_url = str(response.url)[:-7]
        for num in range(1,int(max_num)+1):
            url = bash_url + '_' + str(num) + self.bashurl
            yield Request(url,callback=self.get_name)

    def get_name(self,response):
        tds = BeautifulSoup(response.text,'lxml').find_all('tr',bgcolor = "#FFFFFF")
        for td in tds:
            novelname = td.find_all('a')[1].get_text()
            novelurl = td.find('a')['href']
            yield Request(novelurl,callback= self.get_chapterurl,meta={'name' : novelname,'url':novelurl})


    def get_chapterurl(self,response):
        item = DingdianItem()
        item['name'] = str(response.meta['name']).replace('\xa0','')
        item['novelurl'] = response.meta['url']
        category = BeautifulSoup(response.text,'lxml').find('table').find('td').get_text()
        author = BeautifulSoup(response.text,'lxml').find('table').find('tr').find_all('td')[1].get_text()
        bash_url = BeautifulSoup(response.text,'lxml').find('p',class_="btnlinks").find('a',class_="read")['href']
        name_id = str(bash_url)[-6:-1].replace('/','')
        serialstatus = BeautifulSoup(response.text,'lxml').find('table').find_all('td')[2].get_text()
        serialnumber = BeautifulSoup(response.text,'lxml').find_all('tr')[1].find_all('td')[1].get_text()
        item['category'] = str(category).replace('\xa0','')
        item['author'] = str(author).replace('/','').replace('\xa0','')
        item['name_id'] = name_id
        item['serialStatus'] = str(serialstatus).replace('\xa0','')
        item['serialNumber'] = str(serialnumber).replace('\xa0','')
        yield item
        yield Request(url=bash_url, callback=self.get_chapter, meta={'name_id': name_id})

        def get_chapter(self, response):
            urls = re.findall(r'<td class="L"><a href="(.*?)">(.*?)</a></td>', response.text)
            num = 0
            for url in urls:
                num = num + 1
                chapterurl = response.url + url[0]
                chaptername = url[1]
                rets = Sql.sclect_chapter(chapterurl)
                if rets[0] == 1:
                    print('章节已经存在了')
                    return False
                else:
                    yield Request(chapterurl, callback=self.get_chaptercontent, meta={'num': num,
                                                                                      'name_id': response.meta[
                                                                                          'name_id'],
                                                                                      'chaptername': chaptername,
                                                                                      'chapterurl': chapterurl
                                                                                      })

        def get_chaptercontent(self, response):
            item = DcontentItem()
            item['num'] = response.meta['num']
            item['id_name'] = response.meta['name_id']
            item['chaptername'] = str(response.meta['chaptername']).replace('\xa0', '')
            item['chapterurl'] = response.meta['chapterurl']
            content = BeautifulSoup(response.text, 'lxml').find('dd', id='contents').get_text()
            item['chaptercontent'] = str(content).replace('\xa0', '')
            yield item

