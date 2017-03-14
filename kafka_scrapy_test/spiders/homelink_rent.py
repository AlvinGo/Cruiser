# -*- coding: utf-8 -*-
import scrapy
from scrapy.utils.project import get_project_settings
from kafka_scrapy.items import HomeLinkItemLoader, HomeLinkItem, LinkItem
from scrapy.spiders.init import InitSpider
from scrapy.shell import inspect_response
from pykafka import KafkaClient
from pykafka.exceptions import ConsumerStoppedException
from scrapy.exceptions import CloseSpider, DontCloseSpider
from scrapy import signals

class HomeLinkPageSpider(InitSpider):
    """
        The spider is used to crawl the index pages, and then pass the links of each houses to Kafka.
            1. Read the house list pages and grab links for each house item.
            2. The links for each house item will be sent to Kafka.
                The other machines listen to the Kafka topic and crawl the links as a Consumer Group.
    """
    name = "homelink_houselist"
    default_house_count = 30        # Default house item count in each index pages. It is used to calculate the total page number.
    index_url_template = 'http://bj.lianjia.com/ershoufang/tongzhou/pg{0}/'
    start_urls = []
    
    def __init__(self, param={'entry_url': 'http://bj.lianjia.com/ershoufang/tongzhou/'}):
        self.entry_url = param['entry_url']
        self.settings = get_project_settings()
    
    def init_request(self):
        return scrapy.Request(self.entry_url, callback=self.generate_index_page_list)
    
    def generate_index_page_list(self, response):
        """
            1. Get the total number of houses
            2. Calculate the number of index pages
            3. Generate the start_urls
        """
        total_houses_str = response.css('h2.total.fl span::text').extract_first().strip()
        page_count = int(int(total_houses_str) / self.default_house_count + 1)
        for i in range(1, page_count + 1):
            index_url = self.index_url_template.format(str(i))
            self.start_urls.append(index_url)
        return self.initialized()
    
    def parse(self, response):
        """
            1. Fetch links for each houses
            2. Send the links to Kafka for consuming
        """
        links = response.css('ul.sellListContent li div.title a::attr("href")').extract()
        for link in links:
            print(link)
            link_item = LinkItem()
            link_item['url'] = link
            yield link_item

class HomelinkSellingSpider(scrapy.Spider):
    """
        The spider listens to the Kafka server, and consume the links produced by HomeLinkPageSpider.
    """
    
    name = "homelink_selling_house"
    
    def set_crawler(self, crawler):
        super(HomelinkSellingSpider, self).set_crawler(crawler)

    def setup_kafka(self, settings):
        self.logger.debug("Setup Kafka client...")
        kafka_server = settings['KAFKA_SERVER']
        kafka_port = settings['KAFKA_PORT']
        kafka_hosts = kafka_server + ':' + str(kafka_port)
        self.kafka_client = KafkaClient(hosts=kafka_hosts)
        self.topic = self.kafka_client.topics[settings['KAFKA_TOPIC_HOMELINK_SELLING'].encode('utf-8')]
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=self.settings['KAFKA_CONSUMER_GROUP'].encode('utf-8'),
            auto_commit_enable=True,
            zookeeper_connect=self.settings['ZOOKEEPER_SERVER'] + ':' + str(self.settings['ZOOKEEPER_PORT'])
        )
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def process_kafka_message(self, msg):
        """
            Deal with the messages received from Kafka producer
        """
        if msg is not None:
            msg_value = msg.value.decode('utf-8')
            if len(msg_value) > 5 and msg_value[0:4].lower() == 'http':
                return msg_value
            elif msg_value == 'exit7':
                self.logger.info('Received Exit message. Spider will close.')
                self.consumer.stop()
                raise CloseSpider
            else:
                self.logger.info("Received plain text message: " + msg_value)
        return None

    def next_request(self):
        """
            Obtain message from Kafka producer and generate request accordingly
        """
        try:
            msg = self.consumer.consume()
            url = self.process_kafka_message(msg)
            if not url:
                return None
            else:
                return self.make_requests_from_url(url)
        except CloseSpider:
            self.logger.info('Spider is closed')
        except ConsumerStoppedException:
            self.logger.info('Kafka stopped to consume message')
            self.consumer.stop()
        return None

    def schedule_next_request(self):
        req = self.next_request()
        if req:
            self.crawler.engine.crawl(req, spider=self)
        else:
            return None

    def spider_idle(self):
        if self.schedule_next_request():
            raise DontCloseSpider
        else:
            return None

    def item_scraped(self):
        if not self.schedule_next_request():
            return None

    def parse(self, response):
        self.logger.debug("User agent: {0}".format(response.request.headers['User-Agent']))
        try:
            # Get house properties defined in the detailed page
            house_loader = HomeLinkItemLoader(item=HomeLinkItem(), response=response)
            house_loader.add_css('title', 'div.title h1.main::text')
            house_loader.add_css('price', 'div.price span.total::text')
            house_loader.add_css('district', 'div.areaName span.info a::text')
            house_loader.add_css('cell', 'div.communityName a.info::text')
            house_loader.add_css('rooms', 'div.room div.mainInfo::text')
            house_loader.add_css('floor', 'div.room div.subInfo::text')
            house_loader.add_css('direction', 'div.type div.mainInfo::text')
            house_loader.add_css('area', 'div.area div.mainInfo::text')
            house_loader.add_css('year', 'div.area div.subInfo::text')
            house_loader.add_css('house_id', 'div.aroundInfo div.houseRecord span.info::text')
            return house_loader.load_item()
        except:
            # Open the Scrapy shell and see what's happening when there's any exception
            inspect_response(response, self)

if __name__ == '__main__':
    from scrapy.crawler import CrawlerProcess
    settings = get_project_settings()
    print(settings.get('BOT_NAME'))

    crawler_process = CrawlerProcess(settings)
    entry_url = 'http://bj.lianjia.com/ershoufang/tongzhou/'
    crawler_process.crawl("homelink_selling_house")
    # crawler_process.crawl("homelink_houselist")
    crawler_process.start()
    print('Crawling finished.')
