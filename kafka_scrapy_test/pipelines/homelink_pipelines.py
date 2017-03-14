# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DropItem
from pykafka import KafkaClient

class HomeLinkHouseListPipeline(object):
    
    spider_name = 'homelink_houselist'
    
    def open_spider(self, spider):
        if spider.name == self.spider_name:
            spider.logger.info("Initializing Kafka Producer...")
            self.setup_kafka_producer(spider)
    
    def setup_kafka_producer(self, spider):
        kafka_server = spider.settings['KAFKA_SERVER']
        kafka_port = str(spider.settings['KAFKA_PORT'])
        kafka_hosts = kafka_server + ':' + kafka_port
        self.kafka_client = KafkaClient(hosts=kafka_hosts)
        self.topic = self.kafka_client.topics[spider.settings['KAFKA_TOPIC_HOMELINK_SELLING'].encode('utf-8')]
        self.producer = self.topic.get_sync_producer()
    
    def process_item(self, item, spider):
        if spider.name != self.spider_name:
            return item
        spider.logger.debug(item['url'])
        self.producer.produce(item['url'].encode('utf-8'))
        return item

    def close_spider(self, spider):
        if spider.name == self.spider_name:
            spider.logger.info("Stop Kafka Producer...")
            self.producer.stop()

class HomeLinkSellingPipeline(object):
    """
        Validate the fetched items and insert it into MongoDB
    """
    
    spider_name = 'homelink_selling_house'
    
    def __init__(self):
        self.settings = get_project_settings()
        self.client = MongoClient(
            self.settings['MONGODB_SERVER'],
            self.settings['MONGODB_PORT']
        )
        self.db = self.client[self.settings['MONGODB_DB']]
        self.collection = self.db[self.settings['MONGODB_COLLECTION']]

    def open_spider(self, spider):
        if spider.name == self.spider_name:
            spider.logger.info(self.spider_name + " opened...")
            spider.logger.info("Initializing Kafka for " + spider.name)
            spider.setup_kafka(self.settings)

    def process_item(self, item, spider):
        # Only deal with the 'homelink_spider'
        if spider.name != self.spider_name:
            return item
        if 'price' in item and item['price']:
            print(item['title'])
            self.collection.insert_one(dict(item))
            pass
        else:
            spider.logger.error('Failed to validate item.')
            raise DropItem
    
    def close_spider(self, spider):
        # Close the MongoDB connection
        self.client.close()
        pass