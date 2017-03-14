=======================================================================
Created by Alvin Gong<gongming119@hotmail.com> at 2017/2/14
=======================================================================

Descriptions:
    The project combines Scrapy, Kafka and MongoDB to crawl information of houses on http://bj.lianjia.com/ershoufang in a distributed manner.
    
Workflow:
    1. HomeLinkPageSpider opens the index page and fetch the links of all the houses
    2. HomeLinkPageSpider sends the links to Kafka server
    3. HomelinkSellingSpider is launched in different machines and listens to the Kafka server
    4. HomelinkSellingSpiders fetch detailed information for each house 
    5. Insert house items to MongoDB