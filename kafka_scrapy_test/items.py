# -*- coding: utf-8 -*-

import scrapy
from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy.loader import ItemLoader
from scrapy import Field

class LinkItem(scrapy.Item):
    """
        Represents the link for each house item.
    """
    url = Field()

class HomeLinkItem(scrapy.Item):
    """
        Store detailed information of each house
    """
    title = Field()
    price = Field()
    district = Field()
    cell = Field()
    rooms = Field()
    floor = Field()
    direction = Field()
    area = Field()
    year = Field()
    house_id = Field()


class HomeLinkItemLoader(ItemLoader):
    """
        Define the default input/output behavior for HomeLinkItem
    """
    default_item_class = HomeLinkItem
    default_input_processor = MapCompose(lambda s: s.strip())
    default_output_processor = TakeFirst()
