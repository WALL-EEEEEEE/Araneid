import logging
from typing import Generator
from araneid.data.item import Type

from araneid.spider import parser, item
from araneid.item import ItemType
from araneid.network.http import HttpRequest, HttpResponse
from araneid.crawlers.default import DefaultCrawler

class old_style_item_spider(DefaultCrawler):
    logger = logging.getLogger(__name__)
    url = 'https://github.com/WALL-EEEEEEE'
    parser_name= 'simple_parser'

    async def start_requests(self)-> Generator[HttpRequest, None, None]:
        req = HttpRequest(url=self.url)
        yield  req
    
    @item(name='content', type=ItemType.TEXT, require=True)
    @item(name='url', type=ItemType.URL, require=False)
    @parser(name=parser_name, url=url)
    def parse(self, response: HttpResponse):
        self.items['url'].value = response.request.url
        self.items['content'].value = response.text.strip('"')
        self.logger.info(f'{self.__class__.__name__}')

class new_style_item_spider(DefaultCrawler):
    logger = logging.getLogger(__name__)
    url = 'https://github.com/WALL-EEEEEEE'
    parser_name = 'simple_parse'

    async def start_requests(self)-> Generator[HttpRequest, None, None]:
        req = HttpRequest(url=self.url)
        yield  req
    
    @item(name='content', type=Type.TEXT, require=True)
    @item(name='url', type=Type.URL, require=False)
    @parser(name=parser_name, url=url)
    def parse(self, response: HttpResponse):
        self.items['url'] = response.request.url
        self.items['content'] = response.text.strip('"')
        self.logger.info(f'{self.__class__.__name__}')
