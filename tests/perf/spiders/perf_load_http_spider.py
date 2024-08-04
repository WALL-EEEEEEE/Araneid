import os
import asyncio
import json
from araneid.spider import Spider
from araneid.network.http import HttpRequest, HttpResponse
from araneid.spider import parser

class http_spider(Spider): 
    default_count = 100
    completed = 0
    default_url = 'http://127.0.0.1:8080'
    default_interval = None 

    async def start_requests(self):
        self.logger.info('启动http_spider')
        spider_params = os.getenv('spider_params', None)
        if spider_params:
           spider_params = json.loads(spider_params)
        else:
           spider_params = {}
        url = self.default_url if not spider_params.get('url', None) else spider_params.get('url')
        count = self.default_count if not spider_params.get('count', None) else spider_params.get('count')
        interval = self.default_interval if not spider_params.get('interval', None) else spider_params.get('interval')
        self.logger.debug(f'url: {url}, count: {count}, interval: {interval}')
        for v in range(count):
            req_url = url+f'?num={v}'
            req = HttpRequest(req_url, callback=self.parse)
            yield req
            if interval:
               await asyncio.sleep(interval)
   
    @parser(name='parse_message')
    async def parse(self, response: HttpResponse):
        self.completed+=1
        self.logger.debug(self.completed)