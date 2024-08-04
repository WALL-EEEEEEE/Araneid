import os
import json
from asyncio import Event
from araneid.spider import Spider
from araneid.network.websocket import WebSocketRequest, WebSocketResponse
from araneid.spider import parser

class websocket_spider(Spider): 
    counter = 0
    expected_counter = 0
    wait_close = None
    default_count = 1000
    completed = 0
    default_url = 'ws://127.0.0.1:8081'
    default_interval = None 
    default_connections = 1

    def process_ping(self):
        async def __(ws):
            if not self.wait_close.is_set():
               return
            await ws.close()
        return __

    async def start_requests(self):
        self.logger.info('启动websocket_spider')
        spider_params = os.getenv('spider_params', None)
        if spider_params:
           spider_params = json.loads(spider_params)
        else:
           spider_params = {}
        url = self.default_url if not spider_params.get('url', None) else spider_params.get('url')
        count = self.default_count if not spider_params.get('count', None) else spider_params.get('count')
        connections = self.default_connections if not spider_params.get('connections', None) else spider_params.get('connections')
        interval = self.default_interval if not spider_params.get('interval', None) else spider_params.get('interval')
        self.logger.info(f'url: {url}, count: {count}, connections: {connections}')
        self.wait_close = Event()
        self.counter = 0
        self.expected_counter = count 
        for i in range(connections):
            req = WebSocketRequest(url, on_ping=self.process_ping(), ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
            yield req
    
    @parser(name='parse_message', regex=r'ws://.*')
    async def parse(self, response: WebSocketResponse):
        connection_id = response.attach.get('connection_id')
        counter = response.attach.get('counter')
        counter+=1
        self.counter+=1
        self.logger.info(f'{connection_id} - {counter}/{self.expected_counter}')
        if self.counter == self.expected_counter:
            self.wait_close.set()
        response.attach['counter'] = counter
        
