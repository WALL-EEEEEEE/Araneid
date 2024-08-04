from asyncio import Event
from araneid.spider import Spider
from araneid.network.websocket import WebSocketRequest, WebSocketResponse
from araneid.spider import parser



class websocket_spider(Spider): 
    websocket_url = 'ws://127.0.0.1:8080'
    counter = 0
    connections = 5
    expected_counter = 3000*5
    wait_close = None

    def process_ping(self):
        async def __(ws):
            if not self.wait_close.is_set():
               return
            await ws.close()
        return __

    async def start_requests(self):
        self.wait_close = Event()
        for i in range(self.connections):
            req = WebSocketRequest(self.websocket_url, on_ping=self.process_ping(), ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
            yield req
        stats_counter = getattr(self, 'stats_counter', None)
        if stats_counter:
           await self.stats_counter.wait()
    
    @parser(name='parse_message', regex=r'ws://.*')
    async def parse(self, response: WebSocketResponse):
        connection_id = response.attach.get('connection_id')
        counter = response.attach.get('counter')
        counter+=1
        self.counter+=1
        self.logger.debug(f'{connection_id} - {counter}')
        if self.counter == self.expected_counter:
            self.wait_close.set()
        response.attach['counter'] = counter
        