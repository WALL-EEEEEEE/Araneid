import logging
from araneid.spider import parser
from araneid.network.websocket import WebSocketRequest, WebSocketResponse
from araneid.spider import Spider

class websocket_spider(Spider):
    logger = logging.getLogger(__name__)
    url = 'https://github.com/WALL-EEEEEEE'

    async def start_requests(self) -> WebSocketRequest:
        req = WebSocketRequest(url=self.url)
        yield  req
    
    @parser(name='simple_parse', url=url)
    def parse(self, response: WebSocketResponse):
        self.logger.info('websocket_spider')