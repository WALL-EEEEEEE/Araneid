import logging
from araneid.spider import parser
from araneid.network.socket import SocketRequest, SocketResponse
from araneid.spider import Spider

class socket_spider(Spider):
    logger = logging.getLogger(__name__)
    url = 'https://github.com/WALL-EEEEEEE'

    async def start_requests(self) -> SocketRequest:
        req = SocketRequest(url=self.url)
        yield  req
    
    @parser(name='simple_parse', url=url)
    def parse(self, response: SocketResponse):
        self.logger.info('socket_spider')