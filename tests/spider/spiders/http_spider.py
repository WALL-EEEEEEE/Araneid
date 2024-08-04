import logging
from araneid.spider import parser
from araneid.network.http import HttpRequest, HttpResponse
from araneid.spider import Spider

class http_spider(Spider):
    logger = logging.getLogger(__name__)
    url = ''

    async def start_requests(self) -> HttpRequest:
        req = HttpRequest(url=self.url, callback=self.parse)
        yield  req
    
    @parser(name='simple_parse')
    def parse(self, response: HttpResponse):
        self.logger.debug(response.content)