from araneid.spider import parser
from araneid.network.http import HttpRequest, HttpResponse
from araneid.crawlers.default import DefaultCrawler

class request_signal(DefaultCrawler):
    url = 'https://github.com/WALL-EEEEEEE'

    async def signal_handle(self, request, spider):
        self.logger.info(f'Spider {spider} Request {request} has reached slot.')
        return spider, request

    async def start_requests(self) -> HttpRequest:
        req = HttpRequest(url=self.url)
        yield  req
    
    @parser(name='simple_parse', url=url)
    def parse(self, response: HttpResponse):
        self.logger.info('simple_http_spider')

