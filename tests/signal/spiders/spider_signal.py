from araneid.spider import parser
from araneid.network.http import HttpRequest, HttpResponse
from araneid.crawlers.default import DefaultCrawler

class spider_signal(DefaultCrawler):
    url = 'https://github.com/WALL-EEEEEEE'

    async def start_requests(self) -> HttpRequest:
        req = HttpRequest(url=self.url)
        yield  req
    
    @parser(name='simple_parse', url=url)
    def parse(self, response: HttpResponse):
        self.logger.info('simple_http_spider')
