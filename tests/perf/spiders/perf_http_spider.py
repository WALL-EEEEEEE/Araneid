from araneid.spider import Spider
from araneid.network.http import HttpRequest, HttpResponse
from araneid.spider import parser


class http_spider(Spider): 
    count = 1000
    completed = 0
    url = ''

    async def start_requests(self):
        for v in range(self.count):
            url = self.url+f'?num={v}'
            req = HttpRequest(url, callback=self.parse)
            yield req
    
    @parser(name='parse_message')
    async def parse(self, response: HttpResponse):
        self.completed+=1
        self.logger.debug(self.completed)
