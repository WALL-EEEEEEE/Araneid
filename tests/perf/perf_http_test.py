import logging
import pytest
import asyncio
import re
from .spiders.perf_http_spider import http_spider


logger = logging.getLogger()



test_request_process_group= {
    "request=10": pytest.param(*(10, 1), marks=[]), #(message_count, send_message_cost_time, connections, total_cost_time)
    "request=100": pytest.param(*(100, 5), marks=[]), #(message_count, send_message_cost_time, connections, total_cost_time)
    "request=1000": pytest.param(*(1000, 10), marks=[]), #(message_count, send_message_cost_time, connections, total_cost_time)
    "request=10000": pytest.param(*(10000, 70), marks=[]),
}

async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)


    
@pytest.mark.parametrize("request_time, runtime", list(test_request_process_group.values()), ids=list(test_request_process_group.keys()))
@pytest.mark.asyncio
async def test_request_process(request_time,runtime, aioresponse, async_runner):
    mock_url = 'http://mock.spider.com'
    mock_url_regex = re.compile('http:\/\/mock.spider.com.*')
    spider = http_spider()
    spider.url = mock_url
    spider.count = request_time
    aioresponse.get(mock_url_regex, status=200, payload={'code': 200, 'status':'Success'}, repeat=True)
    async_runner.add_spider(spider)
    await timeout(async_runner.start(), wait=runtime)