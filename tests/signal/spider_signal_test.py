import asyncio
import logging
import pytest
import pytest_asyncio
from araneid.core import  signal
from araneid.scraper import Scraper
from araneid.spider import Spider
from .spiders.spider_signal import spider_signal


logger = logging.getLogger(__name__)

def signal_handler(result):
    async def handler(signal, source, object):
        result['signal'] =  signal
        result['source'] = source
        result['object'] = object
    return handler

async def timeout( coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

@pytest.mark.asyncio
async def test_spider_started(aioresponse, async_runner):
    result = {}
    aioresponse.get(spider_signal.url, status=200, payload=spider_signal.url, repeat=True)
    signal.register(signal.spider_started, signal_handler(result))
    async_runner.add_spider(spider_signal)
    #await timeout(async_runner.start())
    await async_runner.start()
    assert result['signal'] == signal.spider_started
    assert isinstance(result['source'], Scraper)
    assert isinstance(result['object'], Spider)

@pytest.mark.asyncio
async def test_spider_closed(aioresponse, async_runner):
    result = {}
    aioresponse.get(spider_signal.url, status=200, payload=spider_signal.url, repeat=True)
    signal.register(signal.spider_closed, signal_handler(result))
    async_runner.add_spider(spider_signal)
    await timeout(async_runner.start())
    assert result['signal'] == signal.spider_closed
    assert isinstance(result['source'], Scraper)
    assert isinstance(result['object'], Spider)