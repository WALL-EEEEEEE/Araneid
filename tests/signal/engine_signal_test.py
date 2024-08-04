import asyncio
import logging
from numpy import isin
import pytest
from araneid.core.engine import Engine
from araneid.core import signal
from araneid.spider import Spider
from .spiders.spider_signal import spider_signal



logger = logging.getLogger(__name__)

async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)


def signal_handler(result):
    async def handler(signal, source, object):
        result['signal'] =  signal
        result['source'] = source
        result['object'] = object
    return handler

@pytest.mark.asyncio
async def test_engine_started(aioresponse, async_runner):
    result = {}
    aioresponse.get(spider_signal.url, status=200, payload=spider_signal.url, repeat=True)
    signal.register(signal.engine_started, signal_handler(result))
    async_runner.add_spider(spider_signal)
    await timeout(async_runner.start())
    assert result['signal'] == signal.engine_started
    assert isinstance(result['source'], Engine)
    assert result['object'] is None


@pytest.mark.asyncio
async def test_engine_closed(aioresponse, async_runner):
    result = {}
    aioresponse.get(spider_signal.url, status=200, payload=spider_signal.url, repeat=True)
    signal.register(signal.engine_closed, signal_handler(result))
    async_runner.add_spider(spider_signal)
    await timeout(async_runner.start())
    assert result['signal'] == signal.engine_closed
    assert  isinstance(result['source'], Engine)
    assert  result['object'] is None