import asyncio
import logging
import pytest
from araneid.core.request import Request
from araneid.core import signal
from araneid.core.engine import Engine
from araneid.core.downloader import Downloader
from araneid.scraper import Scraper
from .spiders.request_signal import request_signal



logger = logging.getLogger(__name__)

def signal_handler(result):
    async def handler(signal, source, request):
        result['signal'] =  signal
        result['source'] = source
        result['object'] = request
    return handler

async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

@pytest.mark.asyncio
async def test_request_reached_slot(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.request_reached_slot, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert result['signal'] == signal.request_reached_slot
    assert isinstance(result['source'], Scraper)
    assert isinstance(result['object'], Request)

@pytest.mark.asyncio
async def test_request_left_slot(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.request_left_slot, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert result.get('signal') == signal.request_left_slot
    assert isinstance(result.get('source'), Engine)
    assert isinstance(result.get('object'), Request)

@pytest.mark.asyncio
async def test_request_scheduled(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.request_scheduled, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert result.get('signal') == signal.request_scheduled
    assert isinstance(result.get('source'), Engine)
    assert isinstance(result.get('object'), Request)

@pytest.mark.asyncio
async def test_request_reached_downloader(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.request_reached_downloader, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert result.get('signal') == signal.request_reached_downloader
    assert isinstance(result.get('source'), Downloader)
    assert isinstance(result.get('object'), Request)


@pytest.mark.asyncio
async def test_bytes_received(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.bytes_received, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert result.get('signal') == signal.bytes_received
    assert isinstance(result.get('source'), Downloader)
    assert isinstance(result.get('object', {}).get('request'), Request)
    assert isinstance(result.get('object', {}).get('bytes'), bytes)

@pytest.mark.asyncio
async def test_request_left_downloader(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.request_left_downloader, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert result.get('signal') == signal.request_left_downloader
    assert isinstance(result.get('source'), Downloader)
    assert isinstance(result.get('object'), Request)


@pytest.mark.skip(reason="Test case not prepared")
async def test_request_dropped(aioresponse, async_runner):
    result = {}
    aioresponse.get(request_signal.url, status=200, payload=request_signal.url, repeat=True)
    signal.register(signal.request_dropped, signal_handler(result))
    async_runner.add_spider(request_signal)
    await timeout(async_runner.start())
    assert (result.get('signal') == signal.request_dropped)
    assert isinstance(result.get('source'), object)
    assert isinstance(result.get('object'), Request)