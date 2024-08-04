import pytest
import asyncio
from araneid.core.context import RequestContext
from araneid.core.flags import Idle
from araneid.core.engine import Engine
from araneid.core.slot import Slot
from araneid.core import signal
from araneid.scraper import Scraper
from araneid.setting import settings as setting_loader
from araneid.network.http import HttpRequest
from araneid.spider.spider import Spider

class engine_spider(Spider):
   pass

slot: Slot = None
engine: Engine = None
scraper: Scraper = None
spider: Spider = None

def setup_function():
   global slot, engine, scraper, spider
   config_settings = setting_loader.get('core', {})
   signalmanager = signal.SignalManager()
   signal.set_signalmanager(signalmanager)
   slot = Slot()
   engine = Engine.from_settings(settings=config_settings)
   scraper = Scraper.from_settings(settings=config_settings)
   spider = engine_spider()

def mock_spider_request(request):
    request.context = RequestContext(spider=spider)
 
async def timeout(coroutine, wait=30):
        return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)


def test_engine_idle_support_idle_flag():
    assert isinstance(engine.idle(Idle.DOWNLOADERMANAGER|Idle.SCHEDULEMANAGER|Idle.SLOTMANAGER), bool)
    
def test_engine_idle_not_support_idle_flag():
    with pytest.raises(AssertionError):
        engine.idle(Idle.SLOT)
    with pytest.raises(AssertionError):
        engine.idle(Idle.SLOT|Idle.DOWNLOADERMANAGER)

@pytest.mark.asyncio
async def test_wait_idle_engine_returns(aioresponse):
    async def wait_idle():
        scraper.bind(slot)
        scraper.add_spider(spider)
        await engine.add_slot(slot)
        await scraper.process_request(request=request)
        await engine.wait_idle(Idle.SCHEDULEMANAGER | Idle.SIGNALMANAGER | Idle.SLOTMANAGER | Idle.DOWNLOADERMANAGER)
    wait_idle_coro = asyncio.ensure_future(wait_idle())
    scraper_start_coro = asyncio.ensure_future(scraper.start())
    engine_start_coro = asyncio.ensure_future(engine.start())
    request_url = 'https://github.com/WALL-EEEEEEE'
    aioresponse.get(request_url, status=200, payload=request_url, repeat=True)
    request: HttpRequest = HttpRequest(url=request_url)
    mock_spider_request(request)
    await  asyncio.gather(*[wait_idle_coro, scraper_start_coro, engine_start_coro])


@pytest.mark.slow  
@pytest.mark.asyncio
async def test_wait_idle_engine_block(aioresponse):
    async def wait_idle():
        scraper.bind(slot)
        scraper.add_spider(spider)
        await engine.add_slot(slot)
        await scraper.process_request(request=request)
        await engine.wait_idle(Idle.SCHEDULEMANAGER | Idle.SIGNALMANAGER | Idle.SLOTMANAGER | Idle.DOWNLOADERMANAGER)
    request_url = 'https://github.com/WALL-EEEEEEE'
    aioresponse.get(request_url, status=200, payload=request_url, repeat=True)
    request: HttpRequest = HttpRequest(url=request_url)
    with pytest.raises(asyncio.TimeoutError):
        await timeout(wait_idle())