import asyncio
import pytest
import logging
from araneid.core.flags import Idle
from araneid.core.slot import Slot
from araneid.spider import Spider
from araneid.core import response, signal 
from araneid.core.engine import Engine
from araneid.core.context import RequestContext
from araneid.network.http import HttpRequest, HttpResponse
from araneid.core.exception import SlotUnbound 
from araneid.setting import settings as setting_loader
from araneid.scraper import Scraper
from aioresponses import aioresponses


logger = logging.getLogger(__name__)
slot: Slot = None
engine: Engine = None
scraper: Scraper = None

def setup_function():
    global slot, engine, scraper
    config_settings = setting_loader.get('core', {})
    signalmanager = signal.SignalManager()
    signal.set_signalmanager(signalmanager)
    slot  = Slot()
    engine = Engine.from_settings(settings=config_settings)
    scraper  = Scraper.from_settings(settings=config_settings)

def teardown_function():
    global slot, engine, scraper
    slot = None
    engine = None
    scraper = None

"""
def mock_spider_request(request):
    request.context = RequestContext(spider=spider)
"""

async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

def test_slot_idle_support_idle_flag():
    slot.bind(scraper=scraper, engine=engine)
    assert isinstance(slot.idle(Idle.DOWNLOADERMANAGER|Idle.SCHEDULEMANAGER|Idle.SLOTMANAGER|Idle.SLOT|Idle.SIGNALMANAGER), bool)

@pytest.mark.slow
@pytest.mark.asyncio
async def test_unbound_slot_get_response():
    async def get_response():
        slot.bind(engine=engine, scraper=scraper)
        await slot.put_response(response)
        slot.unbind()
        await slot.get_response(spider=False)
    response :HttpResponse = HttpResponse(200, 'test')
    with pytest.raises(SlotUnbound):
         await timeout(get_response())


@pytest.mark.asyncio
async def test_unbound_slot_put_response():
    async def put_response():
        await slot.put_response(response)
    response :HttpResponse = HttpResponse(200, 'test')
    with pytest.raises(SlotUnbound):
         await timeout(put_response())


@pytest.mark.slow 
@pytest.mark.asyncio
async def test_unbound_slot_get_request():
    async def get_request():
        slot.bind(engine=engine, scraper=scraper)
        await slot.put_request(request)
        slot.unbind()
        await slot.get_request(spider=False)
    request :HttpRequest = HttpRequest(url='https://github.com/WALL-EEEEEEE')
    with pytest.raises(SlotUnbound):
         await timeout(get_request())

@pytest.mark.asyncio
async def test_unbound_slot_put_request():
    async def get_request():
        await slot.put_request(request)
    request :HttpRequest = HttpRequest(url='https://github.com/WALL-EEEEEEE')
    with pytest.raises(SlotUnbound):
         await timeout(get_request())

@pytest.mark.asyncio
async def test_unbound_slot_open():
    async def open():
        await slot.open()
    with pytest.raises(SlotUnbound):
         await timeout(open())

@pytest.mark.asyncio
async def test_unbound_slot_set_open():
    async def set_open():
        await slot.set_open()
    with pytest.raises(SlotUnbound):
         await timeout(set_open())

@pytest.mark.asyncio
async def test_unbound_slot_wait_close():
    async def wait_close():
        await slot.wait_close()
    with pytest.raises(SlotUnbound):
         await timeout(wait_close())


@pytest.mark.slow
@pytest.mark.asyncio
async def test_unbound_slot_wait_idle():
    async def wait_idle():
        await slot.wait_idle()
    with pytest.raises(SlotUnbound):
         await timeout(wait_idle())

@pytest.mark.asyncio
async def test_unbound_slot_complete_request():
    async def complete_request():
        await slot.put_request(request=request)
        await slot.complete_request(request=request)
    request: HttpRequest = HttpRequest(url='https://github.com/WALL-EEEEEEE')
    with pytest.raises(SlotUnbound):
         await timeout(complete_request())

@pytest.mark.asyncio
async def test_unbound_slot_complete_response():
    async def complete_response():
        await slot.put_response(response=response)
        await slot.complete_response(response=response)
    response: HttpResponse= HttpResponse(content=b'', status=200)
    with pytest.raises(SlotUnbound):
         await timeout(complete_response())

@pytest.mark.asyncio
async def test_unbound_slot_is_completed():
    async def is_completed():
        await slot.put_response(response=response)
        await slot.is_completed(response=response)
    response: HttpResponse= HttpResponse(content=b'', status=200)
    with pytest.raises(SlotUnbound):
         await timeout(is_completed())

@pytest.mark.asyncio
async def test_unbound_slot_is_open():
    async def is_open():
        await slot.is_open()
    with pytest.raises(SlotUnbound):
         await timeout(is_open())

@pytest.mark.asyncio
async def test_unbound_slot_idle():
    async def idle():
        slot.idle()
    with pytest.raises(SlotUnbound):
         await timeout(idle())

@pytest.mark.asyncio
async def test_unbound_request_idle():
    async def request_idle():
        slot.request_idle()
    with pytest.raises(SlotUnbound):
         await timeout(request_idle())

@pytest.mark.asyncio
async def test_unbound_response_idle():
    async def response_idle():
         slot.response_idle()
    with pytest.raises(SlotUnbound):
         await timeout(response_idle())

@pytest.mark.asyncio
async def test_unbound_join():
    async def join():
        await slot.join()

    with pytest.raises(SlotUnbound):
         await timeout(join())

@pytest.mark.asyncio
async def test_unbound_close():
    with pytest.raises(SlotUnbound):
         await slot.close()

def test_unbound_is_close():
    with pytest.raises(SlotUnbound):
         slot.is_close()

@pytest.mark.asyncio
async def test_open():
    async def open():
        slot.bind(scraper, engine)
        await slot.set_open()
        return await slot.open()
    result = await timeout(open())
    assert True == result

@pytest.mark.slow
@pytest.mark.asyncio
async def test_get_request():
    async def get_request():
        slot.bind(scraper, engine)
        await slot.put_request(request)
        return await slot.get_request(spider=False)
    request: HttpRequest = HttpRequest(url='https://github.com/WALL-EEEEEEE')
    result = await timeout(get_request())
    request == result

@pytest.mark.slow
@pytest.mark.asyncio
async def test_wait_idle_slot_block():
    async def wait_idle():
        slot.bind(scraper, engine)
        await slot.wait_idle(flag=Idle.SLOT)
    with pytest.raises(asyncio.TimeoutError):
        await  timeout(wait_idle())

@pytest.mark.asyncio
async def test_wait_idle_slot_returns(aioresponse):
    async def wait_idle():
        scraper.bind(slot)
        await engine.add_slot(slot)
        await slot.put_request(request=request)
        await slot.wait_idle(flag=Idle.SLOT)
    scraper_start_coro = asyncio.ensure_future(scraper.start())
    engine_start_coro = asyncio.ensure_future(engine.start())
    wait_idle_coro = asyncio.ensure_future(wait_idle())
    request_url = 'https://github.com/WALL-EEEEEEE'
    aioresponse.get(request_url, status=200, payload=request_url, repeat=True)
    request: HttpRequest = HttpRequest(url=request_url)
    await timeout(asyncio.gather(*[wait_idle_coro, scraper_start_coro, engine_start_coro]))

@pytest.mark.skip(reason='not prepared')
@pytest.mark.asyncio
async def test_wait_get_request_after_close_slot(aioresponse):
    async def consumer():
        scraper.bind(slot)
        await engine.add_slot(slot)
        await slot.close()
        await slot.get_request()
    scraper_start_coro = asyncio.ensure_future(scraper.start())
    engine_start_coro = asyncio.ensure_future(engine.start())
    request = HttpRequest(url='https://www.baidu.com')
    await timeout(asyncio.gather(*[scraper_start_coro, engine_start_coro,consumer()]))