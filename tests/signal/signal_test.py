import pytest
import asyncio
import logging
import pytest_asyncio
from araneid.core import signal 

logger = logging.getLogger(__name__)

@pytest_asyncio.fixture
async def setup():
    signalmanager = await signal.SignalManager.create()
    signal.set_signalmanager(signalmanager)

async def timeout(coroutine, wait=30):
   return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

@pytest.mark.asyncio
async def test_signal_trigger(setup):
    def signal_handle(signal, source, object):
        print(signal, source, object)
        return tips
    async def signal_trigger():
        signal.register(signal.spider_closed, signal_handle)
        trigger_handle_wait = await signal.trigger(signal.spider_closed, source=None)
        handle_result = await trigger_handle_wait
        await signal.close()
        return  handle_result
    tips = 'spider_close_handle been triggered'
    result, __ = await timeout(asyncio.gather(signal_trigger(), signal.start()))
    assert tips in result


@pytest.mark.asyncio
async def test_signal_multi_handles(setup):
    def signal_handle(signal, source, object):
        return tip
    def signal_handle2(signal, source, object):
        return tip2
    async def signal_trigger():
        result = []
        signal.register(signal.spider_closed, signal_handle)
        signal.register(signal.spider_closed, signal_handle2)
        handle_waiter = await signal.trigger(signal.spider_closed, source=None)
        handle_result = await handle_waiter
        await signal.close()
        return handle_result
    tip = 'spider_handle been triggered'
    tip2 = 'spider_handle2 been triggered'
    result, __ = await timeout(asyncio.gather(signal_trigger(), signal.start()))
    assert {tip, tip2} == set(result)

@pytest.mark.asyncio
async def test_signal_single_handle_multi_trigger(setup):
    def signal_handle(signal, source, object):
        return tip+ object
    async def signal_trigger():
        signal.register(signal.spider_closed, signal_handle)
        trigger_waiter_1 = await signal.trigger(signal.spider_closed, source=None, object='trigger1')
        trigger_waiter_2 = await signal.trigger(signal.spider_closed, source=None, object='trigger2')
        handle_result =  await  trigger_waiter_1
        handle2_result = await  trigger_waiter_2
        await signal.close()
        return set(handle_result+ handle2_result)
    tip = 'spider_handle been triggered '
    trigger1_tip = tip+"trigger1"
    trigger2_tip =  tip+"trigger2"
    expected_returns = {trigger1_tip, trigger2_tip}
    result, __ = await timeout(asyncio.gather(signal_trigger(), signal.start()))
    assert len(expected_returns) == len(set(result))


@pytest.mark.asyncio
async def test_signal_single_handle_multi_signal(setup):
    def signal_handle(signal, source, object):
        return tip+ object

    async def signal_trigger():
        signal.register(signal.spider_started, signal_handle)
        signal.register(signal.spider_closed, signal_handle)
        trigger_waiter_1 = await signal.trigger(signal.spider_started, source=None, object= 'SpiderStart')
        trigger_waiter_2 = await signal.trigger(signal.spider_closed, source=None, object='SpiderClose')
        handle_result =  await  trigger_waiter_1
        handle2_result = await  trigger_waiter_2
        await signal.close()
        return set(handle_result + handle2_result)
    tip = 'spider_handle been triggered '
    trigger1_tip = tip+"SpiderStart"
    trigger2_tip =  tip+"SpiderClose"
    expected_returns = {trigger1_tip, trigger2_tip}
    result, __ = await timeout(asyncio.gather(signal_trigger(), signal.start()))
    assert len(expected_returns) == len(set(result))