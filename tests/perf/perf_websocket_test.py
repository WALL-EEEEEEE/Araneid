import logging
import asyncio
import pytest
import time
from araneid.core.signal import register, response_received, response_parsed
from araneid.util._async import BoundedCountdownLatch
from .spiders.websocket_spider import  websocket_spider

logger = logging.getLogger()



async def websocket_stats_sample(spider):
    def stats_collector():
        def __(signal, source, response):
            if signal == response_received:
               resp_id = int(response.text)
               spider.stats.set_value(f'response_track.{resp_id}.received', time.time())
            elif signal == response_parsed:
               resp_id = int(response.text)
               spider.stats.set_value(f'response_track.{resp_id}.parsed', time.time())
               stats_counter = getattr(spider, 'stats_counter', None)
               if stats_counter:
                  stats_counter.decrement()
        return __
    register(response_received, stats_collector())
    register(response_parsed, stats_collector())
    
async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

test_process_message_count_group = {
    "message=1000, connections=1": pytest.param(*(1000, 10, 1, 15), marks=[pytest.mark.dependency(name='message_count_1000_1')]), #(message_count, send_message_cost_time, connections, total_cost_time)
    "message=10000,connections=1": pytest.param(*(10000, 10, 1, 60), marks=[pytest.mark.dependency(name='message_count_10000_1', depends=['message_count_3000_1'])]),
    "message=1000, connections=5": pytest.param(*(1000, 10, 5, 360), marks=[pytest.mark.dependency(name='message_count_1000_5', depends=['message_count_3000_1'])]),
    "message=10000,connections=5": pytest.param(*(10000, 10, 5, 600), marks=[pytest.mark.dependency(name='message_count_10000_5', depends=['message_count_10000_1'])]) ,
}
test_process_message_perf_group = {
    "message=1000, connections=1": pytest.param(*(1000, 10, 1, 240, 0.05, 15), marks=[pytest.mark.dependency(name='message_perf_1000_1', depends=['message_count_3000_1'])]),  #(message_count, send_message_cost_time, connections, mean_cost_time, total_cost_time)
    "message=10000, connections=1": pytest.param(*(10000, 10, 1, 240, 0.05, 60), marks=[pytest.mark.dependency(name='message_perf_10000_1', depends=['message_count_10000_1', 'message_perf_3000_1'])]) ,
    "message=1000, connections=5": pytest.param(*(1000, 10, 5, 600, 0.05, 120), marks=[pytest.mark.dependency(name='message_perf_1000_5', depends=['message_count_3000_5', 'message_perf_3000_1'])]),
    "message=10000, connections=5": pytest.param(*(10000, 10, 5, 600, 0.05, 120), marks=[pytest.mark.dependency(name='message_perf_10000_5', depends=['message_count_10000_5', 'message_perf_3000_5', 'message_perf_10000_1'])]),
}
@pytest.mark.parametrize("count, cost, connections, runtime", list(test_process_message_count_group.values()), ids=list(test_process_message_count_group.keys()))
@pytest.mark.asyncio
async def test_process_message_count(count, cost, connections, runtime, websocket_server, async_runner):
    spider = websocket_spider()
    spider.websocket_url = websocket_server.get('websocket_server_url')
    spider.expected_counter = count * connections
    spider.connections = connections
    async_runner.add_spider(spider)
    await timeout(async_runner.start(), wait=runtime)
    assert spider.counter == spider.expected_counter

@pytest.mark.parametrize("count, cost, connections, runtime, expected_mean_cost_time, expected_total_costed_time", list(test_process_message_perf_group.values()), ids=list(test_process_message_perf_group.keys()))
@pytest.mark.asyncio
async def test_process_message_perf(count, cost, connections, runtime, expected_mean_cost_time, expected_total_costed_time, websocket_server, perf_metrics_collector, async_runner):
    spider = websocket_spider()
    spider.websocket_url = websocket_server.get('websocket_server_url')
    spider.connections = connections
    spider.expected_counter = count * connections
    spider.stats_counter = BoundedCountdownLatch(spider.expected_counter)
    async_runner.add_spider(spider)
    await websocket_stats_sample(spider)
    await timeout(async_runner.start(), wait=runtime)
    perf_metrics_collector.collect('response_process_metric', spider.stats.get_value('response_track', {}))
    stats = perf_metrics_collector.get('response_process_metric')
    stats['cost'] = stats['parsed'] - stats['received']
    assert stats['cost'].mean() < expected_mean_cost_time
    assert stats['cost'].sum() <  expected_total_costed_time
    stats['response_no']=stats.index