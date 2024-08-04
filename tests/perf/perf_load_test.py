from contextlib import suppress
import logging
import pytest
import asyncio
import re
import itertools
from araneid.core.signal import register, spider_closed
from .spiders.perf_load_websocket_spider import websocket_spider
from .spiders.perf_load_http_spider import http_spider

logger = logging.getLogger()



test_perf_load_params = [
  {"spider": http_spider, 'param': {'ident': ' [type=memory, request=1000] ', 'spider_perf_type': 'http', 'count': 1000, 'mem_limit': 40, 'cpu_limit': None, 'runtime':20}},
  {"spider": http_spider, 'param': {'ident': ' [type=memory, request=10000] ', 'spider_perf_type': 'http', 'count': 10000, 'mem_limit': 40, 'cpu_limit': None, 'runtime':180}},
  {"spider": http_spider, 'param': {'ident': ' [type=cpu] ', 'spider_perf_type': 'http', 'count': 10, 'mem_limit': 40, 'cpu_limit': 1, 'runtime': 20, 'interval': 1}},
  {"spider": websocket_spider, 'param': {'ident': ' [type=memory, message=1000] ', 'spider_perf_type': 'websocket', 'count': 1000, 'connections': 1, 'mem_limit': 42, 'cpu_limit': None, 'cost':10}},
  {"spider": websocket_spider, 'param': {'ident': ' [type=memory, message=10000] ', 'spider_perf_type': 'websocket', 'count': 10000, 'connections':1, 'mem_limit': 45, 'cpu_limit': None, 'cost':10}},
  {"spider": websocket_spider, 'param': {'ident': ' [type=cpu] ', 'spider_perf_type': 'websocket', 'count': 10, 'connections': 1, 'mem_limit': 40, 'cpu_limit': 1, 'runtime': 20, 'interval': 1, 'cost': 10}},
  {"spider": websocket_spider, 'param': {'ident': ' [type=memory, message=1000, conn=5] ', 'spider_perf_type': 'websocket', 'count': 1000, 'connections': 5, 'mem_limit': 42, 'cpu_limit': None, 'cost':10}},
  {"spider": websocket_spider, 'param': {'ident': ' [type=memory, message=10000, conn=5] ', 'spider_perf_type': 'websocket', 'count': 10000, 'connections': 5, 'mem_limit': 150, 'cpu_limit': None, 'cost':10}},
  {"spider": websocket_spider, 'param': {'ident': ' [type=cpu, conn=5] ', 'spider_perf_type': 'websocket', 'count': 10, 'connections': 5, 'mem_limit': 40, 'cpu_limit': 1, 'cost': 10, 'interval': 1}},


]
test_perf_load_group = {
    param["spider"].__name__+ param.get('param', {}).get('ident', '') : ( param["spider"], param["param"]) for param in test_perf_load_params
}

async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

 
    
@pytest.mark.parametrize("spider, param", list(test_perf_load_group.values()), ids=list(test_perf_load_group.keys()))
@pytest.mark.asyncio
async def test_perf_load(spider, param, spider_perf_starter):
    mem_limit = param.get('mem_limit', None)
    cpu_limit = param.get('cpu_limit', None)
    if not (mem_limit  or cpu_limit):
       pytest.skip("no mem_limit or cpu_limit specified!") 
    if mem_limit:
        mem_stats = spider_perf_starter.memory() 
        inefficient_mem_stats =  list(map(lambda mem: str(mem)+' mb', itertools.filterfalse(lambda mem:  mem <= mem_limit, mem_stats)))
        message_info = "Memory Load: "+', '.join(inefficient_mem_stats)+" larger than {mem_limit} mb in {mem_stats} .".format(mem_limit=mem_limit, mem_stats=mem_stats)
        pytest.assume(len(inefficient_mem_stats) <= 0, message_info)
    if cpu_limit:
       cpu_stats = spider_perf_starter.cpu()
       inefficient_cpu_stats =  list(map(lambda cpu: str(cpu)+'%', itertools.filterfalse(lambda cpu_percent:  cpu_percent <= cpu_limit, cpu_stats)))
       message_info = "CPU Load: "+', '.join(inefficient_cpu_stats)+" larger than {cpu_limit}% in {cpu_stats}s.".format(cpu_limit=cpu_limit, cpu_stats=cpu_stats)
       pytest.assume(len(inefficient_cpu_stats) <= 3, message_info)