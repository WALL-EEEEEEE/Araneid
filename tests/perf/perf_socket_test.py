import logging
import asyncio
import pytest
import time
from random import randint
from asyncio import Event
from unittest.mock import patch
from araneid.core.signal import register, response_received, response_parsed
from araneid.core.stream import Stream
from araneid.downloader.aiosocket import Socket
from araneid.network.socket import SocketRequest, SocketResponse
from araneid.util._async import BoundedCountdownLatch


def aiosocket_download(count, cost=10):
    async def start(request, channel):
        slice_size = int(count/cost)
        counter = 0
        for i in range(cost):
            for j in range(counter+1, counter+slice_size+1):
                resp = SocketResponse.from_request(request=request, content=bytes(str(counter+j), 'utf-8'))
                resp.set_completed(False)
                request.set_completed(True)
                await channel.write(resp)
            counter += slice_size
            await asyncio.sleep(1)
        for i in range(counter+1, count+1):
            resp = SocketResponse.from_request(request=request, content=bytes(str(j), 'utf-8'))
            resp.set_completed(False)
            request.set_completed(True)
            await channel.write(resp)
 

        await channel.join()
        request.set_completed(False)
        await channel.close()

    async def download(self, request):
        channel = Stream()
        asyncio.ensure_future(start(request, channel))
        return channel

    return download
    

async def stats_sample(spider):
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

def process_ping(wait_close):
        async def __(ws):
            if not wait_close.is_set():
               return
            await ws.close()
        return __

def process_message_count_group():

    def message_1000_connection_1():

        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            messages = response.text.split('\n')[:-1]
            for message in messages:
                con_counter+=1
                self.counter+=1
                self.logger.debug(f'{connection_id} - {con_counter}')

            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter

        message =  1000
        connections = 1
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params
    def message_10000_connection_1():
        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            messages = response.text.split('\n')[:-1]
            for message in messages:
                con_counter+=1
                self.counter+=1
                self.logger.debug(f'{connection_id} - {con_counter}')

            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter

        message =  10000
        connections = 1
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 60,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params
    def message_1000_connection_5():
        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            messages = response.text.split('\n')[:-1]
            for message in messages:
                con_counter+=1
                self.counter+=1
                self.logger.debug(f'{connection_id} - {con_counter}')

            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter

        message =  1000
        connections = 5
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params
    def message_10000_connection_5():
        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            messages = response.text.split('\n')[:-1]
            for message in messages:
                con_counter+=1
                self.counter+=1
                self.logger.debug(f'{connection_id} - {con_counter}')

            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter

        message =  10000
        connections = 5
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params

    spiders = list(locals().items())
    test_params = {}
    for name, spider in spiders: 
        test_alia = name
        spider_param = spider()
        param = spider_param.get('param', {})
        test_param = pytest.param(
            param,
            marks=[
                 pytest.mark.spider(
                     name= name,
                     start_requests=spider_param.get('start_requests', None),
                     parse=spider_param.get('parse', None)),
                 pytest.mark.script(
                     script='scripts/src/socket_server.go', params={'--port': param.get('port'), '--count': param.get('message'), '--cost': param.get('cost') },
                 ),
            ]) 
        test_params[test_alia] = test_param
    return test_params

def process_message_perf_group():

    def message_1000_connection_1():

        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            con_counter+=1
            self.counter+=1
            self.logger.debug(f'{connection_id} - {con_counter}')
            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter

        message =  1000
        connections = 1
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 15,
            'mean_time': 0.05,
            'total_time': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params
    def message_10000_connection_1():
        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            con_counter+=1
            self.counter+=1
            self.logger.debug(f'{connection_id} - {con_counter}')
            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter
        message =  10000
        connections = 1
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 60,
            'mean_time': 0.05,
            'total_time': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params
    def message_1000_connection_5():
        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            con_counter+=1
            self.counter+=1
            self.logger.debug(f'{connection_id} - {con_counter}')
            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter
 
        message =  1000
        connections = 5
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 120,
            'mean_time': 0.05,
            'total_time': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params
    def message_10000_connection_5():
        async def start_requests(self):
            self.counter = 0
            for i in range(connections):
                req = SocketRequest(url, on_ping=process_ping(wait_close), callback=parse, ping_interval=1, attach={'connection_id': i+1, 'counter': 0})
                yield req
            stats_counter = getattr(self, 'stats_counter', None)
            if stats_counter:
               await stats_counter.wait()

        def parse(self, response):
            connection_id = response.attach.get('connection_id')
            con_counter = response.attach.get('counter')
            con_counter+=1
            self.counter+=1
            self.logger.debug(f'{connection_id} - {con_counter}')
            if self.counter == param.get('counter'):
                wait_close.set()
            response.attach['counter'] = con_counter
 
        message =  10000
        connections = 5
        port = randint(5000, 9999)
        url = f'127.0.0.1:{port}'  
        param = {
            'cost': 10,
            'runtime': 120,
            'mean_time': 0.05,
            'total_time': 15,
            'message': message,
            'connections': connections,
            'counter': message*connections,
            'port': port,
            'url': url,
        }
        wait_close = Event()
        spider_params = { key : value for key, value in locals().items()}
        return spider_params

    spiders = list(locals().items())
    test_params = {}
    for name, spider in spiders: 
        test_alia = name
        spider_param = spider()
        param = spider_param.get('param', {})
        test_param = pytest.param(
            param,
            marks=[
                 pytest.mark.spider(
                     name= name,
                     start_requests=spider_param.get('start_requests', None),
                     parse=spider_param.get('parse', None)),
                 pytest.mark.mocker(
                     object=Socket, attribute='download', new=aiosocket_download(param.get('message'))
                 )
            ]) 
        test_params[test_alia] = test_param
    return test_params

@pytest.mark.parametrize("param", list(process_message_count_group().values()), ids=list(process_message_count_group().keys()))
@pytest.mark.asyncio
async def test_process_message_count(param,  spider, async_runner, script_runner):
    runtime = param.get('runtime', 10)
    counter = param.get('counter')
    spider_inst = spider()
    async_runner.add_spider(spider_inst)
    await timeout(async_runner.start(), wait=runtime)
    assert spider_inst.counter == counter

@pytest.mark.parametrize("param", list(process_message_perf_group().values()), ids=list(process_message_perf_group().keys()))
@pytest.mark.asyncio
async def test_process_message_perf(param,  spider, async_runner, perf_metrics_collector, mocker):
    runtime = param.get('runtime', 10)
    counter = param.get('counter')
    mean_time = param.get('mean_time')
    total_time = param.get('total_time')
    spider_inst = spider()
    async_runner.add_spider(spider_inst)
    spider.stats_counter = BoundedCountdownLatch(counter)
    await stats_sample(spider_inst)
    await timeout(async_runner.start(), wait=runtime)
    perf_metrics_collector.collect('response_process_metric', spider_inst.stats.get_value('response_track', {}))
    stats = perf_metrics_collector.get('response_process_metric')
    stats['cost'] = stats['parsed'] - stats['received']
    assert stats['cost'].mean() < mean_time
    assert stats['cost'].sum() <  total_time
    stats['response_no']=stats.index