import asyncio
from unittest.mock import patch
from attr import Attribute, attributes
import pytest
import logging
import aiohttp 
from asyncmock import AsyncMock
from araneid.core import signal
from araneid.network.websocket import WebSocketRequest
from araneid.downloader.aiosocket import SocketConnection
from araneid.downloader.aiowebsocket import WebSocketConnection
from araneid.network.socket import SocketRequest
from .spiders.http_spider import http_spider
from .spiders.item_spider import old_style_item_spider, new_style_item_spider



async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

test_old_spider_group = {
    'old_spider_cli_variable_alias': ('old_spider_cli_variable_alias', 'test'),
    'old_spider_cli_variable': ('old_spider_cli_variable', 'test'),
}
test_spider_group = {
    'old_style_item_spider': (old_style_item_spider, None),
    'new_style_item_spider': (new_style_item_spider, {'url': new_style_item_spider.url, 'content': new_style_item_spider.url})
}
test_spider_close_params = [
  {"spider": http_spider, 'param': {'url': 'https://mock.spider.url'}},
  {"spider": http_spider, 'param': {'ident': '_connection_error', 'url': 'https://mock.spider.url', 'request_exception': ConnectionError('Connection Error')}},
  {"spider": http_spider, 'param': {'ident': '_parser_error', 'url': 'https://mock.spider.url', 'parser_exception': ValueError('value error')}},
  {"spider": http_spider, 'param': {'ident': '_starter_error', 'url': 'https://mock.spider.url', 'starter_exception': ValueError('value error')}},
]
test_spider_close_group = {
    param["spider"].__name__+ param.get('param', {}).get('ident', '') : ( param["spider"], param["param"]) for param in test_spider_close_params
}
test_spider_stats_group = {}

def request_group():

    def socket_request_str_list():

        def mockers():
            def write(message):
                result.append(message)

            async def open_connection(**kwargs):
                writer = AsyncMock()
                writer.write = write
                return None, writer

            async def process_response(self):
                await asyncio.sleep(2)
                return

            return [
                pytest.mark.mocker(object=SocketConnection, attribute='process_response', new=process_response),
                pytest.mark.mocker(object=asyncio, attribute='open_connection', new=open_connection)
                ]

        async def start_requests(self):
            req = SocketRequest(url, on_ping=on_ping, on_open=on_open, ping_interval=1)
            yield req
        
        def check():
            assert set(result)  == set([*on_ping, *on_open])

        result = []
        url = '127.0.0.1:8080'
        on_ping = ['ping']
        on_open = ['hello', 'hi']
        param = {
            'runtime': 4,
        }
        spider_params = { key : value for key, value in locals().items()}
        return spider_params

    def socket_request_bytes_list():

        def mockers():
            def write(message):
                result.append(message)

            async def open_connection(**kwargs):
                writer = AsyncMock()
                writer.write = write
                return None, writer

            async def process_response(self):
                await asyncio.sleep(2)
                return

            return [
                pytest.mark.mocker(object=SocketConnection, attribute='process_response', new=process_response),
                pytest.mark.mocker(object=asyncio, attribute='open_connection', new=open_connection)
                ]

        async def start_requests(self):
            req = SocketRequest(url, on_ping=on_ping, on_open=on_open, ping_interval=1)
            yield req
        
        def check():
            assert set(result)  == set([*on_ping, *on_open])

        result = []
        url = '127.0.0.1:8080'
        on_ping = [b'ping']
        on_open = [b'hello', b'hi']
        param = {
            'runtime': 4,
        }
        spider_params = { key : value for key, value in locals().items()}
        return spider_params

    def socket_request_callback():

        async def process_ping(ws):
            for msg in on_ping:
                await ws.async_write(msg)

        async def process_open(ws):
            for msg in on_open:
                await ws.async_write(msg)

        def mockers():
            def write(message):
                result.append(message)

            async def open_connection(**kwargs):
                writer = AsyncMock()
                writer.write = write
                return None, writer

            async def process_response(self):
                await asyncio.sleep(2)
                return

            return [
                pytest.mark.mocker(object=SocketConnection, attribute='process_response', new=process_response),
                pytest.mark.mocker(object=asyncio, attribute='open_connection', new=open_connection)
                ]

        async def start_requests(self):
            req = SocketRequest(url, on_ping=on_ping, on_open=on_open, ping_interval=1)
            yield req

        def check():
            assert set(result)  == set([*on_ping, *on_open])


        result = []
        url = '127.0.0.1:8080'
        on_ping = ['ping']
        on_open = ['hello', 'hi']
        param = {
            'runtime': 4,
        }
        url = '127.0.0.1:8080'
        spider_params = { key : value for key, value in locals().items()}
        return spider_params

    def websocket_request_str_list():

        async def process_ping(ws):
            for msg in on_ping:
                await ws.async_write(msg)

        async def process_open(ws):
            for msg in on_open:
                await ws.async_write(msg)

        def mockers():
            async def write(message):
                result.append(message)

            async def ws_connect(self, **kwargs):
                conn = AsyncMock()
                conn.send_bytes = write
                conn.send_str = write
                conn.ping = write
                return  conn

            async def process_response(self):
                await asyncio.sleep(2)
                return

            return [
                pytest.mark.mocker(object=WebSocketConnection, attribute='process_response', new=process_response),
                pytest.mark.mocker(object=aiohttp.ClientSession, attribute='ws_connect', new=ws_connect)
                ]

        async def start_requests(self):
            req = WebSocketRequest(url, on_ping=on_ping, on_open=on_open, ping_interval=0.1)
            yield req

        def check():
            assert set(result)  == set([*on_ping, *on_open])


        result = []
        url = 'ws://127.0.0.1:8080'
        on_ping = ['ping']
        on_open = ['hello', 'hi']
        param = {
            'runtime': 4,
        }
        url = '127.0.0.1:8080'
        spider_params = { key : value for key, value in locals().items()}
        return spider_params

    def websocket_request_bytes_list():

        async def process_ping(ws):
            for msg in on_ping:
                await ws.async_write(msg)

        async def process_open(ws):
            for msg in on_open:
                await ws.async_write(msg)

        def mockers():
            async def write(message):
                result.append(message)

            async def ws_connect(self, **kwargs):
                conn = AsyncMock()
                conn.send_bytes = write
                conn.send_str = write
                conn.ping = write
                return  conn

            async def process_response(self):
                await asyncio.sleep(2)
                return

            return [
                pytest.mark.mocker(object=WebSocketConnection, attribute='process_response', new=process_response),
                pytest.mark.mocker(object=aiohttp.ClientSession, attribute='ws_connect', new=ws_connect)
                ]

 
        async def start_requests(self):
            req = WebSocketRequest(url, on_ping=on_ping, on_open=on_open, ping_interval=0.1)
            yield req

        def check():
            assert set(result)  == set([*on_ping, *on_open])


        result = []
        url = 'ws://127.0.0.1:8080'
        on_ping = [b'ping']
        on_open = [b'hello', b'hi']
        param = {
            'runtime': 4,
        }
        url = '127.0.0.1:8080'
        spider_params = { key : value for key, value in locals().items()}
        return spider_params



    def websocket_request_callback():

        async def process_ping(ws):
            for msg in on_ping:
                await ws.async_send(msg)

        async def process_open(ws):
            for msg in on_open:
                await ws.async_send(msg)

        def mockers():
            async def write(message):
                result.append(message)

            async def ws_connect(self, **kwargs):
                conn = AsyncMock()
                conn.send_bytes = write
                conn.send_str = write
                conn.ping = write
                return  conn

            async def process_response(self):
                await asyncio.sleep(2)
                return

            return [
                pytest.mark.mocker(object=WebSocketConnection, attribute='process_response', new=process_response),
                pytest.mark.mocker(object=aiohttp.ClientSession, attribute='ws_connect', new=ws_connect)
                ]


        async def start_requests(self):
            req = WebSocketRequest(url, on_ping=on_ping, on_open=on_open, ping_interval=0.1)
            yield req

        def check():
            assert set(result)  == set([*on_ping, *on_open])


        result = []
        url = '127.0.0.1:8080'
        on_ping = ['ping']
        on_open = ['hello', 'hi']
        param = {
            'runtime': 4,
        }
        url = '127.0.0.1:8080'
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
            spider_param.get('check', lambda _: None),
            marks=[
                 pytest.mark.spider(
                     name= name,
                     start_requests=spider_param.get('start_requests', None),
                     parse=spider_param.get('parse', None)),
                 *spider_param.get('mockers', lambda: [])(),
            ]) 
        test_params[test_alia] = test_param
    return test_params


@pytest.mark.spider
@pytest.mark.parametrize('spider, url', list(test_old_spider_group.values()), ids=list(test_old_spider_group.keys()))
def test_old_spider(spider, url, spider_starter):
    output = spider_starter.get('output', '')
    assert url in output
    assert 'Exception' not in output

@pytest.mark.spider
@pytest.mark.parametrize('spider, expected_item', list(test_spider_group.values()), ids=list(test_spider_group.keys()))
@pytest.mark.asyncio
async def test_spider(spider, expected_item, caplog, aioresponse, async_runner):
    expected_logs = [
        f'{spider.__name__}'
    ]
    caplog.set_level(logging.INFO)
    aioresponse.get(spider.url, status=200, payload=spider.url, repeat=True)
    spider_inst = spider()
    async_runner.add_spider(spider_inst)
    await timeout(async_runner.start())
    for expect_log in expected_logs:
        assert expect_log in caplog.messages
    if expected_item is not None:
       parser = spider_inst.get_parser(spider_inst.parser_name)
       assert parser.items['url'].value ==  expected_item.get('url')
       assert parser.items['content'].value == expected_item.get('content')

@pytest.mark.spider
@pytest.mark.parametrize('spider, param', list(test_spider_close_group.values()), ids=list(test_spider_close_group.keys()))
@pytest.mark.asyncio
async def test_spider_close(spider, param, aioresponse, async_runner):
    url = param.get('url')
    request_exception = param.get('request_exception', None)
    spider_inst = spider()
    spider_inst.url = url
    if request_exception:
       aioresponse.get(url, exception=request_exception)
    else:
       aioresponse.get(url, status=200, payload={'status': 'ok', 'content':spider_inst.url}, repeat=True)
    async_runner.add_spider(spider_inst)
    await timeout(async_runner.start())


@pytest.mark.parametrize("param, check", list(request_group().values()), ids=list(request_group().keys()))
@pytest.mark.asyncio
async def test_request(param, check,  spider, async_runner, mocker):
    runtime = param.get('runtime', 10)
    spider_inst = spider()
    async_runner.add_spider(spider_inst)
    await timeout(async_runner.start(), wait=runtime)
    check()