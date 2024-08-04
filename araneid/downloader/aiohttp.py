import logging
import time
from socket import AF_INET
from asyncio import TimeoutError, ensure_future
from urllib.parse import urlparse
from araneid.core import signal
from araneid.core.downloader import Downloader
from araneid.core.exception import DownloaderWarn, RequestException
from araneid.network.http import HttpRequest, HttpResponse
from araneid.core.stream import Stream
from araneid.core.exception import HttpRequestProxyError

logger = None
class HttpConnectionClose(DownloaderWarn, RequestException):
    def __init__(self, request, exception=None) -> None:
        self.__request__ = request
        self.__exception__ = exception
    
    def __str__(self):
        return '{request} connection has been closed (exception: {exception})'.format(request=self.__request__, exception=self.__exception__)

class Tracer:

    @classmethod
    async def on_request_start(cls, session, trace_config_ctx, params):
        http_req = f'HttpRequest<url={params.url}, method={params.method}, headers={params.headers}>'
        trace_config_ctx.request = http_req
        trace_config_ctx.start = time.time()
        logger.debug(f"{http_req} start.")


    @classmethod
    async def on_connection_create_start(cls, session, trace_config_ctx, params):
        trace_config_ctx.connection_create_start = time.time()
        logger.debug(f"{trace_config_ctx.request} connection create.")

    @classmethod
    async def on_connection_create_end(cls, session, trace_config_ctx, params):
        current = time.time()
        trace_config_ctx.connection_create_end = current
        connection_create_elapsed = current - trace_config_ctx.connection_create_start
        logger.debug(f"{trace_config_ctx.request} connection created. (connection create cost: {connection_create_elapsed:.4} s).")
 
    @classmethod
    async def on_dns_resolvehost_start(cls, session, trace_config_ctx, params):
        trace_config_ctx.dns_resolvehost_start = time.time()
        logger.debug(f"{trace_config_ctx.request} dns resolve start. (host:<{params.host}>).")

    @classmethod
    async def on_dns_resolvehost_end(cls, session, trace_config_ctx, params):
        current = time.time()
        trace_config_ctx.dns_resolvehost_end = current
        dns_resolvehost_elapsed = current - trace_config_ctx.dns_resolvehost_start
        logger.debug(f"{trace_config_ctx.request} dns resolve end. (host:<{params.host}>, dns resolve cost: {dns_resolvehost_elapsed:.4} s).")

    @classmethod
    async def on_response_chunk_received(cls, session, trace_config_ctx, params):
        elapsed =  time.time() - trace_config_ctx.start 
        logger.debug(f"{trace_config_ctx.request} response received. (cost: {elapsed:.4} s).")

    @classmethod
    async def on_request_exception(cls, session, trace_config_ctx, params):
        http_req = trace_config_ctx.request
        elapsed = time.time() - trace_config_ctx.start
        logger.debug(f"{http_req}  end by exception (cost: {elapsed:.4} s).", exc_info=params.exception)


    @classmethod
    async def on_request_end(cls, session, trace_config_ctx, params):
        http_req = trace_config_ctx.request
        elapsed = time.time() - trace_config_ctx.start
        logger.debug(f"{http_req} end (cost: {elapsed:.4} s).")

    """
    @classmethod
    async def on_request_headers_sent(cls, session, trace_config_ctx, params):
        logger.debug("on_request_headers_sent")
    """

    @classmethod
    def register(cls, trace_config):
        for signal in dir(cls):
            if not signal.startswith('on_'):
               continue
            if not hasattr(trace_config, signal):
               logger.warning(f'{signal} is not supported, skipped!')
               continue
            signal_handle = getattr(cls, signal)
            signal_handles = getattr(trace_config, signal)
            signal_handles.append(signal_handle)
        return trace_config

class Http(Downloader):

    def __init__(self):
        global logger
        super().__init__()
        logger = logging.getLogger(__name__)
        self.session = None
    
    def __init_session__(self):
        import aiohttp
        from aiohttp import client_exceptions
        from aiohttp_proxy import ProxyConnector 
        self.ProxyConnector = ProxyConnector
        self.TCPConnector = aiohttp.TCPConnector
        self.ClientSession = aiohttp.ClientSession
        self.client_exceptions = client_exceptions
        self.dns_resolver = aiohttp.AsyncResolver()
        tracer_config = aiohttp.TraceConfig()
        # fix aiohttp use ipv6 automatically if target host support it, but host not support ipv6 route 
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver= self.dns_resolver, family=AF_INET), trace_configs=[Tracer.register(tracer_config)],trust_env=True)
    
    @classmethod
    async def create(cls, settings=None):
        instance = cls()
        return instance

    
    def __simple_cookie_to_dict(self, simple_cookie):
        cookies_dict = {}
        for key, morsel in simple_cookie.items():
            cookies_dict[key] = morsel.value
        return cookies_dict

    async def download(self, request: HttpRequest):
        assert (isinstance(request, HttpRequest))
        async def close(channel: Stream):
            await channel.join()
            await channel.close()
        channel = await Stream.create()
        if not self.session:
           self.__init_session__()
        proxy = None if not request.proxy else request.proxy.get('http', None)
        timeout = request.timeout
        if not proxy:
           connector = None
           session = self.session
        else:
           # fix aiohttp use ipv6 automatically if target host support it, but host not support ipv6 route 
           connector = self.ProxyConnector.from_url(proxy, verify_ssl=False, resolver=self.dns_resolver, family=AF_INET)
           session = self.ClientSession(connector=connector, trust_env=True)
        try:
            req_start = time.time()
            logger.debug(f'{request}  -> {{ URL: {request.uri}, Method: {request.method}, Timeout: {request.timeout}, Headers: {request.headers}, Cookies: {request.cookies}, Data: {request.data}, Json: {request.json} }}')
            async with session.request(url=request.uri, method=request.method, data=request.data, json=request.json, headers=request.headers, timeout=timeout, cookies=request.cookies, ssl=False) as response:
                resp_content = await response.read()
                await signal.trigger(signal.bytes_received, source=self, object={ 'request':request, 'bytes': resp_content}, wait=False)
                cookies = self.__simple_cookie_to_dict(response.cookies)
                resp = HttpResponse.from_request(request=request, content=resp_content, status=response.status, headers=response.headers, history=response.history, encoding=response.get_encoding(), reason=response.reason, cookies=cookies)
                await channel.write(resp)
        except TimeoutError as e:
            req_elapsed = time.time() - req_start
            raise HttpConnectionClose(request, exception=f'Timeout {req_elapsed}s > {timeout}s (limited)') from e
        except Exception as e:
            logger.exception(e)
            raise HttpConnectionClose(request, exception=e) from e
        finally:
            if connector:
               await session.close()
        ensure_future(close(channel))
        return channel

    async def close(self):
        if not self.session:
            return
        await self.session.close()
        self.session = None

