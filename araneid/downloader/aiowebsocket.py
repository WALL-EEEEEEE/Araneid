
import logging
import asyncio
from asyncio.locks import Event
from araneid.core.downloader import Downloader
from araneid.core.exception import DownloaderWarn, RequestException
from araneid.core.stream import Stream
from araneid.network.websocket import WebSocketRequest, WebSocketResponse
from collections.abc import Callable
from araneid.util._async import ensure_asyncfunction

WSMsgType = None
WSMessage = None
ClientError = None

class WebSocketConnectionClose(DownloaderWarn, RequestException):
    def __init__(self, request, close_code=None, exception=None) -> None:
        self.__request__ = request
        self.__close_code__ = close_code
        self.__exception__ = exception
    
    def __str__(self):
        return '{request} connection has been closed (code: {close_code}, exception: {exception})'.format(request=self.__request__, close_code=self.__close_code__, exception=self.__exception__)

class WebSocketConnection:
    logger = None
    __conn = None
    __closed = None
    __channel = None
    request = None

    @property
    def closed(self):
        return self.__closed
    
    @property
    def close_code(self):
        return self.__conn.close_code

    def __init__(self, connection, request: WebSocketRequest):
        self.logger = logging.getLogger(__name__)
        self.__conn = connection 
        self.__closed = False
        self.request: WebSocketRequest = request
    
 
    
    @classmethod
    async def create(cls, connection, request: WebSocketRequest):
        instance = cls(connection, request)
        instance.__channel: Stream = Stream.create(f'aiowebsocket_request_{id(request)}')

    async def set_exception(self, exception):
        await self.__channel.set_exception(exception)

    
    async def async_ping(self, message):
        assert isinstance(message, (bytes, bytearray, str))
        await self.__conn.ping(message)
    
    async def ping(self, message):
        asyncio.create_task(self.async_ping(message))
    
    async def async_send(self, message):
        assert isinstance(message, (bytes, bytearray, str))
        if isinstance(message, (bytes, bytearray)):
            await self.__conn.send_bytes(message)
        else:
            await self.__conn.send_str(message)

    def send(self, message):
        asyncio.create_task(self.async_send(message))
    
    async def async_send_binary(self, message):
        assert isinstance(message, (bytes, bytearray))
        await self.__conn.send_bytes(message)

    def send_binary(self, message):
        asyncio.create_task(self.async_send_binary(message))
    
    async def async_recv(self):
        recv_msg = await self.__conn.receive()
        return recv_msg

    @asyncio.coroutine
    def recv(self):
        task = asyncio.create_task(self.async_recv())
        if not task.done():
            yield from asyncio.sleep(1)
        return task.result()

    def default_ping_back(self, ping_message):
        assert isinstance(ping_message, (list, str,bytes, bytearray))
        async def __(websocket: WebSocketConnection):
            if not isinstance(ping_message, list):
               await websocket.async_ping(ping_message)
               return
            for message in ping_message:
                await websocket.async_ping(message)
        return __
    
    def default_open_back(self, open_message):
        assert isinstance(open_message, (list, str,bytes, bytearray))
        async def __(websocket):
           if not isinstance(open_message, list):
               await websocket.async_send(open_message)
               return
           for message in open_message:
                await websocket.async_send(message)
        return __

    async def process_open(self):
        open_back = self.request.on_open
        if not open_back:
           return
        if not isinstance(open_back, Callable):
            open_back = self.default_open_back(open_back)
        else:
            open_back = ensure_asyncfunction(open_back)
        if self.closed:
           self.request.set_completed(False)
           await self.set_exception(WebSocketConnectionClose(self.request, self.close_code, self.__conn.exception()))
           return
        try:
            await open_back(self)
        except Exception as e:
            self.request.set_completed(False)
            await self.set_exception(WebSocketConnectionClose(self.request, exception=e))
            return

    
    async def __process_ping(self, heartbeat):
        ping_back = self.request.on_ping
        if not isinstance(ping_back, Callable):
            ping_back = self.default_ping_back(ping_back)
        else:
            ping_back = ensure_asyncfunction(ping_back)
        if self.closed:
            self.request.set_completed(False)
            await self.set_exception(WebSocketConnectionClose(self.request, self.close_code, self.__conn.exception()))
            return
        try:
            await ping_back(self)
        except Exception as e:
            self.request.set_completed(False)
            await self.set_exception(WebSocketConnectionClose(self.request, exception=e))
            return
        asyncio.get_running_loop().call_later(heartbeat, lambda: asyncio.create_task(self.__process_ping(heartbeat)))
               
    async def process_ping(self):
        loop = asyncio.get_running_loop()
        ping_back = self.request.on_ping
        if not ping_back:
           return
        heartbeat = self.request.ping_interval
        loop.call_later(heartbeat,lambda: asyncio.create_task(self.__process_ping(heartbeat)))
    
    async def process_response(self):
        while not self.closed:
            resp = await self.async_recv()
            if isinstance(resp, WSMessage):
                if resp.type == WSMsgType.TEXT or resp.type == WSMsgType.BINARY or resp.type == WSMsgType.CONTINUATION:
                    resp = WebSocketResponse.from_request(request=self.request, content=resp.data)
                    resp.set_completed(False)
                    self.request.set_completed(True)
                    await self.__channel.write(resp)
                elif resp.type in (WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    self.request.set_completed(False)
                    await self.set_exception(WebSocketConnectionClose(self.request, self.close_code, self.__conn.exception()))
                else:
                    self.logger.debug(resp)
        await self.set_exception(WebSocketConnectionClose(self.request, self.close_code, self.__conn.exception()))

    async def start(self):
        try:
            await asyncio.gather(self.process_open(), self.process_ping(), self.process_response())
        except ClientError as e:
            self.request.set_completed(False)
            await self.set_exception(WebSocketConnectionClose(request=self.request,exception=e))
        except Exception as e:
            self.request.set_completed(False)
            await self.set_exception(e)
        finally:
            await self.close()

    def stream(self):
        return self.__channel

    async def close(self):
        if self.closed:
           return
        self.__closed = True
        self.request.set_completed(False)
        await self.__channel.close()
        await self.__conn.close()

class WebSocket(Downloader):
    logger = None

    def __init__(self):
        self.session = None
        self.logger = logging.getLogger(__name__)
    
    @classmethod
    async def create(cls, settings=None):
        instance = cls()
        return instance

    def __init_session__(self):
        global WSMessage, WSMsgType, ClientError
        import aiohttp
        self.session = aiohttp.ClientSession()
        WSMsgType = aiohttp.WSMsgType
        WSMessage = aiohttp.WSMessage
        ClientError = aiohttp.ClientError

 
    async def download(self, request: WebSocketRequest):
        assert (isinstance(request, WebSocketRequest))
        if not self.session:
           self.__init_session__()
        proxy = None if not request.proxy else request.proxy.get('http', None)
        timeout = getattr(request, 'timeout', None)
        pingback = request.on_ping
        if proxy:
            proxy = proxy.replace('https', 'http')
        self.logger.debug("url: {url}".format(url=request.url))
        self.logger.debug("headers: {url}".format(url=request.headers))
        self.logger.debug('method: {method}'.format(method=request.method))
        self.logger.debug('proxy: {proxy}'.format(proxy=proxy))
        self.logger.debug('timeout:{timeout}'.format(timeout=timeout))
        self.logger.debug('pingback:{pingback}'.format(pingback=pingback))
        websocket = None
        try:
            if not pingback:
                conn = await self.session.ws_connect(url=request.url, method=request.method, headers=request.headers, proxy=proxy, timeout=timeout, verify_ssl=False)
                websocket = WebSocketConnection(connection=conn, request=request)
            else:
                conn = await self.session.ws_connect(url=request.url, method=request.method, headers=request.headers, proxy=proxy, timeout=timeout, autoping=False, verify_ssl=False)
                websocket = WebSocketConnection(connection=conn, request=request)
            asyncio.ensure_future(websocket.start())
        except Exception as e:
            raise WebSocketConnectionClose(request=request,exception=e) from e
        return websocket.stream()
            
    async def close(self):
        if not self.session:
            return
        await self.session.close()
        self.session = None