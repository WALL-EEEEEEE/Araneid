import asyncio
import logging
from asyncio import Event
from araneid.core.exception import DownloaderWarn, RequestException
from araneid.core.downloader import Downloader
from araneid.network.socket import SocketRequest, SocketResponse
from araneid.core.stream import Stream
from collections.abc import Callable
from araneid.util._async import ensure_asyncfunction

class SocketConnectionClose(DownloaderWarn, RequestException):
    def __init__(self, request, close_code=None, exception=None) -> None:
        self.__request__ = request
        self.__close_code__ = close_code
        self.__exception__ = exception
    
    def __str__(self):
        return '{request} connection has been closed (code: {close_code}, exception: {exception})'.format(request=self.__request__, close_code=self.__close_code__, exception=self.__exception__)


class SocketConnection:
    __conn = None
    __closed = None
    __channel = None
    __exception = None
    request = None
    buffer_limit = 4096

    @property
    def closed(self):
        return self.__closed

    def __init__(self, request, socket_reader, socket_writer, buffer_limit=4096):
        self.__reader__ =  socket_reader
        self.__writer__ = socket_writer
        self.__closed = False
        self.request: SocketRequest = request
        self.buffer_limit = buffer_limit
    
    @classmethod
    async def create(cls, request, socket_reader, socket_writer, buffer_limit=4096):
        instance = cls(request, socket_reader, socket_writer, buffer_limit=buffer_limit)
        instance.__channel: Stream = Stream(f'aiosocket_request_{id(request)}')
        return instance
    
    async def async_recv(self, limit=4096):
        return await self.__reader__.read(limit)
    
    async def async_read(self, limit=4096):
        return await self.__reader__.read(limit)
    
    @asyncio.coroutine
    def read(self, limit=4096):
        task = asyncio.create_task(self.async_read(limit))
        while not task.done():
            yield from asyncio.sleep(1)
        return task.result()
    
    @asyncio.coroutine
    def recv(self, limit=4096):
        task = asyncio.create_task(self.async_recv(limit))
        while not task.done():
            yield from asyncio.sleep(1)
        return task.result()
    
    
    async def async_write(self, message):
        self.__writer__.write(message)

    async def async_send(self, message):
        self.__writer__.write(message)
    
    def write(self, message):
        asyncio.create_task(self.async_write(message))
    
    def send(self, message):
        asyncio.create_task(self.async_send(message))
   
    async def set_exception(self, exception):
        await self.__channel.set_exception(exception)

    def default_ping_back(self, ping_message):
        assert isinstance(ping_message, (list, str,bytes, bytearray))
        async def __(socket: SocketConnection):
            if not isinstance(ping_message, list):
               await socket.async_write(ping_message)
               return
            for message in ping_message:
                await socket.async_write(message)
        return __
    
    def default_open_back(self, open_message):
        assert isinstance(open_message, (list, str,bytes, bytearray))
        async def __(socket: SocketConnection):
           if not isinstance(open_message, list):
               await socket.async_write(open_message)
               return
           for message in open_message:
                await socket.async_write(message)
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
           await self.set_exception(SocketConnectionClose(self.request))
           return
        try:
            await open_back(self)
        except Exception as e:
            self.request.set_completed(False)
            await self.set_exception(SocketConnectionClose(self.request, exception=e))
            return

    async def __process_ping(self, heartbeat):
        ping_back = self.request.on_ping
        if not isinstance(ping_back, Callable):
            ping_back = self.default_ping_back(ping_back)
        if asyncio.iscoroutinefunction(ping_back):
            ping_back = asyncio.coroutine(ping_back)
        async_ping_back = ensure_asyncfunction(ping_back)
        if self.closed:
           self.request.set_completed(False)
           await self.set_exception(SocketConnectionClose(self.request))
           await self.__channel.close()
           return
        try:
           await async_ping_back(self)
        except Exception as e:
            self.request.set_completed(False)
            await self.set_exception(SocketConnectionClose(self.request, exception=e))
            return
        asyncio.get_running_loop().call_later(heartbeat, lambda: asyncio.create_task(self.__process_ping(heartbeat)))

    async def process_ping(self):
        loop = asyncio.get_running_loop()
        heartbeat = self.request.ping_interval
        ping_back = self.request.on_ping
        if not ping_back:
           return
        loop.call_later(heartbeat,lambda: asyncio.create_task(self.__process_ping(heartbeat)))
    
    async def process_response(self):
        while not self.closed:
            try:
                resp = await self.async_read(self.buffer_limit)
            except Exception as e:
                self.request.set_completed(False)
                await self.set_exception(SocketConnectionClose(self.request, exception=e))
                break
            if not resp:
               continue
            resp = SocketResponse.from_request(request=self.request, content=resp)
            await self.__channel.write(resp)
            self.request.set_completed(True)
            resp.set_completed(False)
        self.request.set_completed(False)
        await self.__channel.close()
    
    async def start(self):
        try:
            await asyncio.gather(self.process_open(), self.process_ping(), self.process_response())
        except ConnectionError as e:
            self.request.set_completed(False)
            await self.set_exception(SocketConnectionClose(request=self.request,exception=e))
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
        self.__writer__.close()
        await self.__writer__.wait_closed()


class Socket(Downloader):
    logger = None

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.buffer_limit = 4096
    
    @classmethod
    async def create(cls, settings=None):
        instance = cls()
        return instance

        
    async def download(self, request: SocketRequest):
        assert (isinstance(request, SocketRequest))
        socket_host, socket_port = request.url.split(':')
        socket = None
        try:
            socket_reader, socket_writer = await asyncio.open_connection(host=socket_host, port=socket_port)
            socket = await SocketConnection.create(request, socket_reader=socket_reader, socket_writer=socket_writer)
            asyncio.ensure_future(socket.start())
        except ConnectionError as e:
            request.set_completed(False)
            raise SocketConnectionClose(request=request, exception=e)
        except Exception as e:
            raise SocketConnectionClose(request=request, exception=e) from e
        return socket.stream()
        