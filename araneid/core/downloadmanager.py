import logging
import asyncio
from contextlib import suppress
from typing import Coroutine, List, Optional
from araneid.core import Slotable
from araneid.core.exception import DownloaderNotFound, InvalidDownloader, RequestException
from araneid.core.downloader import Downloader
from araneid.core.middlewaremanager import DownloaderMiddlewareManager
from araneid.network.websocket import WebSocketRequest
from araneid.network.socket import SocketRequest
from araneid.core.response import Response
from araneid.core.request import Request
from araneid.core.pipeline import Pipeline
from araneid.core.stream import Stream
from araneid.spider import Spider
from araneid.core import signal
from araneid.core import plugin as plugins
from araneid.util._async import ensure_asyncfunction, ensure_asyncgenfunction
from araneid.util import cast_exception_to
from asyncio.locks import BoundedSemaphore, Event
from asyncio import Task

class DownloadManager(object):
    logger = None
    __running__: List[Task]
    __downloadermiddlewaremanager__: DownloaderMiddlewareManager
    __MAX_DOWNLOADER_PROCESSES__:int
    __IDLE_EVENT__: Event
    __AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__: Optional[BoundedSemaphore]
    __channel_receivers__: List[Coroutine]
    __channel__: Stream
    __download_channel__: Stream
    __closed__: bool

    @classmethod
    def from_settings(cls, settings):
        return cls(settings) 

    def __init__(self, settings=None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("DownloaderManager init.")
        self.__MAX_DOWNLOADER_PROCESSES__ = settings.get('MAX_DOWNLOADER_PROCESSES', -1)
        self.__closed__ = False
        self.__channel_receivers__ = []
    
    @classmethod
    async def create(cls, settings = None):
        instance = cls.from_settings(settings)
        instance.__IDLE_EVENT__ = Event()
        if instance.__MAX_DOWNLOADER_PROCESSES__ > 0:
            instance.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__ = BoundedSemaphore(instance.__MAX_DOWNLOADER_PROCESSES__)
        else:
            instance.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__ = None
        instance.__downloader__ = await instance.__load_plugin(settings)
        instance.__channel__ = await Stream.create()
        instance.__download_channel__ = await Stream.create()
        instance.__downloadermiddlewaremanager__ = await DownloaderMiddlewareManager.create(settings)
        return instance

    async def __load_plugin(self, settings):
        downloader_plugins = plugins.load(plugins.PluginType.DOWNLOADER)
        downloaders = dict()
        for plugin in downloader_plugins:
            name = plugin.name
            downloader = plugin.load()
            if not issubclass(downloader, Downloader):
               raise  InvalidDownloader('Downloader '+name+' is invalid.')
            if hasattr(downloader, 'from_settings'):
               downloaders[name] = await downloader.create(settings=settings)
            else:
               downloaders[name] = await downloader.create()

            self.logger.debug(f'Loaded downloader: {name}.')
        return downloaders 
   
    def __select_downloader(self, downloader=None):
        d = None
        if not downloader:
            d = self.__downloader__.get('Http', None)
        else:
            d = self.__downloader__.get(downloader, None)
        if d is not None:
           return d
        raise DownloaderNotFound('Downloader '+downloader+' not found!')
    
    def idle(self):
        return self.__channel__.idle()
    
    async def wait_idle(self)-> None:
        await self.__IDLE_EVENT__.wait()
        self.__IDLE_EVENT__.clear()

    async def acquire_pipeline_semaphor(self):
        if not self.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__:
            return
        await self.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__.acquire()
        self.logger.debug('Semaphor value: '+str(getattr(self.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__, '_value')))

    def release_pipeline_semaphor(self):
        if not self.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__:
            return
        self.__AVAILABLE_DOWNLOADER_PIPELINE_SEMAPHOR__.release()

    async def fork_download_pipeline(self, request :Request, downloader=None):
        def __(fut):
            nonlocal request
            self.release_pipeline_semaphor()
        await self.acquire_pipeline_semaphor()
        pipeline = Pipeline(self.download(request,downloader))
        pipeline_name = 'downloader_'+str(request.uri)
        pipeline.set_name(pipeline_name)
        pipeline.add_done_callback(__)
        return pipeline
    
    async def process_download(self, request, downloader=None):
        pipeline = await self.fork_download_pipeline(request, downloader)
        await self.__download_channel__.write(pipeline)
    
    async def process_close(self, request, spider, scraper):
        closeback = getattr(request, 'closeback', None)
        if not closeback:
           return None
        async_closeback = ensure_asyncfunction(closeback)
        result = await async_closeback(request, spider)
        return result
    
    async def complete_request(self, request, spider, scraper):
        scraper.complete_request(request)
        if self.idle():
           self.__IDLE_EVENT__.set()
    
    async def download(self, request, downloader=None):
        spider = None if not request.context else request.context.spider
        scraper = None if not request.context else request.context.scraper
        try:
            downloadermw_ret = await self.__downloadermiddlewaremanager__.process_request(request, spider)
            if isinstance(downloadermw_ret, Response):
                await self.__channel__.write(downloadermw_ret) 
            elif isinstance(downloadermw_ret, Request):
                await self.__download(downloadermw_ret, downloadermw_ret.downloader)
            else:
                await self.complete_request(request, spider, scraper)
        except Exception as e:
            self.logger.exception(e)
            await self.complete_request(request, spider, scraper)

    async def __download(self, request, downloader=None):
        downloader = self.__select_downloader(downloader)
        spider = None if not request.context else request.context.spider
        scraper = None if not request.context else request.context.scraper
        download_handler = downloader.download
        async_download_handler = ensure_asyncfunction(download_handler)
        try:
            await signal.trigger(signal.request_reached_downloader, source=downloader, object=request, wait=False)
            self.logger.debug("Downloading {request}".format(request=request))
            resp_stream: Stream = await async_download_handler(request)
            await signal.trigger(signal.request_left_downloader,source=downloader, object=request, wait=False)
            async for download_ret in resp_stream.read():
                if isinstance(download_ret, Request):
                    await self.__channel__.write(download_ret)
                    continue
                self.logger.debug("Downloaded {request}: {response}".format(request=request, response=download_ret))
                await signal.trigger(signal.response_downloaded, source=downloader, object=download_ret, wait=False)
                downloadermiddleware_ret = await self.__downloadermiddlewaremanager__.process_response(request, download_ret, spider)
                if isinstance(downloadermiddleware_ret, Request) or isinstance(downloadermiddleware_ret, Response):
                    await self.__channel__.write(downloadermiddleware_ret)
            request.set_state(request.States.download)
            try:
                close_ret = await self.process_close(request, spider, scraper)
                if isinstance(close_ret, Request) :
                    await self.__channel__.write(close_ret)
                    await self.complete_request(request, spider, scraper)
                elif isinstance(close_ret, Response):
                    await self.__channel__.write(close_ret)
                else:
                    if isinstance(request, (WebSocketRequest, SocketRequest)):
                       await self.complete_request(request, spider ,scraper)
            except Exception as e:
                downloadermiddleware_ret = await self.__downloadermiddlewaremanager__.process_exception(request, e, spider)
                if isinstance(downloadermiddleware_ret, Request) or isinstance(downloadermiddleware_ret, Response):
                    await self.__channel__.write(downloadermiddleware_ret)
                else:
                    await self.complete_request(request, spider, scraper)
        except Exception as e:
            downloadermiddleware_ret = await self.__downloadermiddlewaremanager__.process_exception(request, e, spider)
            if isinstance(downloadermiddleware_ret, Request) or isinstance(downloadermiddleware_ret, Response):
                await self.__channel__.write(downloadermiddleware_ret)
            else:
                await self.complete_request(request, spider, scraper)
    
    async def __process_downloads(self):
        downloading_pipelines = []
        async with self.__download_channel__.read() as reader:
              async for download_pipeline in reader:
                    downloading_pipeline = asyncio.ensure_future(download_pipeline)
                    downloading_pipelines.append(downloading_pipeline)
                    downloading_pipelines = [ pipeline for pipeline in downloading_pipelines if not pipeline.done()]
        await asyncio.gather(*downloading_pipelines)


    async def start(self):
       self.logger.debug('DownloaderManager start.')
       try:
           await asyncio.gather(self.__process_downloads(), self.__process_channel())
       except Exception as e:
            self.logger.exception(e)
       finally:
            await self.close()
            self.logger.debug('DownloaderManager closed.')

    def add_channel_receiver(self, receiver):
        self.__channel_receivers__.append(receiver)

    async def __process_channel(self):
        async with self.__channel__.read() as reader:
            async for item in reader:
                try:
                    await asyncio.gather(*[receiver(item) for receiver in self.__channel_receivers__])
                except Exception as e:
                    self.logger.exception(e)
                finally:
                    if self.idle():
                        self.__IDLE_EVENT__.set()

    async def close(self):
        if self.__closed__:
            return
        self.__closed__ = True
        self.__IDLE_EVENT__.set()
        await self.__channel__.join()
        await self.__channel__.close()
        await self.__download_channel__.join()
        await self.__download_channel__.close()
        acloses = []
        for d in self.__downloader__.values():
            close = ensure_asyncfunction(d.close)
            acloses.append(close())
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*acloses)
        except Exception as e:
            self.logger.exception(e)
        await self.__downloadermiddlewaremanager__.close()
        self.logger.debug('DownloaderManager being closed.')
