import asyncio
from contextlib import suppress
import logging
import inspect
from asyncio import CancelledError
from araneid.util._async import ensure_asyncfunction
from araneid.spider import Spider
from .context import RequestContext
from .middleware import SpiderMiddleware, DownloaderMiddleware
from .request import Request
from .response import Response
from .exception import DownloaderWarn, InvalidSpiderMiddleware, InvalidDownloaderMiddleware,  \
DownloaderMiddlewareError, SpiderMiddlewareError, IgnoreRequest, NotConfigured, SpiderException, InvalidRequestErrback
from . import signal, plugin as plugins


class MiddlewareManager(object):
    pass

class SpiderMiddlewareManager(MiddlewareManager):
    logger = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('SpiderMiddlewareManager init.')
        self.__process_spider_output = []
        self.__process_spider_input = []
        self.__process_spider_exception = []
        self.__process_spider_start_requests = []
        self.__process_close = []
        self.__active_middlewares = []
    
    @classmethod
    async def create(cls, settings=None):
        instance = cls.from_settings(settings)
        spider_middlewares:dict = await instance.__load_plugin(settings)
        for _, middlewares in spider_middlewares.items():
            for name, middleware in  middlewares.items():
                instance.add_middleware(name, middleware)
        return instance

    async def __load_plugin(self, settings):
        spidermiddleware_plugins = plugins.load(plugins.PluginType.SPIDERMIDDLEWARE)
        spider_middlewares = dict()
        for plugin in spidermiddleware_plugins:
            name = plugin.name
            spider_middleware = plugin.load()
            try:
                if not issubclass(spider_middleware, SpiderMiddleware):
                   raise InvalidSpiderMiddleware(f'SpiderMiddleware {name} is invalid, {name} must implement one of [process_spider_input(), process_spider_output(), process_spider_exception(), process_spider_start_requests()] and [order()] method!') 
                if hasattr(spider_middleware, 'from_settings'):
                    inst_spider_middleware  = await spider_middleware.create(settings)
                else:
                    inst_spider_middleware = await spider_middleware.create()
                try:
                    order = float(inst_spider_middleware.order())
                except ValueError:
                   raise InvalidSpiderMiddleware(f'SpiderMiddleware {name} order method must return a number!') 
                order_spider_middleware = spider_middlewares.get(order, {})
                order_spider_middleware[name] = inst_spider_middleware
                spider_middlewares[order] = order_spider_middleware 
            except NotConfigured as e:
                self.logger.warning(f"SpiderMiddleware {name} is not configured, skipped load.")
                continue
            except Exception as e:
                raise SpiderMiddlewareError(f"Error occurred in while loading spidermiddleware {name}!") from e
            self.logger.debug(f'Loaded spidermiddleware: {name}.')
        return dict(sorted(spider_middlewares.items()))

    @classmethod
    def from_settings(cls, setting):
        inst = cls()
        return  inst

    def add_middleware(self, name, middleware):
        if not isinstance(middleware, SpiderMiddleware):
            raise InvalidSpiderMiddleware(f'SpiderMiddleware {name} is invalid.') 
        process_spider_output = getattr(middleware, 'process_spider_output', None)
        process_spider_input = getattr(middleware, 'process_spider_input', None)
        process_spider_start_requests = getattr(middleware, 'process_spider_start_requests', None)
        process_spider_exception = getattr(middleware, 'process_spider_exception', None)
        close = getattr(middleware, 'close', None)
        self.__process_spider_output.append(process_spider_output)
        self.__process_spider_input.append(process_spider_input)
        self.__process_spider_start_requests.append(process_spider_start_requests)
        self.__process_spider_exception.append(process_spider_exception)
        self.__process_close.append(close)
        self.__active_middlewares.append(getattr(middleware,'__module__', 'module')+'.'+getattr(getattr(middleware, '__class__', object()),'__qualname__', ''))
    
    def list_middleware(self):
        return self.__active_middlewares
    
    async def default_exception_handle(self, response: Response, exception: Exception, spider: Spider, scraper):
        """默认的异常处理函数，处理异常处理链未处理的异常

        Args:
            response (_type_): 异常发生时处理的请求响应
            exception (_type_): 发生的异常
            spider (_type_): 发生异常的爬虫
            scraper (_type_): 发生异常的Scraper示例
        """
        if isinstance(spider, Spider):
            try:
                raise SpiderException(f"Exception in Spider {spider.name}") from  exception
            except Exception as e:
                self.logger.exception(e)
        else:
            self.logger.exception(exception)

    async def process_spider_output(self, response, result, spider, scraper, start_from=0):
        assert(start_from >= 0)
        processes = self.__process_spider_output[start_from:]
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            if not result:
                break
            async_p = ensure_asyncfunction(p)
            try:
                result = await async_p(response, result, spider)
                if not isinstance(result, list):
                   raise InvalidSpiderMiddleware('return type of {middleware_procedure} must be a list.'.format(middleware_procedure=async_p))
                # update context of request from spidermiddleware
                for item in result:
                    if not isinstance(item, Request):
                        continue
                    if item.context:
                        continue
                    item.context = RequestContext(p, p.__self__, scraper)
            except Exception as e:
                result = await self.process_spider_exception(response, e, spider, index+1) 
                return result
        return result
    
    async def process_spider_input(self, response, spider, scraper, start_from=0):
        assert(start_from >= 0)
        processes = self.__process_spider_input[start_from:]
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            async_p = ensure_asyncfunction(p)
            try:
                result = await async_p(response, spider)
            except Exception as e:
                errback = getattr(response.request, 'errback', None)
                error = e
                result = None
                if errback:
                    try:
                       result = errback(response, error, spider)
                    except Exception as e:
                        error = e
                    else:
                        error = None
                if not error:
                    result = await self.process_spider_output(response, result, spider, scraper)
                    return result
                else:
                    result = await self.process_spider_exception(response, error, spider, scraper)
                    return result
            else:
                if result is not  None:
                    break
        return response
 
    async def process_spider_exception(self, response, exception, spider, scraper, start_from=0):
        """当爬虫异常的时候，链式调用调用注册的爬虫中间件的 `process_spider_exception` 方法。
        1. 异常经process_exception函数进行处理，并没有发生异常并产生非空的结果，则中断异常处理链, 按照异常处理链继续处理异常。
        2. 如果经过整个处理链条，还有异常未处理，调用默认的异常处理函数进行处理。

        Args:
            response (_type_): 异常发生时处理的请求响应
            exception (_type_): 发生的异常
            spider (_type_): 发生异常的爬虫
            scraper (_type_): 发生异常的Scraper示例
            start_from (int, optional): 当前处理异常的顺序，控制避免出现重复调用

        Raises:
            exception: 未处理的异常
        """
        assert(start_from >= 0)
        unhandle_exception = exception
        processes = self.__process_spider_exception[start_from:]
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            async_p = ensure_asyncfunction(p)
            try:
                result = await async_p(response, unhandle_exception, spider, scraper)
            except Exception as e: 
                unhandle_exception = e
                result = None
            else:
                unhandle_exception = None

            if result is None:
               continue
            elif not isinstance(result, Exception):
                try:
                   await self.process_spider_output(response, result, spider, scraper, index+1)
                except Exception as e:
                    unhandle_exception =  e
                else:
                    unhandle_exception = None
                    break
            else:
               unhandle_exception =  result
               continue
        if not unhandle_exception:
           return
        await self.default_exception_handle(response, unhandle_exception, spider ,scraper)
    
    async def process_spider_start_requests(self, start_requests, spider, scraper, start_from=0):
        assert(start_from >= 0)
        processes = self.__process_spider_start_requests[start_from:]
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            async_p = ensure_asyncfunction(p)
            try:
                start_requests = await async_p(start_requests, spider)
            except Exception as e :
                result = await self.process_spider_exception(None, e, spider, index)
                return result
            if inspect.isgenerator(start_requests) or inspect.isasyncgen(start_requests):
                continue
            else:
                self.logger.warning('process_spider_start_requests in {spidermw} must return a generator!'.format(spidermw=p))
                break
        return start_requests
    
    async def close(self):
        acloses = []
        for close in self.__process_close:
            if not close:
                continue
            async_close = ensure_asyncfunction(close)
            acloses.append(async_close())
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*acloses)
        except Exception as e:
            self.logger.exception(e)
        self.logger.debug('SpiderMiddlewareManager closed.')


class DownloaderMiddlewareManager(MiddlewareManager):
    logger = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__process_request = []
        self.__process_response = []
        self.__process_exception= []
        self.__process_close = []
        self.__active_middlewares = []

    @classmethod
    def from_settings(cls, setting):
        inst = cls()
        return  inst

    @classmethod
    async def create(cls, settings=None):
        instance = cls.from_settings(settings)
        download_middlewares:dict = await instance.__load_plugin(settings)
        for _, middlewares in download_middlewares.items():
            for name, middleware in middlewares.items():
                instance.add_middleware(name, middleware)
        return instance
    
    async def __load_plugin(self, settings):
        downloadmiddleware_plugins = plugins.load(plugins.PluginType.DOWNLOADMIDDLEWARE)
        download_middlewares = dict()
        for plugin in downloadmiddleware_plugins:
            name = plugin.name
            download_middleware = plugin.load()
            try:
                if not issubclass(download_middleware, DownloaderMiddleware):
                   raise InvalidDownloaderMiddleware(f'DownloaderMiddleware {name} is invalid,  {name} must implement one of [process_request(), process_response(), process_exception()] and [order()] method!') 
                if hasattr(download_middleware, 'from_settings'):
                    inst_download_middleware = await download_middleware.create(settings)
                else:
                    inst_download_middleware = await download_middleware.create()
                try:
                    order = float(inst_download_middleware.order())
                except ValueError:
                   raise InvalidSpiderMiddleware(f'DownloaderMiddleware {name} order method must return a number!') 
                order_download_middlewares=download_middlewares.get(order, {})
                order_download_middlewares[name] = inst_download_middleware 
                download_middlewares[order] = order_download_middlewares 
            except NotConfigured as e:
                self.logger.warning(f"Downloadmiddleware {name} is not configured, skipped load.")
                continue
            except Exception as e:
                raise DownloaderMiddlewareError(f"Error occurred in while loading downloadmiddleware {name}!") from e
            self.logger.debug(f'Loaded downloadermiddleware: {name}.')
        return dict(sorted(download_middlewares.items()))

    def add_middleware(self, name, middleware):
        if not isinstance(middleware, DownloaderMiddleware):
            raise InvalidDownloaderMiddleware('DownaloderMiddleware '+name+' is invalid.') 
        process_response = getattr(middleware, 'process_response', None)
        process_request = getattr(middleware, 'process_request', None)
        process_exception = getattr(middleware, 'process_exception', None)
        close = getattr(middleware, 'close', None)
        self.__process_request.append(process_request)
        self.__process_response.append(process_response)
        self.__process_exception.append(process_exception)
        self.__process_close.append(close)
        self.__active_middlewares.append(getattr(middleware,'__module__', 'module')+'.'+getattr(getattr(middleware, '__class__', object()),'__qualname__', ''))
   
    def list_middleware(self):
        return self.__active_middlewares
    
    async def default_exception_handle(self, request, exception, spider):
        if isinstance(exception, CancelledError):
            return
        if isinstance(exception, DownloaderWarn):
            self.logger.warning(exception)
        else:
            if isinstance(spider, Spider):
               self.logger.exception('Exception occurred while downloading request {request} from {spider}:'.format(request=request, spider=spider), exc_info=exception)
            else:
               self.logger.exception('Exception occurred while downloading request {request}:'.format(request=request, spider=spider), exc_info=exception)

    
    async def process_request(self, request, spider, start_from=0):
        assert(type(start_from) is int and start_from >= 0)
        processes = self.__process_request[start_from:]
        result = request
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            async_p = ensure_asyncfunction(p)
            try:
                result = await async_p(result, spider)
            except Exception as e:
                return await self.process_exception(result, e, spider, index)
            else:
                if not isinstance(result, Request):
                   await signal.trigger(signal.request_dropped, source=p, object=request, wait=False)
                   return result
        return result

    async def process_response(self, request, response, spider, start_from=0):
        assert(type(start_from) is int and start_from >= 0)
        processes = self.__process_response[start_from:]
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            async_p = ensure_asyncfunction(p)
            try:
                result = await async_p(response, request, spider)
            except IgnoreRequest as ige:
                errback = getattr(request, 'errback', None)
                if not errback:
                   break
                async_errback = ensure_asyncfunction(errback)
                return await async_errback(request, ige, spider)
            except Exception as e:
                errback = getattr(request, 'errback', None)
                if not errback:
                    raise e
                async_errback = ensure_asyncfunction(errback)
                return await async_errback(request, e, spider)
            else:
                if result is None:
                    continue
                elif isinstance(result, Request):
                    return result
                elif isinstance(result, Response):
                    response = result
                    continue
        return response 

    async def process_exception(self, request, exception, spider, start_from=0):
        assert(type(start_from) is int and start_from >= 0)
        processes = self.__process_exception[start_from:]
        unhandled_exception = exception
        for index in range(len(processes)):
            p = processes[index]
            if p is None:
                continue
            async_p = ensure_asyncfunction(p)
            try:
                result = await async_p(request, unhandled_exception, spider)
            except Exception as e:
                unhandled_exception =  e
            else:
                if isinstance(result, Response):
                    return await self.process_response(request, result, spider, start_from=start_from)
                elif isinstance(result, Request):
                    return await self.process_request(request, spider, start_from=start_from) 
        if not unhandled_exception:
            return
        errback = getattr(request, 'errback', None)
        if errback:
           async_errback = ensure_asyncfunction(errback)
           try:
              result = await async_errback(request, exception, spider)
           except Exception as e:
               unhandled_exception = e
           else:
               if inspect.isgenerator(result) or inspect.isasyncgen(result):
                  raise InvalidRequestErrback(f"errback {errback} must be not be a generator or async generator!")
               return result
        await self.default_exception_handle(request, unhandled_exception, spider)

    async def close(self):
        acloses = []
        for close in self.__process_close:
            if not close:
                continue
            async_close = ensure_asyncfunction(close)
            acloses.append(async_close())
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*acloses)
        except Exception as e:
            self.logger.exception(e)
        self.logger.debug('DownloadMiddlewareManager closed.')
