import asyncio
from contextlib import suppress
import logging
from typing import List, Optional, Tuple, Union, Type
from copy import deepcopy
from asyncio.tasks import FIRST_COMPLETED, Task, wait
from asyncio.locks import BoundedSemaphore, Event
from .core.response import Response
from .core.request import Request
from .core.middlewaremanager import SpiderMiddlewareManager
from .core.extension import ExtensionManager
from .core.pipeline import Pipeline
from .core import signal 
from .core.stream import Stream
from .util._async import CountdownLatch
from .core.context import Context, RequestContext
from .state import SpiderState
from .util._async import ensure_asyncgen 
from .core.exception import StarterNotFound, SpiderException, ParseException, StartException
from .core.slot import Slot, ReadFlag
from .spider import Parser, Starter, Spider
from .spider.routermanager import RouterManager
from .spider.statsmanager import StatsManager
from .stats import StatsCollector
from .core.flags import Idle
from .setting import settings as settings_loader

class RequestGraph:

    def __init__(self) -> None:
        self.__chain__ = []

    def add(self, item: Request, extra: object):
        pass

    def get(self, item: Request):
        pass

    def remove(self, item: Request):
        pass

class Scraper(object):
    """主要负责爬虫脚本的启动, 引擎（Engine）的调度通信，SpiderMiddleware的管理以及路由请求响应到指定的解析器中进行解析操作
    """

    logger = None 
    __slot : Slot
    __spiders : List[Spider]
    __running_tasks__: List[Task]
    __spidermiddlewaremanager__: SpiderMiddlewareManager
    __routemanager__:RouterManager
    __MAX_PIPELINES__:int
    __AVAILABLE_PIPELINES_SEMAPHOR__: Optional[BoundedSemaphore]
    __SPIDERMIDDLEWAREMANAGER_CLOSE__: Event
    __SPIDERS_STATUS__: StatsCollector
    __close__: bool
    settings:dict

    def __init__(self, settings: dict):
        self.logger = logging.getLogger(__name__)
        settings = settings if settings else settings_loader
        self.__MAX_PIPELINES__ = settings.get('MAX_PARSER_PROCESSES', -1)
        self.__spiders = []
        self.__running_tasks__ = []
        self.__closed__ = False
        self.__request_graph = RequestGraph()
        self.settings = settings
    
    @classmethod
    def from_settings(cls, settings: dict):
        """通过配置生成实例化Scraper对象

        Args:
            settings (dict): 配置对象

        Returns:
            Scraper: Scraper实例对象

        """
        return cls(settings)
    
    @classmethod
    async def create(cls, settings = None):
        settings = settings if settings is not None else settings_loader
        instance = cls.from_settings(settings)
        if instance.__MAX_PIPELINES__ > 0:
           instance.__AVAILABLE_PIPELINES_SEMAPHOR__ = BoundedSemaphore(instance.__MAX_PIPELINES__)
        else:
           instance.__AVAILABLE_PIPELINES_SEMAPHOR__ = None
        instance.__SPIDERMIDDLEWAREMANAGER_CLOSE__ = Event()
        instance.logger.debug('Scraper init.')
        instance.__spidermiddlewaremanager__ = await SpiderMiddlewareManager.create(settings)
        instance.__extensions__ = await ExtensionManager.create(settings)
        instance.__routemanager__ = RouterManager.from_settings(settings)
        instance.__SPIDERS_STATUS__ = StatsCollector.from_settings(settings)
        return instance
    
    def __set_spider_state(self, spider: Spider, state: SpiderState.States)->None:
        """设置爬虫实例的状态

        Args:
            spider (Spider): 爬虫实例对象
            state (SpiderState.States): 状态

        Returns:
            None

        :meta private:
        """
        ident = id(spider)
        spider_state = self.__SPIDERS_STATUS__.get_value(f'{ident}.state', None)
        if not spider_state:
           spider_state = SpiderState()
           self.__SPIDERS_STATUS__.set_value(f'{ident}.state', spider_state)
        spider_state.set_state(state)
    
    def __in_state(self, spider: Spider,state: SpiderState.States) -> bool:
        """获取爬虫实例是否处于某种状态

        Args:
            spider (Spider): 爬虫实例对象
            state (SpiderState.States): 状态

        Returns:
            bool: 爬虫实例是否处于状态，True为已经过某种状态，False为未经过某种状态
        
        :meta private:
        """
        ident = id(spider)
        spider_state = self.__SPIDERS_STATUS__.get_value(f'{ident}.state', None)
        if not spider_state:
           spider_state = SpiderState()
           self.__SPIDERS_STATUS__.set_value(f'{ident}.state', spider_state)
        return spider_state.in_state(state)
    
    async def __wait_spider_state(self, spider:Spider ,state: SpiderState.States):
        """等待爬虫实例直到它处于某种状态

        Args:
            spider (Spider): 爬虫实例对象
            state (SpiderState.States): 状态

        Returns:
            None

        :meta private:
        """
        ident = id(spider)
        spider_state = self.__SPIDERS_STATUS__.get_value(f'{ident}.state', None)
        if not spider_state:
           spider_state = SpiderState()
           self.__SPIDERS_STATUS__.set_value(f'{ident}.state', spider_state)
        await spider_state.wait_state(state)
 
    
    async def __acquire_pipeline_semaphor(self) -> None:
        """获取Pipeline信号量, 如果已有的信号量已使用完，该方法一直阻塞，知道有信号量被释放

        :meta private:
        """
        if not self.__AVAILABLE_PIPELINES_SEMAPHOR__:
            return
        await self.__AVAILABLE_PIPELINES_SEMAPHOR__.acquire()
        self.logger.debug('Semaphor value: '+str(getattr(self.__AVAILABLE_PIPELINES_SEMAPHOR__, '_value')))

    def __release_pipeline_semaphor(self):
        """释放Pipeline信号量

        :meta private:
        """
        if not self.__AVAILABLE_PIPELINES_SEMAPHOR__:
            return
        self.__AVAILABLE_PIPELINES_SEMAPHOR__.release()


    def add_spider(self, spider: Spider):
        """为Scraper添加管理的一个爬虫实例

        Args:
            spider (Spider): 爬虫实例

        """
        assert isinstance(spider, Spider), '{spider} is not an instance of Spider.'.format(spider=spider)

        spider.settings = self.settings
        self.__SPIDERS_STATUS__.set_value(f'{id(spider)}.state', SpiderState())
        self.__SPIDERS_STATUS__.set_value(f'{id(spider)}.parser_alive_counter', CountdownLatch())
        self.__SPIDERS_STATUS__.set_value(f'{id(spider)}.request_alive_counter', CountdownLatch())
        spider.stats = StatsManager.from_crawler(spider)
        for starter in spider.get_starters():
            self.__routemanager__.add_starter_route(spider.name+'.'+starter.name, starter)
        for parser in spider.get_parsers():
            self.__routemanager__.add_parser_route(spider.name+'.'+parser.name, parser)
        self.__spiders.append(spider)

    def bind(self, slot: Slot):
        """将 :py:obj:`Scraper <Scraper>` 与一个 :py:obj:`Slot <araneid.core.slot.Slot>` 绑定，绑定后，Scraper通过 :py:obj:`Slot <araneid.core.slot.Slot>` 和 :py:obj:`Engine <araneid.core.engine.Engine>` 进行通信

        Args:
            slot (Slot): Slot对象

        """
        self.__slot = slot
        slot.bind(scraper=self)

    def complete_response(self, response: Response):
        """完成对应的请求响应

        Args:
            response (Response): 请求响应对象

        """
        response_completed = self.__slot.complete_response(response)
        if response_completed:
           for state in response.States:
               if response.in_state(state):
                  continue
               response.set_state(state)
 
    def complete_request(self, request: Request):
        """完成对应的请求

        Args:
            request (Request): 请求对象
        
        """
        request_completed = self.__slot.complete_request(request)
        spider = request.context.spider
        if request_completed:
           for state in request.States:
               if request.in_state(state):
                  continue
               request.set_state(state)
        if spider is not None and request_completed:
           request_alive_counter: CountdownLatch = self.__SPIDERS_STATUS__.get_value(f'{id(spider)}.request_alive_counter')
           if request_alive_counter and request_alive_counter.count > 0:
              request_alive_counter.decrement()
           if spider.is_sync():
              self.__update_request_sync_state(request, incre=False)
    
    def __prepare_request_sync_state(self, request: Request):
        request_seeds_counter = CountdownLatch()
        request.meta['seeds'] = request_seeds_counter
        return request

    def __update_request_sync_state(self, request: Request, incre=True):
        prev = request
        while prev is not None:
            seeds_counter: CountdownLatch = prev.meta.get('seeds', None)
            if not seeds_counter:
               break
            if incre:
               seeds_counter.increment()
            elif seeds_counter.count > 0:
               seeds_counter.decrement()
            self.logger.debug(f'{prev} seeds: {seeds_counter.count}')
            prev = prev.meta.get('prev', None)

    async def __sync_request(self, request: Request):
        request_seeds_counter: CountdownLatch = request.meta.get('seeds', None)
        if not request_seeds_counter:
           return
        self.logger.debug(f'{request} sync lock.')
        await request_seeds_counter.wait()
        self.logger.debug(f'{request} sync unlock.')
    
    def is_sync(self, requestOrResponse:Union[Request, Response], spider: Spider):
        if spider and isinstance(spider, Spider) and spider.is_sync() and requestOrResponse.context and requestOrResponse.context.spider == spider:
            return True
        return False
 
    
    def is_completed(self, requestOrReqesponse: Union[Request,Response]) -> bool:
        """判断对应的请求或者请求响应是否完成

        Args:
            requestOrReqesponse (Union[Request,Response]): 请求或者请求响应的对象

        Returns:
            bool: 如果已完成，返回True，如果未完成，返回False
        
        """
        return self.__slot.is_completed(requestOrReqesponse)
    
    
    async def __create_parser_pipeline(self, response: Response, spider: Spider) -> None:
        """新开一个pipeline用来处理对应请求响应的解析任务。

        Args:
            response (Response): 需要被解析的请求响应
            spider (Spider): 请求响应所属的爬虫对象

        :meta private:
        """
        def __():
            self.complete_response(response)
            self.complete_request(response.request)
            if spider is not None:
               parser_alive_counter: CountdownLatch = self.__SPIDERS_STATUS__.get_value(f'{id(spider)}.parser_alive_counter')
               if parser_alive_counter.count > 0:
                  parser_alive_counter.decrement()
               self.__release_pipeline_semaphor()
        if spider is not None:
           await self.__acquire_pipeline_semaphor()
           parser_alive_counter: CountdownLatch = self.__SPIDERS_STATUS__.get_value(f'{id(spider)}.parser_alive_counter')
           parser_alive_counter.increment()
        pipeline_name = 'parser_pipe_'+str(response.request.uri)
        #self.logger.debug('fork_parser_pipeline for response: '+str(response))
        pipeline_task = self.__process_input(response, spider)
        pipeline = Pipeline(pipeline_task)
        pipeline.set_name(pipeline_name)
        pipeline.add_done_callback(lambda fut: __())
        return pipeline

    async def process_request(self, request: Request) -> None:
        """处理请求, 会调用 :py:meth:`Slot.put_request <araneid.core.slot.Slot.put_request>` 方法,将请求放到 :py:obj:`Slot <araneid.core.slot.Slot>`  的请求队列里面，等待引擎调度处理

        Args:
            request (Request): 处理的请求
        
        """
        # 如果已经关闭，所有Spider的请求都停止处理，并将请求的状态置为完成
        """
        if self.__closed__:
           for state in request.States:
               if request.in_state(state):
                  continue
               request.set_state(state)
           self.logger.debug(f'{request} is ignored (scraper closed)!')
           return
        """
        if not request.context:
           request.context =  RequestContext(None, None, self)
        elif request.context.scraper != self:
           request.context.scraper = self
        if request.in_state(request.States.start):
           request = request.from_request(request)
        request.set_state(request.States.start)
        if request.context and request.context.spider and isinstance(request.context.spider, Spider):
           spider = request.context.spider
           request_alive_counter: CountdownLatch = self.__SPIDERS_STATUS__.get_value(f'{id(spider)}.request_alive_counter')
           request_alive_counter.increment()
        await self.__slot.put_request(request)
        await signal.trigger(signal=signal.request_reached_slot,source=self, object=request, wait=False)

    async def process_response(self, response: Response) -> None:
        """处理请求响应, 会调用 :py:meth:`Slot.put_response <araneid.core.slot.Slot.put_response>` 方法，将请求响应放到 :py:obj:`Slot <araneid.core.slot.Slot>` 的请求响应队列里面，等待 ``Scraper`` 对对应的请求响应进行处理

        Args:
            response (Response): 处理的请求响应
        
        """
        if not response.context:
            response.context =  RequestContext(None, None, self)
        elif response.context.scraper != self:
            response.context.scraper = self
        await self.__slot.put_response(response)
    
    async def __process_input(self, response: Response, spider: Spider) -> None:
        """处理爬虫的输入

        该方法会将请求响应交给 :py:obj:`SpiderMiddlewareManager <araneid.core.middlewaremanager.SpiderMiddlewareManager>` 的 :py:meth:`process_spider_input <araneid.core.middlewaremanager.SpiderMiddlewareManager.process_spider_input>` 进行处理，然后，对爬虫中间件返回进行处理，如果返回是 :py:obj:`Request <araneid.core.request.Request>` , 
        会调用 :py:meth:`process_request <Scraper.process_request>` 方法将请求放到 :py:obj:`Slot <araneid.core.slot.Slot>` 的请求队列里面，等待引擎调度处理，如果返回的是 :py:obj:`Response <araneid.core.response.Response>` ,
        会调用 :py:meth:`parser_dispatch <Scraper.parser_dispatch>` 方法处理请求响应

        Args:
            response (Response): 请求响应
            spider (Spider): 请求响应所属的爬虫
        
        :meta private:
        """
        assert isinstance(response, Response)
        await signal.trigger(signal=signal.response_received, source=self, object=response, wait=False)
        spidermw_ret = await self.__spidermiddlewaremanager__.process_spider_input(response, spider, self)
        spidermw_ret = ensure_asyncgen(spidermw_ret)
        async for ret in spidermw_ret:
           if isinstance(ret, Response):
              await self.__parser_dispatch(ret, spider)
           elif isinstance(ret, Request):
              call_context = ret.context.caller if ret.context else None
              self.logger.debug(f'Put {ret} into slot cache from {str(call_context)}')
              if self.is_sync(ret, spider):
                 ret.meta['prev'] = response.request
                 self.__prepare_request_sync_state(ret)
                 self.__update_request_sync_state(ret)
              await self.process_request(ret)
              if self.is_sync(ret, spider):
                 await self.__sync_request(ret)
        await signal.trigger(signal=signal.response_parsed, source=self, object=response, wait=False)
    
    async def __process_output(self, response:Response, result: List[Union[Request, Response]], parser:Parser, spider: Spider):
        """处理爬虫的输出

        该方法会将请求响应交给 :py:obj:`SpiderMiddlewareManager <araneid.core.middlewaremanager.SpiderMiddlewareManager>` 的 :py:meth:`process_spider_output <araneid.core.middlewaremanager.SpiderMiddlewareManager.process_spider_output>` 进行处理，然后，对爬虫中间件返回进行处理，如果返回是 :py:obj:`Request <araneid.core.request.Request>` , 
        会调用 :py:meth:`process_request <Scraper.process_request>` 方法将请求放到 :py:obj:`Slot <araneid.core.slot.Slot>` 的请求队列里面，等待引擎调度处理，如果返回的是 :py:obj:`Response <araneid.core.response.Response>` ,
        会调用 :py:meth:`process_response <Scraper.process_response>` 方法将请求放到 :py:obj:`Slot <araneid.core.slot.Slot>` 的请求响应队列里面，等待Scraper对请求响应进行处理

        Args:
            response (Response): 被处理的请求响应
            result (List[Union[Request, Response]]): 被处理的请求响应的结果
            spider (Spider): 请求响应所属的爬虫
        
        :meta private:
        """

        if not isinstance(result, list):
            result = [result]
        spidermw_ret = await self.__spidermiddlewaremanager__.process_spider_output(response, result, spider, self)
        spidermw_ret = ensure_asyncgen(spidermw_ret)
        async for ret in spidermw_ret:
            if isinstance(ret, Response):
                await self.process_response(ret)
            elif isinstance(ret, Request):
                call_context = ret.context.caller if ret.context else None
                ret.meta['prev'] = response.request
                self.logger.debug(f'Put {ret} into slot cache from {str(call_context)}')
                try:
                    is_sync, is_circle, circle_req = (True, *self.__predict_parser_dag_circle(ret, parser, spider)) if self.is_sync(ret, spider) else (False, False, None)
                    if is_circle:
                       ret.meta['prev'] = circle_req
                    if is_sync:
                       self.__prepare_request_sync_state(ret)
                       self.__update_request_sync_state(ret)
                    await self.process_request(ret)
                    if is_sync and not is_circle:
                       await self.__sync_request(ret)
                except Exception as e:
                    self.logger.exception(e)
                

    async def __process_parse_exception(self, response: Response, exception: Exception, spider: Spider, parser: Parser):
        """处理爬虫解析异常

        该方法会将异常首先交给 :py:obj:`SpiderMiddlewareManager <araneid.core.middlewaremanager.SpiderMiddlewareManager>`的 :py:meth:`process_spider_exception <araneid.core.middlewaremanager.SpiderMiddlewareManager.process_spider_exception>` 进行处理，然后，对爬虫中间件返回进行处理，如果返回是 :py:obj:`Request <araneid.core.request.Request>` , 
        会调用 :py:meth:`process_request <Scraper.process_request>` 方法将请求放到 :py:obj:`Slot <araneid.core.slot.Slot>` 的请求队列里面，等待引擎调度处理，如果返回的是 :py:obj:`Response <araneid.core.response.Response>` ,
        会调用 :py:meth:`process_response <Scraper.process_response>` 方法将请求放到 :py:obj:`Slot <araneid.core.slot.Slot>` 的请求响应队列里面，等待Scraper对请求响应进行处理

        Args:
            response (Response): 被处理的请求响应
            exception (Exception): 被处理的请求响应的结果
            spider (Spider): 请求响应所属的爬虫
            ctx (Context): 出现异常的上下文, 默认为None
        
        :meta private:
        """
        try:
            if not isinstance(spider, Spider):
               raise ParseException(f"Exception in Parser {parser.name}") from exception
            raise ParseException(f"Exception in Parser {parser.name} of Spider {spider.name}") from exception
        except ParseException as e:
            spidermw_ret = await self.__spidermiddlewaremanager__.process_spider_exception(response, e, spider, self)
            spidermw_ret = ensure_asyncgen(spidermw_ret)
            async for ret in spidermw_ret:
                if isinstance(ret, Response):
                    await self.process_response(ret)
                elif isinstance(ret, Request):
                    await self.process_request(ret)

    def __parser_route_from_response(self, response: Response, spider=None) -> List[Parser]:
        """通过请求响应来路由对应的解析器

        该方法通过 :py:obj:`araneid.spider.routemanager.RouteManager` 的 :py:meth:`parser_route <araneid.spider.routemanager.RouteManager.parser_route>` 方法获取对应
        的请求响应路由的解析器，然后如果请求响应对应的请求有定义 :py:attr:`callbacks <araneid.core.request.Request.callbacks>` 或者 :py:attr:`callback <araneid.core.request.Request.callback>`
        属性，对应的函数也会被当作解析器加入.


        Args:
            response (Response): 请求响应

        Returns:
            List[Parser]: 对应的请求响应的解析器

        :meta private:
        """
        def ensure_parser(callback):
           if isinstance(callback, Parser):
              return callback
           parser = Parser(name=callback.__name__)
           parser.bind(callback, spider=spider)
           return parser

        parsers = []
        callback = getattr(getattr(response, 'request', {}), 'callback', None)
        callbacks = getattr(getattr(response, 'request', {}), 'callbacks', [])
        parser_route_rules =  [getattr(getattr(response, 'request', {}), 'uri', '')]
        routed_target_parser = self.__routemanager__.parser_route(parser_route_rules)
        if callback:
           callback_parser = ensure_parser(callback) 
           parsers.append(callback_parser)
        if not callback and callbacks:
           callback_parsers =  [ ensure_parser(callback) for callback in callbacks]
           parsers.extend(callback_parsers)
        if routed_target_parser:
           parsers.append(routed_target_parser)
        return parsers

    def __parser_route_from_request(self, request: Request, spider=None) -> List[Parser]:
        def ensure_parser(callback):
           if isinstance(callback, Parser):
              return callback
           parser = Parser(name=callback.__name__)
           parser.bind(callback, spider=spider)
           return parser
        parsers = []
        callback = getattr(request, 'callback', None)
        callbacks = getattr(request, 'callbacks', [])
        parser_route_rules =  [getattr(request, 'uri', '')]
        routed_target_parser = self.__routemanager__.parser_route(parser_route_rules)
        if callback:
           callback_parser = ensure_parser(callback) 
           parsers.append(callback_parser)
        if not callback and callbacks:
           callback_parsers =  [ ensure_parser(callback) for callback in callbacks]
           parsers.extend(callback_parsers)
        if routed_target_parser:
           parsers.append(routed_target_parser)
        return parsers

    def __predict_parser_dag_circle(self, request: Request, parser: Parser, spider) -> Tuple[bool, Request]:
        request_parsers = self.__parser_route_from_request(request, spider)
        prev_request = request.meta.get('prev', None)
        is_circle = False
        circle_request = None
        while prev_request is not None:
            prev_request_parsers = self.__parser_route_from_request(prev_request, spider)
            if prev_request_parsers == request_parsers:
               is_circle  = True
               circle_request = prev_request
               break
            prev_request = prev_request.meta.get('prev', None)
        else:
            is_circle = parser in request_parsers
            circle_request = request
        return is_circle, circle_request.meta['prev'] if circle_request.meta['prev'] else circle_request

         
    
    def __parser_items_from_response(self, parser: Parser, response: Response) -> None:
        """将从 :py:obj:`Response <araneid.core.response.Response>` 中获取上一步 :py:obj:`Parser <araneid.spider.spider.Parser>` 中的解析出来的items数据, 并更新 `parser` 的items数据

        Args:
            parser (Parser): 需要更新items数据的Parser
            response (Response): 获取items数据的请求响应
        
        :meta private:
        """
        assert isinstance(parser, Parser) and isinstance(response, Response)
        last_items = getattr(response, '__odata__', {}).get('items', None)
        if not last_items:
            return
        parser.items.update(last_items)
    
    def __parser_items_to_request(self, parser: Parser, request: Request) -> None:
        """将 :py:obj:`Parser <araneid.spider.spider.Parser>` 中解析的items数据保存到请求中

        Args:
            parser (Parser): 需要保存items数据的Parser
            request (Request): 保存items数据的请求
        
        :meta private:
        """
        assert isinstance(parser, Parser) and isinstance(request, Request)
        __data__ = getattr(request, '__odata__')
        __data__['items'] = deepcopy(parser.items)

    async def __parser_dispatch(self, response: Response, spider: Spider) -> None:
        """给请求响应路由解析器，并调用解析器对请求响应进行解析

        Args:
            response (Response): 需要解析的请求响应
            spider (Spider): 请求响应所属的爬虫
        
        :meta private:
        """
        assert isinstance(response, Response)
        context_from_spider = None if not response.context else response.context.spider
        context_from_caller = None if not response.context else response.context.caller
        context_from_scraper = None if not response.context else response.context.scraper
        parsers = self.__parser_route_from_response(response, spider=spider)
        if not parsers:
            self.logger.warning(str(response.request)+' is not bind by any parser, skip.')
            return
        for parser in parsers:
            try:
                # collect all previous item results into parser
                if isinstance(parser, Parser):
                   self.__parser_items_from_response(parser, response)
                async for res in parser(response):
                    if not res:
                        continue
                    if isinstance(res, Request):
                        res.context = RequestContext(parser, context_from_spider, context_from_scraper)
                        if isinstance(parser, Parser):
                            self.__parser_items_to_request(parser, res)
                    await self.__process_output(response, res, parser, spider)
            except Exception as e:
                await self.__process_parse_exception(response, e, spider, parser)
        request:Request = response.request
        request.set_state(request.States.parse)
        response.set_state(response.States.parse)
 
    async def __process_start_requests(self, starter: Starter, spider: Spider) -> None:
        """处理爬虫的请求初始化器

        Args:
            starter (Starter): 需要处理的爬虫请求初始化器
            spider (Spider): 请求初始化器所属的爬虫

        Raises:
            CancelledError: 协程被取消运行时抛出
        
        :meta private:
        """

        after_starter = await self.__spidermiddlewaremanager__.process_spider_start_requests(starter(), spider, self)
        async_after_starter = ensure_asyncgen(after_starter)
        try:
            async for ritem in async_after_starter:
                if self.__closed__:
                   self.logger.debug(f'Starter {starter.name} of Spider {spider.name} stop processing (scraper closed)! ')
                   break
                if not isinstance(ritem, Request):
                    continue
                ritem.context = RequestContext(starter, spider, self) # Context(parser or starter, spider instance, scraper instance)
                self.logger.debug(f'Put {ritem} into slot cache from {starter}')
                self.logger.debug(f'{ritem} context {ritem.context}')
                if spider.is_sync():
                   self.__prepare_request_sync_state(ritem)
                   self.__update_request_sync_state(ritem)
                await self.process_request(ritem)
                if spider.is_sync():
                   await self.__sync_request(ritem)
        except asyncio.CancelledError as e:
            raise e
        except Exception as e:
            try:
                raise StartException(f"Exception in Starter {starter.name} of {spider.name}") from e
            except StartException as e:
                await self.__spidermiddlewaremanager__.process_spider_exception(None, e, spider, self)

    async def __process_spider_parsers(self, spider: Spider):
        """处理爬虫的请求响应的解析

        Args:
            spider (Spider): 需要处理请求响应的爬虫
        
        :meta private:
        """
        async def __process():
            running_parsers: List[Pipeline] = [] 
            while not self.__slot.is_close():
                  response = await self.__slot.get_response(delay_complete=True, spider=True)
                  if not isinstance(response, Response):
                    continue
                  parser_pipeline: Pipeline = await self.__create_parser_pipeline(response, spider)
                  running_parsers.append(parser_pipeline)
                  running_parsers = [ parser  for parser in running_parsers if not parser.done()]
            await asyncio.gather(*running_parsers)
        await asyncio.gather(__process())
    
    async def __start_spidermiddleware_parsers(self):
        """处理爬虫中间件中的请求响应的解析
        """
        async def __process():
            running_parsers: List[Pipeline] = [] 
            while not self.__slot.is_close():
                  response = await self.__slot.get_response(delay_complete=True, spider=False)
                  if not isinstance(response, Response):
                    continue
                  parser_pipeline: Pipeline = await self.__create_parser_pipeline(response, None)
                  running_parsers.append(parser_pipeline)
                  running_parsers = [ parser  for parser in running_parsers if not parser.done()]
            await asyncio.gather(*running_parsers)
        await asyncio.gather(__process())
         
    async def __start_spider_starter(self, spider: Spider) -> None:
        """处理爬虫的请求初始化器, 通过路由找到对应的请求初始化器，并调用请求初始化器

        Args:
            spider (Spider): 请求初始化器的爬虫

        Raises:
            StarterNotFound: 请求初始化器找不到时抛出改异常
        """
        starter_route_rule = ['starter.'+spider.name+'.'+spider.get_start_starter().name]
        starter = self.__routemanager__.starter_route(starter_route_rule)
        if not starter:
            raise StarterNotFound('Stater '+str(starter_route_rule)+' not found ')
        try:
            await self.__process_start_requests(starter,spider)
        except Exception as e:
            raise e
        finally:
            self.__set_spider_state(spider, SpiderState.States.STARTER_CLOSE)
        self.logger.debug(f'Spider {spider.name} starter {starter.name} closed.')

    async def __start_spider_parser(self, spider: Spider) -> None:
        """处理爬虫的解析器

        Args:
            spider (Spider): 调用解析器的爬虫
        """
        await asyncio.gather(self.__process_spider_parsers(spider))
        self.__set_spider_state(spider, SpiderState.States.PARSER_CLOSE)
        self.logger.debug(f'Spider {spider.name} parsers closed.')

      
    async def __start_spider(self, spider: Spider)-> None:
        """处理爬虫, 启动爬虫的解析器处理，以及请求初始化器处理，并管理爬虫的生命周期：START -> RUNNING -> CLOSE

        Args:
            spider (Spider): 处理的爬虫实例
        """
        try:
            spider_running_tasks = []
            self.__set_spider_state(spider, SpiderState.States.START)
            await spider.start()
            await spider.stats.start_spider(spider, self)
            await signal.trigger(signal.spider_started, source=self, object=spider, wait=False)
            self.__set_spider_state(spider, SpiderState.States.RUNNING)
            spider_running_tasks.extend([asyncio.create_task(self.__start_spider_starter(spider)), asyncio.create_task(self.__start_spider_parser(spider))])
            self.__running_tasks__.extend(spider_running_tasks)
            await asyncio.gather(*[ asyncio.shield(spider_running_task) for spider_running_task in spider_running_tasks])
            await spider.stats.close_spider(spider, self)
        except asyncio.CancelledError:
            self.logger.warning(f'Spider {spider.name} is cancelled while running!')
        except Exception as e:
            raise SpiderException(f"Exception in Spider {spider.name}") from e
        finally:
            self.__set_spider_state(spider, SpiderState.States.CLOSE)
            await spider.close()
            self.logger.debug(f'Spider {spider.name} closed.')

    async def __start_spidermiddleware(self):
        """处理爬虫中间件, 启动爬虫中间件的解析器处理
        """
        await self.__start_spidermiddleware_parsers()
        self.__SPIDERMIDDLEWAREMANAGER_CLOSE__.set()

    async def __wait_close_spidermiddleware_manager(self):
        """等待爬虫中间件关闭
        """
        await self.__SPIDERMIDDLEWAREMANAGER_CLOSE__.wait()

    
    async def __start(self):
        """开始爬虫中间件的处理以及爬虫处理
        """
        try:
            start_spiders = [ asyncio.create_task(self.__start_spider(spider)) for  spider in self.__spiders]
            start_spidermids = [asyncio.create_task(self.__start_spidermiddleware())]
            self.__running_tasks__.extend(start_spiders)
            self.__running_tasks__.extend(start_spidermids)
            start_tasks = start_spiders + [asyncio.shield(start_spidermid)  for start_spidermid in start_spidermids]
            await self.__slot.open()
            await asyncio.gather(*start_tasks)
        except Exception as e:
            self.logger.exception(e)

    async def wait_close_spider(self, spider_cls: Union[Type[Spider], str]=None) -> None:
        """等待爬虫关闭, 如果未指定爬虫, 则等待所有爬虫关闭。

        Args:
            spider_cls (Union[Type[Spider], str], optional): 指定需要等待关闭的爬虫，可以是爬虫实例，爬虫类，以及爬虫名字. 默认为： None, 则等待所有爬虫关闭.
        """
        close_spiders = []
        if not spider_cls:
           close_spiders += [ self.__wait_spider_state(spider, SpiderState.States.CLOSE) for spider in self.__spiders]
        else:
            for spider in self.__spiders:
                if isinstance(spider_cls, Spider) and spider == spider_cls:
                    close_spiders.append(self.__wait_spider_state(spider, SpiderState.States.CLOSE))
                elif issubclass(spider_cls, Spider) and isinstance(spider, spider_cls):
                    close_spiders.append(self.__wait_spider_state(spider, SpiderState.States.CLOSE))
        if not close_spiders:
           return
        await asyncio.gather(*close_spiders)
    
    async def wait_idle(self)-> None:
        """等待Scraper空闲，即所有的爬虫处理完成后。
        """
        await self.wait_close_spider()
    
    async def __wait_closable_spider(self, spider):
        request_alive_counter: CountdownLatch = self.__SPIDERS_STATUS__.get_value(f'{id(spider)}.request_alive_counter')
        #parser_alive_counter: CountdownLatch = self.__SPIDERS_STATUS__.get_value(f'{id(spider)}.parser_alive_counter')
        await self.__wait_spider_state(spider, SpiderState.States.STARTER_CLOSE)
        #await parser_alive_counter.wait()
        if request_alive_counter:
           await request_alive_counter.wait()
        #t = await asyncio.gather(self.__wait_spider_state(spider, SpiderState.States.STARTER_CLOSE), parser_alive_counter.wait())
        self.logger.debug(f'Spider {spider} is closable')


    
    async def __wait_closable(self):
        """当所有的爬虫的请求初始化器（Starter）和所有解析器（Parser）都关闭后，Scraper应该关闭
        """
        await asyncio.gather(*[ self.__wait_closable_spider(spider) for spider in self.__spiders])
        self.logger.debug('Scraper is closable')

    async def start(self):
        """启动Scraper，会同时启动爬虫, 以及爬虫中间件的处理
        """
        try:
            self.__running_tasks__.append(asyncio.create_task(self.__start()))
            done, _= await asyncio.wait([*self.__running_tasks__, self.__wait_closable()], return_when=FIRST_COMPLETED)
            for future in done:
                future.result()
        except asyncio.CancelledError:
            self.logger.warning(f'Scraper closing by being canceled!')
        except Exception as e:
            self.logger.exception(e)
        finally:
            await self.close()
            self.logger.debug("Scraper closed.")
    
    async def close(self):
        """关闭Scraper， 关闭爬虫中间件以及跟Scraper绑定的 :py:obj:`Slot <araneid.core.slot.Slot>`
        """
        if self.__closed__:
            return
        self.__closed__ = True
        signal_waiters = []
        for spider in self.__spiders:
            waiter = await signal.trigger(signal.spider_closed, source=self, object=spider)
            signal_waiters.append(waiter)
        await asyncio.gather(*signal_waiters)
        await self.__spidermiddlewaremanager__.close()
        await self.__extensions__.close()
        await self.__slot.close()
        self.logger.debug('scraper_close_1')
        await self.wait_close_spider()
        self.logger.debug('scraper_close_2')
        await self.__wait_close_spidermiddleware_manager()
        self.logger.debug('scraper_close_3')
        self.__SPIDERS_STATUS__.clear_stats()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*self.__running_tasks__)
        self.logger.debug("Scraper being closed.")

