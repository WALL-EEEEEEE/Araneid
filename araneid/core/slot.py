from enum import IntFlag
import logging
import random
import asyncio
from enum import IntEnum, auto
from asyncio.locks import Event
from asyncio.futures import Future
from araneid.core.flags import Idle
from araneid.core.stream import Stream
from araneid.spider.spider import Spider
from .request import Request
from .response import Response
from .exception import SlotUnbound

class ReadFlag(IntEnum):
   spider_request = auto()
   nospider_request = auto()
   spider_response = auto()
   nospider_response = auto()

class Slot:
    """
    :py:obj:`Scraper <araneid.scraper.Scraper>` 主要通过它与  :py:obj:`Engine <araneid.core.engine.Engine>` 进行一系列的通信。
    """
 
    __slots__ = ['logger', '__scraper', '__engine', '__response_channel', '__request_channel', '__spider_request_channel', '__spider_response_channel', '__middleware_request_channel', '__middleware_response_channel', '__open', '__close', '__close_waiter', '__open_waiter', '__IDLE_EVENT', '__idle_status']

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__close = False
        self.__open = False
        self.__scraper = None
        self.__engine =  None
        self.__IDLE_EVENT: Event = Event()
    
    @classmethod
    async def create(cls):
        instance = cls()
        instance.__spider_request_channel = await Stream.create(name='spider_request',confirm_ack=True) 
        instance.__spider_response_channel = await Stream.create(name='spider_response',confirm_ack=True) 
        instance.__middleware_request_channel = await Stream.create(name='middleware_request', confirm_ack=True)
        instance.__middleware_response_channel = await Stream.create(name='middleware_response', confirm_ack=True)
        instance.__request_channel = [instance.__spider_request_channel, instance.__middleware_request_channel]
        instance.__response_channel = [instance.__spider_response_channel, instance.__middleware_response_channel]
        instance.__close_waiter = Future()
        instance.__open_waiter = Future()
        return instance


    def bind(self, scraper = None, engine = None) -> None:
        """将 :py:obj:`Slot <Slot>` 与一个 :py:obj:`Scraper <araneid.scraper.Scraper>` 和  :py:obj:`Engine <araneid.core.engine.Engine>` 绑定，在通过它进行通信前必须先完成绑定

        Args:
            scraper (Scraper, optional): 绑定的 :py:obj:`Scraper <araneid.scraper.Scraper>` . Defaults to None.
            engine (Engine, optional): 绑定的 :py:obj:`Engine <araneid.core.engine.Engine>` . Defaults to None.

        Returns:
            None
        """
        if scraper:
            self.__scraper = scraper
        if engine:
            self.__engine = engine

    def unbind(self, scraper=True, engine=True) -> None:
        if engine:
           self.__engine = None
        if scraper:
           self.__engine = None

    
    def is_bound(self):
        return self.__scraper is not None and self.__engine is not None
    
    
    async def open(self):
        """[Slot.open()]
        Commence crawler processing bound to the slot and update status of the crawler to be RUNNNG.
        Raises:
            StarterNotFound: [description]

        Returns:
            [None]: [Return nothing except adding all requests to the cached request queue in slot]
        """
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        result = await self.__open_waiter
        return result

    async def set_open(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        if self.__open_waiter.done():
            return
        self.__open_waiter.set_result(True)
        self.__open = True

    async def wait_close(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        result = await self.__close_waiter
        return result
    
    async def wait_idle(self, flag:Idle =Idle.SLOT) -> None:
        """等待插槽空闲，同时支持等待插槽绑定的相关引擎是否空闲的相关状态, 一直阻塞直到空闲
        Args:
            flag (Idle, optional): 该标志表示等待空闲的类型，支持的标志类型有: Idle.SLOT (插槽空闲),  Idle.DOWNLOADERMANAGER (下载管理器空闲), 
            Idle.SLOTMANAGER(插槽管理器空闲), Idle.SIGNALMANAGER (信号管理器空闲), Idle.SCHEDULEMANAGER (调度管理器空闲)  默认为: Idle.SLOT (SLOT插槽空闲)

        Raises:
            SlotUnbound: [插槽未绑定异常]
        Returns:
            None: 
        """
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        result = True
        idle_events_waiter = []
        if Idle.SLOT in flag:
           idle_events_waiter.append(self.__wait_idle())
           flag &=  ~Idle.SLOT
        if flag:
           idle_events_waiter.append(self.__engine.wait_idle(flag))
        await asyncio.gather(*idle_events_waiter)
    
    async def __wait_idle(self) -> None:
        """阻塞直到空闲

        Returns:
            None: None
        """
        await self.__IDLE_EVENT.wait()
        self.__IDLE_EVENT.clear()
    
    
    async def get_request(self, delay_complete=False, spider=None):
        assert(type(delay_complete) is bool and (type(spider) is bool or spider is None))
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        if self.is_close():
           return
        qname = ''
        if spider is True:
            request_channel = self.__spider_request_channel
            qname ='spider'
        elif spider is False:
            request_channel = self.__middleware_request_channel
            qname = 'spidermiddleware'
        else:
            request_channel = self.__request_channel[random.randint(0, len(self.__request_channel)-1)]
        self.logger.debug('Get {qname} request from slot cache ({slot}) ...'.format(qname=qname, slot=id(self)))
        request = await request_channel.get()
        self.logger.debug('Get {qname} request from slot cache ({slot}): {request} '.format(qname=qname, slot=id(self), request=request))
        if delay_complete:
           return request
        self.complete_request(request)
        return request
      

    async def put_request(self, request):
        assert isinstance(request, Request)
        if not self.is_bound():
           raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        if self.is_close():
           request.set_state(request.States.slot)
           return
        qname = ''
        request_channel = None
        if request.context and request.context.spider and isinstance(request.context.spider, Spider):
            request_channel = self.__spider_request_channel
            qname = 'spider'
        else:
            request_channel = self.__middleware_request_channel
            qname = 'spidermiddleware'
        self.logger.debug('Put {qname} request to slot cache ({slot}): {request} '.format(qname=qname, slot=id(self), request=request))
        request.set_state(request.States.slot)
        await request_channel.write(request)
        if request.slot is None:
           request.bind(self)
    
    def read(self, flag= ReadFlag.spider_request):
        assert flag in ReadFlag
        if flag == ReadFlag.spider_request:
            return self.__spider_request_channel.read()
        elif flag == ReadFlag.nospider_request:
            return self.__middleware_request_channel.read()
        elif flag == ReadFlag.spider_response:
            return self.__spider_response_channel.read()
        else:
            return self.__middleware_response_channel.read()

    async def get_response(self, delay_complete=False, spider=None):
        assert(type(delay_complete) is bool and type(spider) is bool or spider is None) 
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        if self.is_close():
           return
        qname = ''
        if spider is True:
            qname = 'spider'
            response_channel = self.__spider_response_channel
        elif spider is False:
            qname = 'spidermiddleware'
            response_channel = self.__middleware_response_channel
        else:
            response_channel = self.__response_channel[random.randint(0, len(self.__response_channel)-1)]
        self.logger.debug('Get {qname} response from slot cache ({slot}) ...'.format(qname=qname, slot=id(self)))
        response = await response_channel.get()
        self.logger.debug('Get {qname} response from slot cache ({slot}): {response} '.format(qname=qname, slot=id(self), response=response))
        if delay_complete:
            return response
        self.complete_response(response)
        return response
    
    async def put_response(self, response):
        assert isinstance(response, Response)
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        if self.is_close():
           response.set_state(response.States.slot)
           return
        qname = ''
        if response.context and response.context.spider and isinstance(response.context.spider, Spider):
            qname = 'spider'
            response_channel = self.__spider_response_channel
        else:
            qname = 'spidermiddleware'
            response_channel = self.__middleware_response_channel
        response.set_state(response.States.slot)
        await response_channel.write(response)
        self.logger.debug('Put {qname}  response  to slot cache ({slot}): {response} '.format(qname=qname, slot=id(self), response=response))
        if response.slot is None:
           response.bind(self)
    
    def complete_request(self, request):
        assert isinstance(request, Request)
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        # check if request completed
        is_completed = request.is_completed()
        if is_completed:
           return False
        request.complete()
        request.set_state(request.States.complete)
        qname = ''
        if request.context and request.context.spider and isinstance(request.context.spider, Spider):
            qname = 'spider'
            self.__spider_request_channel.ack(request)
        else:
            qname = 'spidermiddleware'
            self.__middleware_request_channel.ack(request)
        self.logger.debug("Complete {qname} request {request} in slot {slot}.".format(qname=qname, request=request, slot=self.id()))
        if self.response_idle() and self.request_idle():
            self.__IDLE_EVENT.set()
        return True
    
    def complete_response(self, response):
        assert isinstance(response, Response)
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        # check if response completed
        is_completed = response.is_completed()
        if is_completed:
            return False
        response.complete()
        response.set_state(response.States.complete)
        qname = ''
        if response.context and response.context.spider and isinstance(response.context.spider, Spider):
            qname = 'spider'
            self.__spider_response_channel.ack(response)
        else:
            qname = 'spidermiddleware'
            self.__middleware_response_channel.ack(response)
        self.logger.debug("Complete {qname} response {response} in slot {slot}.".format(qname=qname, response=response, slot=self.id()))
        if self.response_idle() and self.request_idle():
            self.__IDLE_EVENT.set()
        return True
    
    def is_completed(self, requestOrResponse):
        assert isinstance(requestOrResponse, (Request, Response))
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        return requestOrResponse.is_completed()
    
    def is_open(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        return self.__open

    def id(self):
        return id(self)
    
    def idle(self, flag: Idle = Idle.SLOT): 
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        is_idle = True
        if Idle.SLOT in flag:
            is_idle &= self.response_idle() and self.request_idle()
            flag &= ~Idle.SLOT
        if flag:
           is_idle &= self.__engine.idle(flag)
        return is_idle
    
    def request_idle(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        return self.__spider_request_channel.idle()
    
    def response_idle(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        return self.__spider_response_channel.idle()
    
    async def join(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        await self.__request_channel.join()
        await self.__response_channel.join()
    
    def is_close(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        return self.__close

    async def close(self):
        if not self.is_bound():
            raise SlotUnbound('{slot} is not bound to any scraper and engine instance.'.format(slot=self))
        if self.is_close():
           return
        self.__open = False
        self.__close = True
        self.__close_waiter.set_result(self.id())
        self.__IDLE_EVENT.set()
        await self.__middleware_request_channel.close()
        await self.__middleware_response_channel.close()
        await self.__spider_request_channel.close()
        await self.__spider_response_channel.close()
        self.logger.debug(f'{self} closed.')


    def __str__(self):
        return  'Slot(engine={engine}, scraper={scraper}, spider_request_channel={spider_request_channel}, spider_response_channel={spider_response_channel}, middleware_request_channel={middleware_request_channel}, middleware_response_channel={middleware_response_channel})'.format(spider_request_channel=self.__spider_request_channel, spider_response_channel=self.__spider_response_channel, middleware_request_channel=self.__middleware_request_channel, middleware_response_channel=self.__middleware_response_channel, engine=self.__engine, scraper=self.__scraper)