import asyncio
from asyncio.tasks import Task
from contextlib import closing, suppress
import logging
from asyncio.locks import Event
from multiprocessing.connection import wait
from typing import Coroutine, Dict, Iterable, List
from araneid.core.stream import Stream
from araneid.core.request import Request
from araneid.core.response import Response
from .exception import SlotError, SlotNotFound
from .slot import Slot


class SlotManager(object):
    __slots__ = ['logger', '__slots', '__channel__','__channel_receivers__', '__processing_slot_channel__','__closing_slot_channel__','__openning_slot_channel__', '__running_tasks__', '__IDLE_EVENT__', '__closed__']
    __slots :Dict[int, Slot]
    __IDLE_EVENT__: Event
    __channel__: Stream
    __channel_receivers__: List[Coroutine]
    __processing_slot_channel__: Stream
    __closing_slot_channel__: Stream
    __openning_slot_channel__: Stream
    __running_tasks__:List[Task]
    __closed__: bool

    def __init__(self, settings = None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("SlotManager init.")
        self.__slots = {}
        self.__running_tasks__ = []
        self.__channel_receivers__ = []
        self.__closed__ = False
    
    @classmethod
    async def create(cls, settings = None):
        instance = cls.from_settings(settings=settings)
        instance.__IDLE_EVENT__ = Event()
        instance.__channel__ = await Stream.create()
        instance.__processing_slot_channel__ = await Stream.create()
        instance.__closing_slot_channel__ = await Stream.create()
        instance.__openning_slot_channel__ = await Stream.create()
        return instance

    @classmethod
    def from_settings(cls, settings):
        """以配置文件实例化SlotManager

        Args:
            settings ([type]): 配置

        Returns:
            [SlotManager]: SlotManager实例
        """
        return cls(settings)

    async def __open_slots(self):
        """
            @method: __open_slots(self)
            @desc:  open and initiate request slots from registered crawlers
            @return: void
        """
        openning_slots: List[Task] =  []
        async with self.__openning_slot_channel__.read() as reader:
            async for slot in reader:
                openning_slot: Task = asyncio.ensure_future(slot.set_open())
                openning_slots.append(openning_slot)
                openning_slots = [ openning_slot for openning_slot in openning_slots if not openning_slot.done()]
        try:
            await asyncio.gather(*openning_slot)
        except Exception as e:
            self.logger.exception(e)


      

    async def __process_slots(self):
        processing_slots: List[Task] = []
        async with self.__processing_slot_channel__.read() as reader:
            async for slot in reader:
                processing_slot: Task = asyncio.ensure_future(self.__get_request(slot.id(), delay_complete=True))
                processing_slots.append(processing_slot)
                processing_slots = [ processing_slot for processing_slot in processing_slots if not processing_slot.done()]
        try:
            await asyncio.gather(*processing_slots)
        except Exception as e:
            self.logger.exception(e)
        finally:
            if self.idle():
                self.__IDLE_EVENT__.set()


    async def __close_slots(self):
        closing_slots: List[Task] = []

        async def close(slot: Slot):
            await slot.wait_close()
            await self.close_slot(slot.id())
             
        async with self.__closing_slot_channel__.read() as reader:
            async for slot in reader:
                closing_slot: Task = asyncio.ensure_future(close(slot))
                closing_slots.append(closing_slot)
                closing_slots = [ closing_slot for closing_slot in closing_slots if not closing_slot.done()]
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*closing_slots)
               

    async def add_slot(self, slot: Slot):
        self.__slots[slot.id()] = slot
        await self.__openning_slot_channel__.write(slot)
        await self.__processing_slot_channel__.write(slot)
        await self.__closing_slot_channel__.write(slot)
        self.logger.debug('Add slot: {slot}'.format(slot=slot.id()))
        self.logger.debug('Active slots: {slots}'.format(slots=len(self.__slots)))
    
    def del_slot(self, slot_id):
        pass

    async def close_slot(self, slot_id):
        if slot_id not in self.__slots:
            return
        slot = self.__slots[slot_id]
        await slot.close()
        del self.__slots[slot_id]
        self.logger.debug('Slot {slot} closed.'.format(slot=slot_id))
    
    def get_slot(self, slot_id):
        slot = self.__slots.get(slot_id, None)
        if not slot:
           self.logger.debug('Slot {slot_id} not exists.'.format(slot_id=slot_id))
           return None
        return slot

 
    
    async def put_response(self, response, slot_id=None):
        if slot_id is None:
           slot_id =  response.slot if response.slot is None else id(response.slot)
        if not slot_id:
           raise SlotError('Response {response} not bind to any slot.'.format(response=response)) 
        slot = self.get_slot(slot_id)
        if not slot:
           return
        await slot.put_response(response)

    async def put_request(self, request, slot_id=None):
        if slot_id is None:
           slot_id =  request.slot if request.slot is None else id(request.slot)
        if not slot_id:
           raise SlotError('Request {request} not bind to any slot.'.format(request=request)) 
        slot = self.get_slot(slot_id)
        if not slot:
           return
        await slot.put_request(request)
    
    async def __get_request(self, slot_id, delay_complete=False):
        slot = self.get_slot(slot_id)
        if not slot:
           return
        pending = []
        waiters = []
        get_spider_request = None
        get_nospider_request = None
        while not slot.is_close():
            if not waiters:
               get_spider_request = asyncio.ensure_future(slot.get_request(delay_complete=delay_complete, spider=True))
               get_nospider_request = asyncio.ensure_future(slot.get_request(delay_complete=delay_complete, spider=False))
               waiters = [get_spider_request, get_nospider_request]
            done, pending= await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                request = task.result()
                await self.__channel__.write(request)
                if task is get_nospider_request:
                   get_nospider_request = asyncio.ensure_future(slot.get_request(delay_complete=delay_complete, spider=False))
                   waiters = list(pending) + [get_nospider_request]
                elif task is get_spider_request:
                   get_spider_request = asyncio.ensure_future(slot.get_request(delay_complete=delay_complete, spider=True))
                   waiters = list(pending) + [get_spider_request]


    async def get_response(self, slot_id, delay_complete=False):
        slot = self.get_slot(slot_id)
        if not slot:
            return
        done, __= await asyncio.wait([slot.get_response(delay_complete=delay_complete, spider=True), slot.get_response(delay_complete=delay_complete, spider=False)], return_when=asyncio.FIRST_COMPLETED)
        return [ t.result() for t in done]
    
    async def get_request(self, slot_id, delay_complete=False):
        slot = self.get_slot(slot_id)
        if not slot:
            return
        done, __= await asyncio.wait([slot.get_request(delay_complete=delay_complete, spider=True), slot.get_request(delay_complete=delay_complete, spider=False)], return_when=asyncio.FIRST_COMPLETED)
        return [ t.result() for t in done]
 
    def idle(self)-> bool:
        is_idle = True
        for slot in self.__slots.values():
            slot: Slot = slot
            is_idle &= slot.idle()
        return is_idle
    
    async def wait_idle(self) -> None:
        await self.__IDLE_EVENT__.wait()
        self.__IDLE_EVENT__.clear()
        

    async def join(self):
        slots_close_waiters = [ asyncio.ensure_future(slot.wait_close()) for slot in  self.__slots.values()]
        while slots_close_waiters:
            slots_closed,slots_alive  =  await asyncio.wait(slots_close_waiters, return_when=asyncio.FIRST_COMPLETED)
            for slot in slots_closed:
                closed_slot_id = slot.result()
                await self.close_slot(closed_slot_id)
            slots_close_waiters = [ slot.wait_close() for slot in  self.__slots.values()]
    
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

    async def start(self):
        self.logger.debug('SlotManager start')
        self.logger.debug('Active slots: {slot_num}'.format(slot_num=len(self.__slots)))
        try:
            self.__running_tasks__.append(asyncio.create_task(self.__open_slots()))
            self.__running_tasks__.append(asyncio.create_task(self.__close_slots()))
            self.__running_tasks__.append(asyncio.create_task(self.__process_slots()))
            self.__running_tasks__.append(asyncio.create_task(self.__process_channel()))
            await asyncio.gather(*self.__running_tasks__,return_exceptions=True)
        except Exception as e:
            self.logger.exception(e)
        finally:
            await self.close()
            self.logger.debug('SlotManager closed.')

    async def close(self):
        if self.__closed__:
           return
        self.__closed__ = True
        self.__IDLE_EVENT__.set()
        for slot_id, slot in list(self.__slots.items()):
            await slot.close()
        await self.__openning_slot_channel__.close()
        await self.__processing_slot_channel__.close()
        await self.__closing_slot_channel__.close()
        await self.__channel__.close()
        self.__slots.clear()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*self.__running_tasks__)
        self.logger.debug('SlotManager being closed.')