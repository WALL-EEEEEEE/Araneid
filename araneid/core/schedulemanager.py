import asyncio
from asyncio.tasks import Task
import logging
from contextlib import suppress
from random import randint
from asyncio.locks import Event
from typing import Coroutine, List
from araneid.util._async import itertools
from araneid.core.stream import Stream
from . import plugin as plugins
from .exception import SchedulerRuntimeException, PluginError, NotConfigured, SchedulerError
from .scheduler import Scheduler

class ScheduleManager(object):
    logger = None
    __IDLE_EVENT__: Event 
    __channel__: Stream
    __running_tasks__: List[Task]
    __channel_receivers__: List[Coroutine]
    __active_schedulers__:List[Scheduler]
    __closed__: bool

    def __init__(self, settings = None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("SchedulerManager init.")
        self.__running_tasks__ = []
        self.__channel_receivers__ = []
        self.__active_schedulers__ = []
        self.__closed__ = False
 
    
    @classmethod
    async def create(cls, settings = None):
        instance = cls.from_settings(settings)
        instance.__IDLE_EVENT__: Event = Event()
        instance.__channel__ : Stream = await Stream.create()
        await instance.__init_schedulers(settings=settings)
        if not instance.__active_schedulers__:
            raise SchedulerError("No schedulers found")
        return instance
    
    async def __init_schedulers(self, settings):
        schedulers = await self.__load_plugin(settings=settings)
        for name, scheduler in schedulers.items():
            self.__active_schedulers__.append(scheduler)

    async def __load_plugin(cls, settings):
        scheduler_plugins = plugins.load(plugins.PluginType.SCHEDULER)
        schedulers = dict()
        for plugin in scheduler_plugins:
            name = plugin.name
            scheduler = plugin.load()
            try:
                if hasattr(scheduler, 'from_settings'):
                    schedulers[name] = await scheduler.create(settings=settings)
                else:
                    schedulers[name] = await scheduler.create()
            except NotConfigured as e:
                cls.logger.warning("Scheduler {name} is not configured, skipped load.")
                continue
            except Exception as e:
                raise PluginError(f"Error occurred in while loading scheduler {name}!") from e
            cls.logger.debug(f'Loaded scheduler: {name}.')
        return schedulers

    def idle(self) -> bool:
        is_idle = True 
        for sched in self.__active_schedulers__:
            is_idle &= sched.idle()
        return is_idle
    
    async def wait_idle(self)-> None:
        await self.__IDLE_EVENT__.wait()
        self.__IDLE_EVENT__.clear()
    
    def __select__(self):
        sched_len = len(self.__active_schedulers__)
        sched_index = randint(0, sched_len-1)
        return self.__active_schedulers__[sched_index]
    
    async def add_request(self, request):
        scheduler = self.__select__()
        await scheduler.add_request(request)

    async def add_response(self, response):
        scheduler = self.__select__()
        await scheduler.add_response(response)

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)
    
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
   
    async def __start_schedulers__(self):
        try:
            __schedules__ = [ scheduler.run(self.__channel__) for scheduler in self.__active_schedulers__] 
            await asyncio.gather(*__schedules__)
        except Exception as e:
            raise SchedulerRuntimeException from e
        
    async def close(self):
        if self.__closed__:
           return
        self.__closed__ = True
        self.__IDLE_EVENT__.set()
        await self.__channel__.close()
        wait_scheduler_close = set()
        for scheduler in self.__active_schedulers__:
            wait_scheduler_close.add(scheduler.close())
        try:
            with suppress(asyncio.CancelledError):
                 await asyncio.gather(*wait_scheduler_close)
        except Exception as e:
            self.logger.exception(e)
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self.__running_tasks__)
        except Exception as e:
            self.logger.exception(e)
        self.logger.debug('ScheduleManager being closed.')

    async def start(self):
        self.logger.debug('ScheduleManager start.')
        try:
            self.__running_tasks__.append(asyncio.create_task(self.__start_schedulers__()))
            self.__running_tasks__.append(asyncio.create_task(self.__process_channel()))
            await asyncio.gather(*self.__running_tasks__)
        except Exception as e:
            self.logger.exception(e)
        finally:
            await self.close()
            self.logger.debug('ScheduleManager closed')