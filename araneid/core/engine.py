import logging
import asyncio
from asyncio.tasks import FIRST_COMPLETED
from contextlib import suppress
from inspect import iscoroutine
from typing import List, AnyStr
from araneid.setting import settings as settings_loader  
from araneid.core.signal import SignalManager, SignalManagerInvalidState
from .request import Request
from .response import Response
from .downloadmanager  import DownloadManager
from .slot import Slot
from .slotmanager import SlotManager
from .schedulemanager import ScheduleManager
from .flags import Idle
from . import plugin
from . import signal 

class Engine(object):
    """引擎类，负责调度请求以及与其他核心组件协调工作。 

    引擎管理的组件：

    * :py:obj:`~araneid.core.schedulemanager.ScheduleManager` - 请求调度器: 负责中间件和爬虫生成的请求调度
    * :py:obj:`~araneid.core.signal.SignalManager` - 信号处理器: 负责处理爬虫生命周期的信号处理
    * :py:obj:`~araneid.core.slotmanager.SlotManager` - Slot管理器: 负责统一管理Slot
    * :py:obj:`~araneid.core.downloadmanager.DownloadManager` - 下载器管理器: 负责加载和管理下载器,以及统一负责请求的下载

    引擎主要负责这些组件的协调工作, 以及组件和组件之前的通信.

    """
    __slots__ = ['settings', 'logger', '__downloadmanager__', '__signalmanager__', '__slotmanager__', '__schedulemanager__', '__extensionmanager__', '__stop', '__running_tasks', '__idle_status', '__idle_events']

    def __init__(self, settings: dict=None):
        # make sure logger of engine init after CliRunner
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Engine init.")
        self.logger.debug(f"Settings: {settings}")
        self.settings = settings
        self.__downloadmanager__: DownloadManager = None
        self.__signalmanager__ :SignalManager = None
        self.__slotmanager__ :SlotManager = None
        self.__schedulemanager__ :ScheduleManager = None
        self.__stop = False
        self.__running_tasks = []
    
    @classmethod
    async def create(cls, settings=None):
        settings = settings if settings is not None else settings_loader
        instance = cls.from_settings(settings=settings)
        """初始化请求调度器管理器
        """
        instance.__schedulemanager__ = await ScheduleManager.create(instance.settings)
        """初始化下载器管理器
        """
        instance.__downloadmanager__ = await DownloadManager.create(instance.settings)
        """初始化信号管理器
        """
        instance.logger.debug("SignalManager init.")
        instance.__signalmanager__  = signal.get_signalmanager()
        """初始化Slot管理器
        """
        instance.__slotmanager__ = await SlotManager.create(instance.settings)
        instance.__idle_status = { Idle.DOWNLOADERMANAGER: instance.__downloadmanager__.idle,
                       Idle.SCHEDULEMANAGER: instance.__schedulemanager__.idle,
                       Idle.SIGNALMANAGER: instance.__signalmanager__.idle,
                       Idle.SLOTMANAGER: instance.__slotmanager__.idle
                      }
        instance.__idle_events = { Idle.DOWNLOADERMANAGER: instance.__downloadmanager__.wait_idle,
                       Idle.SCHEDULEMANAGER: instance.__schedulemanager__.wait_idle,
                       Idle.SIGNALMANAGER: instance.__signalmanager__.wait_idle,
                       Idle.SLOTMANAGER: instance.__slotmanager__.wait_idle
                      }
        return instance
    
    @classmethod
    def from_settings(cls, settings: dict):
        """从配置实例化生成Engine对象

        Args:
            settings (dict): 配置

        Returns:
            Engine: 引擎对象
        """
        engine = cls(settings=settings)
        return engine
    

    def idle(self, flag: Idle=Idle.DOWNLOADERMANAGER | Idle.SIGNALMANAGER | Idle.SLOTMANAGER| Idle.SCHEDULEMANAGER) -> bool:
        """检测引擎的是否空闲

        Args:
            flag (Idle, optional): 指定需要检测的组件. 默认为：Idle.DOWNLOADERMANAGER|Idle.SIGNALMANAGER|Idle.SLOTMANAGER|Idle.SCHEDULEMANAGER.
            即当下载管理器，信号处理器, 调度器，插槽队列管理器都空闲的时候，才返回true，否则返回false

        Returns:
            bool: 空闲返回true，否则返回false
        """
        assert Idle.SLOT not in flag, "Idle.Slot flag is not support!"
        is_idle = True

        for idle_flag in Idle:
            if idle_flag not in flag:
                continue
            is_idle &= self.__idle_status[idle_flag]()
        return is_idle
    
    async def wait_idle(self, flag: Idle=Idle.DOWNLOADERMANAGER | Idle.SIGNALMANAGER | Idle.SLOTMANAGER| Idle.SCHEDULEMANAGER)-> None:
        """等待直到引擎的空闲
        Args:
            flag (Idle, optional): 指定需要检测的组件. 默认为：Idle.DOWNLOADERMANAGER|Idle.SIGNALMANAGER|Idle.SLOTMANAGER|Idle.SCHEDULEMANAGER.
            即当下载管理器，信号处理器, 调度器，插槽队列管理器都空闲的时候，才返回，否则一直阻塞

        Returns:
            None
        """
        assert Idle.SLOT not in flag, "Idle.Slot flag is not support!"
        idle_event_waiters = []
        for idle_flag in Idle:
            if idle_flag not in flag:
                continue
            idle_event_waiters.append(self.__idle_events[idle_flag]())
        await asyncio.gather(*idle_event_waiters)


    async def add_slot(self, slot: Slot):
        """添加引擎管理的 :py:obj:`~araneid.core.slot.Slot`, 引擎会托管 :py:obj:`~araneid.core.slotmanager.SlotManager` 对Slot进行管理

        Args:
            slot (Slot): 增加管理的Slot
        """
        slot.bind(engine=self)
        await self.__slotmanager__.add_slot(slot)
    
    async def __process__download(self, request : Request, downloader: List[AnyStr]):
        """对请求进行下载, 引擎会托管 :py:obj:`~araneid.core.downloadmanager.DownloadManager` 对请求进行下载

        Args:
            request (Request): 需要下载的请求
            downloader (List[AnyStr]): 请求指定分配的下载器, 下载管理器会根据下载器列表为请求分配对应的下载器进行下载
        """
        if isinstance(request, Request):
           await self.__downloadmanager__.process_download(request, downloader)
    
    async def __start_schedulemanager(self):
        """启动调度器管理器
        """
        async def __receiver(reqOrResp):
            if isinstance(reqOrResp, Request):
                await self.__signalmanager__.trigger(signal.request_scheduled, self, reqOrResp, wait=False)
                reqOrResp.set_state(reqOrResp.States.schedule)
                await self.__process__download(reqOrResp, reqOrResp.downloader)
            elif isinstance(reqOrResp, Response):
                reqOrResp.set_state(reqOrResp.States.schedule)
                await self.__slotmanager__.put_response(reqOrResp)
        self.__schedulemanager__.add_channel_receiver(__receiver)
        await asyncio.gather(self.__schedulemanager__.start())

    
    async def __start_slotmanager(self):
        """启动slot管理器
        """
        async def __receiver(reqOrResp):
            if isinstance(reqOrResp, (Response, Request))  and reqOrResp.in_state(reqOrResp.States.schedule):
               self.logger.warning(f'{reqOrResp} is rescheduled!')
               return
            if isinstance(reqOrResp, Response) and not reqOrResp.in_state(reqOrResp.States.schedule):
                await self.__schedulemanager__.add_response(reqOrResp)
            elif isinstance(reqOrResp, Request) and not reqOrResp.in_state(reqOrResp.States.schedule):
                await self.__signalmanager__.trigger(signal.request_left_slot, self, reqOrResp, wait=False)
                await self.__schedulemanager__.add_request(reqOrResp)
        self.__slotmanager__.add_channel_receiver(__receiver)
        await asyncio.gather(self.__slotmanager__.start())
   
    async def __start_signalmanager(self):
        """启动信号管理器
        """
        await self.__signalmanager__.start()
    
    async def __start_downloadermanager(self):
        """启动下载管理器
        """
        async def __receiver(reqOrRsp):
            if isinstance(reqOrRsp, Response) and not reqOrRsp.in_state(reqOrRsp.States.schedule):
               await self.__schedulemanager__.add_response(reqOrRsp)
            elif isinstance(reqOrRsp, Request) and not reqOrRsp.in_state(reqOrRsp.States.schedule):
               await self.__schedulemanager__.add_request(reqOrRsp)
        self.__downloadmanager__.add_channel_receiver(__receiver)
        await asyncio.gather(self.__downloadmanager__.start())

    async def __start(self):
        """触发 :py:obj:`araneid.core.signal.Signal.EngineStart`事件, 启动组件 :py:obj:`~araneid.core.schedulemanager.ScheduleManager` , :py:obj:`~araneid.core.signal.SignalManager` 
        , :py:obj:`~araneid.core.slotmanager.SlotManager` , :py:obj:`~araneid.core.downloadmanager.DownloadManager` 的处理
        """
        await self.__signalmanager__.trigger(signal.engine_started, source=self, object=None, wait=False)
        running_tasks = [asyncio.create_task(self.__start_slotmanager()),asyncio.create_task(self.__start_schedulemanager()), asyncio.create_task(self.__start_downloadermanager()), asyncio.create_task(self.__start_signalmanager())]
        self.__running_tasks.extend(running_tasks)
        await asyncio.gather(*running_tasks)

    async def wait_closable(self):
        """等待引擎可关闭, 即 :py:obj:`~araneid.core.slotmanager.SlotManager` 的slot都关闭了的时候
        """
        await self.__slotmanager__.join()

    async def start(self):
        """启动引擎, 并启动相关组件 :py:obj:`~araneid.core.schedulemanager.ScheduleManager` , :py:obj:`~araneid.core.signal.SignalManager` 
        , :py:obj:`~araneid.core.slotmanager.SlotManager` , :py:obj:`~araneid.core.downloadmanager.DownloadManager` 的处理, 并在 :py:obj:`~araneid.core.slotmanager.SlotManager` 中的请求都处理完成,引擎会自动退出

        Raises:
            SignalManagerInvalidState : 信号处理器状态异常
        """
        self.logger.debug("Engine start.")
        self.__running_tasks.append(asyncio.create_task(self.__start()))
        try:
            done, _ = await  asyncio.wait([ *[ asyncio.shield(task) for task in self.__running_tasks] , self.wait_closable()], return_when=FIRST_COMPLETED)
            [task.result() for task in done] # 检索wait的协程错误
        except asyncio.CancelledError as e:
            self.logger.warning(f'Engine closing by being canceled!')
        except Exception as e:
            self.logger.exception(e)
        finally:
            await self.close()
    
    async def start2(self):
        """永久运行引擎, 该方法除非出现异常,否则不会自动退出引擎

        Raises:
            Exception: 未处理异常
        """
        self.logger.debug("Engine start")
        self.__running_tasks.append(asyncio.create_task(self.__start()))
        try:
            await asyncio.gather(*self.__running_tasks)
        except asyncio.CancelledError as e:
            self.logger.warning(f'Engine closing by being canceled!')
        except Exception as e:
            self.logger.exception(e)
        finally:
            await self.close()
    
    async def close(self):
        """关闭引擎, 该方法会触发 :py:obj:`araneid.core.signal.Signal.EngineClose` 事件, 并负责关闭相关组件 :py:obj:`~araneid.core.schedulemanager.ScheduleManager` , :py:obj:`~araneid.core.signal.SignalManager` 
        , :py:obj:`~araneid.core.slotmanager.SlotManager` , :py:obj:`~araneid.core.downloadmanager.DownloadManager` .
        """
        if self.__stop:
            return
        self.__stop = True
        await self.__signalmanager__.trigger(signal.engine_closed, source=self, object=None, wait=False)
        await self.__slotmanager__.close()
        self.logger.debug('engine_close_1')
        await self.__schedulemanager__.close()
        self.logger.debug('engine_close_2')
        await self.__downloadmanager__.close()
        self.logger.debug('engine_close_3')
        await self.__signalmanager__.close()
        self.logger.debug('engine_close_4')


        plugin.clear()
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self.__running_tasks)
        except Exception as e:
            self.logger.exception(e)
        self.logger.debug("Engine closed.")
