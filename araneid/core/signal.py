#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.09.15
"""

"""
    module: araneid.core.event
    desc:   basic event support for araneid
"""
import logging
import asyncio
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import IntEnum, auto
from asyncio.locks import Event
from asyncio import Future
from contextlib import suppress
from itertools import filterfalse
from typing import Any, Coroutine, Dict, List, Optional, Set, Callable, Tuple, AnyStr
from araneid.state import State, States
from araneid.core.stream import Stream
from araneid.util._async import  ensure_asyncfunction
from araneid.util import cast_exception_to




class Signal(IntEnum):
    """信号接口
    """
    pass

class Signals(Signal):
    """支持的核心信号的枚举类, 包含所有Araneid支持的核心信号种类，可以被扩展，添加其他信号种类。

    Args:
        Signal ([type]): [description]
    """
    engine_started = 1 #: 引擎启动 
    engine_closed = auto() #: 引擎关闭
    spider_started = auto() #: 爬虫启动
    spider_closed = auto()  #: 爬虫关闭
    bytes_received = auto() #: 接受到请求响应字节，该信号可能被一个请求触发多次，信号处理器可以通过抛出`StopDownload`异常，来停止中断处理该请求响应。
    request_reached_slot = auto()  #: 请求到达插槽队列
    request_left_slot = auto()  #: 请求离开插槽队列
    request_scheduled = auto()  #: 请求被调度
    request_reached_downloader = auto() #: 请求到达下载器
    request_left_downloader = auto() #: 请求离开下载器
    request_dropped = auto() #: 请求被拒绝
    response_downloaded= auto() #: 请求响应被下载
    response_reached_slot = auto() #: 请求响应到达插槽处理队列
    response_left_slot  = auto()   #: 请求响应离开插槽处理队列
    response_scheduled = auto()    #: 请求响应被调度
    response_parsed = auto()       #: 请求响应被解析
    response_received = auto()     #: 请求响应开始收到响应
    response_ignored = auto()      #: 请求响应被忽略

def export_signals(signals: Signal, module):
    for signal in signals:
        setattr(module, signal.name, signal)
class Notify(ABC):
    """提醒接口

    Args:
        ABC ([type]): [description]

    Raises:
        NotImplementedError: [description]
    """

    @abstractmethod
    def notify(self, result: Any, source:object = None) -> None:
        """发出提醒

        Args:
            result (Any): 提醒的消息
            source (object, optional): 触发提醒的源. 默认为: None.

        Raises:
            NotImplementedError: 未实现
        """
        raise NotImplementedError


class SignalNotify(Notify):
    """信号通知

    Args:
        Notify ([type]): 通知接口

    Returns:
        [type]: [description]

    Yields:
        [type]: [description]
    """
    logger = None
    signal: Signal #: 被通知的信号
    __notify_event: Event #: 通知事件

    @property
    def name(self):
        """信号通知的名字, 由SignalNotify类名 + 信号名 + 信号通知对象的hash值组成

        Returns:
            [type]: [description]
        """
        return f'{self.__class__.__name__}_{self.signal.name}_{id(self)}'

    def __init__(self, signal: Signal) -> None:
        """信号通知的构造方法

        Args:
            signal (Signal): 被通知的信号
            default_notify_message (object, optional): 默认的通知消息. 未指定时为：None
        """
        self.logger = logging.getLogger(__name__)
        self.signal = signal
        self.__notify_event = Event()

    def notify(self, result: Any, source: object=None) -> None:
        if self.__notify_event.is_set():
           self.__notify_event.clear()
        self.__notify_event.set()
        self.__result = (source, result)
        self.logger.debug(f"{self.name} -> {source} notifying {result}")
    
    def is_notified(self):
        return self.__notify_event.is_set()
    
    def result(self)-> object:
        return self.__result

    async def wait(self):
        self.logger.debug(f"{self.name} -> wait")
        await self.__notify_event.wait()
        self.logger.debug(f"{self.name} -> notified")
        return self
    
    def __await__(self):
        return self.wait().__await__()
    
    def __hash__(self) -> int:
        return hash(self.signal)
    
    def __eq__(self, o: object) -> bool:
        return self.__hash__() == o.__hash__()
    

class SignalHandleNotify(Notify):

    logger = None
    __pair_key: object = None
    __name: AnyStr = None
    __signal: Signal

    @property
    def name(self):
        return  self.__name
    
    @property
    def signal(self):
        return self.__signal

    def __init__(self, signal: Signal, pair_key: object=None, name=None) -> None:
        self.logger = logging.getLogger(__name__)
        self.__notify_event: Event = Event()
        self.__pair_key= pair_key
        self.__signal = signal
        if  not name:
            self.__name = f'{self.__class__.__name__}_{id(self)}_{self.__pair_key}[{signal.name}]'
        else:
            self.__name = f'{self.__class__.__name__}_{name}[{signal.name}]'
    
    def notify(self,  result: object, source: object=None) -> None:
        if self.__notify_event.is_set():
            self.__notify_event.clear()
        self.result = result
        self.__notify_event.set()
        self.logger.debug(f"{self.name} -> notifying ( <- {self.result} )")
    
    def is_notified(self):
        return self.__notify_event.is_set()
    
    async def wait(self):
        self.logger.debug(f"{self.name} -> wait")
        await self.__notify_event.wait()
        self.logger.debug(f"{self.name} -> notified ( <- {self.result})")
        return self.result
    
    def __await__(self):
        return self.wait().__await__()
    
    def pair(self, pair_key: object) -> bool:
        return self.__pair_key == pair_key 

class SignalHandler(object):
    __name__: str
    __callable: Optional[Callable] = None
    __default_object: Any

    @property
    def name(self):
        return f'{self.__class__.__name__}_{self.__name__}_{id(self)}'

    def __init__(self, callable: Callable, default_object=None) -> None:
        self.__callable = callable
        self.__default_object = default_object
        callable_name = getattr(callable, '__name__', repr(callable))
        callable_module = getattr(callable, '__module__', '')
        callable_object =  getattr(callable, '__self__', '')
        if callable_object:
           callable_object = callable_object.__class__.__name__
        self.__name__ = '.'.join([callable_module, callable_object, callable_name])

    def __call__(self, signal: Signal, source: object, object: object, is_async: bool=True) -> Any:
        if is_async: 
          callable = ensure_asyncfunction(self.__callable)
        else:
          callable = self.__callable
        if not object:
           object = self.__default_object
        return callable(signal, source, object)
    
    def __str__(self) -> str:
        return f'{self.__name__}()'
        
class SignalHandleException(Exception):
    __signal_handle: SignalHandler
    __exception: Exception
    __source: object
    __object: object

    def __init__(self, signal_handle: SignalHandler, source: object, object:object, exception: Exception = None) -> None:
        self.__signal_handle = signal_handle
        self.__exception = exception
        self.__source = source
        self.__object = object

    
class TriggerStates(States):
    STARTED = auto()
    TRIGGERING = auto()
    TRIGGERED = auto()
    FINISHED = auto()

class Trigger(State):
    logger = None
    signal: Signal
    __payload: Optional[Tuple[object,object]] = None
    __waiters: Set[Future]
    __result: Any
    States = TriggerStates

    @property
    def name(self):
        name = '{cls}_{signal}_{id}'.format(cls=self.__class__.__name__, signal=self.signal.name, id=id(self))
        return  name
    
    @property
    def source(self):
        if not self.__payload:
           return 
        return self.__payload[0]
    
    @property
    def trigger_message(self):
        if not self.__payload:
           return
        return self.__payload[1]
    
    @property
    def result(self):
        return self.__result

    def __init__(self, signal: Signal) -> None:
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.signal = signal
        self.set_state(self.States.STARTED)
        self.__waiters = set()
        self.__result = None
    
    def trigger(self, source, message: object, wait=True):
        self.__payload = (source, message)
        ret = None
        if wait:
           ret = self.wait_result()
        return ret
    
    def add_waiter(self, waiter: Future):
        self.__waiters.add(waiter)

    async def wait(self):
        await self.wait_state(self.States.TRIGGERED)
        if self.__waiters:
           self.__result = await asyncio.gather(*self.__waiters)
        self.set_state(self.States.FINISHED)
        self.clear()
        self.logger.debug('{trigger} -> end.'.format(trigger=self.name))
    
    async def wait_result(self):
        await self.wait_state(self.States.FINISHED)
        return self.result
    
    def clear(self):
        self.__payload = None
    
    def __hash__(self) -> int:
        return hash((self.signal))

    
    def __eq__(self, o: Any) -> bool:
        return self.__hash__() == o.__hash__()
    
    def __str__(self) -> str:
        return f'{self.__class__.__name__}(signal={self.signal.name}, source={self.source}, message={self.trigger_message})'

    

    
class SignalManagerInvalidState(Exception):
    pass

class SignalManagerStates(States):
   STARTED = auto()
   RUNNING = auto()
   CLOSING = auto()
   CLOSED = auto()


class SignalManager(State):
    """信号处理器，负责信号的调度和分发

    Args:
        ABC ([type]): [description]

    Raises:
        SignalManagerInvalidState: 信号处理器状态异常

    Returns:
        [type]: [description]
    """
    logger = None
    __signal_handles: Dict[Signal, Set[SignalHandler]] #: 注册的信号处理函数
    __signal_notify_channel: Stream#: 收到的信号通知队列
    __signal_handle_notify_channel: Stream#: 收到的信号处理通知队列
    __trigger_channel: Stream#: 触发队列
    __running_tasks__: List[Coroutine] #: 当前运行的协程队列
    States: SignalManagerStates = SignalManagerStates #: 当前的信号处理起状态

    def __init__(self):
        """信号处理器的构造方法
        """
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.__signal_handles = {}
        self.__running_tasks__ = []
    
    @classmethod
    async def create(cls):
        instance = cls()
        instance.__signal_notify_channel= await Stream.create(confirm_ack=True)
        instance.__signal_handle_notify_channel= await Stream.create(confirm_ack=True)
        instance.__trigger_channel= await Stream.create(confirm_ack=True)
        return instance

    def idle(self) -> bool:
        """判断信号处理器的状态是否空闲

        Returns:
            bool: 当信号处理器空闲时，返回`True`， 否则返回`False`
        """
        is_idle = self.__signal_handle_notify_channel.idle() & self.__signal_notify_channel.idle() & self.__trigger_channel.idle()
        is_idle |= self.in_state(self.States.CLOSED)
        return is_idle
    
    async def wait_idle(self) -> None:
        """等待信号处理器，知道其处于空闲状态

        Returns:
            None
        """
        if not self.in_state(self.States.CLOSED) :
           await self.join()

    async def join(self, timeout=None):
        waiter = asyncio.gather(self.__signal_notify_channel.join(),self.__trigger_channel.join(), self.__signal_handle_notify_channel.join())
        if timeout:
           waiter = asyncio.wait_for(waiter, timeout=timeout)
        await waiter
    
    def get_signal_handles(self, signal:Signal)-> Set[SignalHandler]:
        handles = self.__signal_handles.get(signal, set())
        return handles
    
    def __add_signal_handle(self, signal: Signal, handle: Callable, default_object=None) -> SignalHandler:
        handles = self.__signal_handles.get(signal, set())
        signal_handler  = SignalHandler(handle, default_object=default_object) 
        handles.add(signal_handler)
        self.__signal_handles[signal] = handles
        return signal_handler
    
    async def __create_signal_handle_task(self, notify_msg, signal_notify, signal_handle) -> Coroutine:
        async def guard_signal_handle(signal_handle, source, object)-> Any:
            try:
               res =  await asyncio.ensure_future(signal_handle(signal=signal_notify.signal, source=source, object=object))
            except Exception as e:
                res = None
                e = cast_exception_to(e, SignalHandleException(signal_handle=signal_handle, source=source, object=object, exception=e))
                self.logger.exception(e)
            finally:
                res = await after(res)
            return res
        async def after(result):
            async with self.__signal_handle_notify_channel.read() as signal_handle_notify_reader:
                async for signal_handle_notify in signal_handle_notify_reader:
                    if signal_handle_notify.pair(signal_handle):
                        signal_handle_notify.notify(result)
                        self.__signal_handle_notify_channel.ack(signal_handle_notify)
                        self.logger.debug('{signal_handler} -> end ( <- {handle_result} ).'.format(signal_handler=signal_handle.name, handle_result=result))
                        break
                    self.__signal_handle_notify_channel.ack(signal_handle_notify)
                    await self.__signal_handle_notify_channel.write(signal_handle_notify)

        source, object = notify_msg
        return await guard_signal_handle(signal_handle, source, object)
    

    def __process_signal_notify(self, signal_notify:SignalNotify):

        self.logger.debug('Received signal notify: <{signal_notify}>.'.format(signal_notify=signal_notify.name))
        signal_handles: Set[SignalHandler] = self.get_signal_handles(signal_notify.signal)
        signal_handle_tasks: Set[Future] = set()
        notify_msg: object = signal_notify.result()
        self.logger.debug('Received signal notify message: {notify_message}'.format(notify_message=notify_msg))
        for signal_handle in signal_handles:
            self.logger.debug('{signal_handle} -> start'.format(signal_handle=signal_handle.name))
            signal_handle_task = self.__create_signal_handle_task(signal_notify=signal_notify, signal_handle=signal_handle, notify_msg=notify_msg)
            signal_handle_tasks.add(signal_handle_task)
        if not signal_handle_tasks:
            return None
        signal_handle_task_gather = asyncio.gather(*signal_handle_tasks) 
        return signal_handle_task_gather
    
    
    async def __start_signal_process(self):
        self.logger.debug('Signal process start.')
        handle_tasks: Set[Future] = set()
        async with self.__signal_notify_channel.read() as reader:
            async for signal_notify in reader:
                try:
                    signal_notify_handle_task = self.__process_signal_notify(signal_notify)
                    if signal_notify_handle_task is not None:
                       handle_tasks.add(asyncio.ensure_future(signal_notify_handle_task))
                    self.__signal_notify_channel.ack(signal_notify)
                    handle_tasks = set(filterfalse(lambda task: task.done(), handle_tasks))
                except asyncio.CancelledError:
                    break 
                except SignalHandleException as e:
                    raise e
        await self.__closing_warn_running_signal_handler()
        if handle_tasks:
           await asyncio.gather(*handle_tasks)

    async def __trigger_signal(self, trigger: Trigger):
        if trigger.in_state(Trigger.States.TRIGGERING):
           return
        handlers = self.get_signal_handles(trigger.signal)
        if not handlers:
           trigger.set_state(Trigger.States.TRIGGERED)
           trigger.set_state(Trigger.States.FINISHED)
           trigger.clear()
           self.logger.debug(f'{trigger} -> end (no handle registers).')
           return
        for handle in handlers:
            handle_notify_name = f'{handle.name}'
            handle_notify = SignalHandleNotify(signal=trigger.signal, pair_key=handle, name=handle_notify_name)
            await self.__signal_handle_notify_channel.write(handle_notify)
            trigger.add_waiter(handle_notify)
        signal_notify = SignalNotify(signal=trigger.signal)
        self.logger.debug('{trigger} -> notify {signal_notify}.'.format(trigger=trigger.name, signal_notify=signal_notify.name))
        signal_notify.notify(source=trigger.source, result=trigger.trigger_message)
        await self.__signal_notify_channel.write(signal_notify)
        trigger.set_state(Trigger.States.TRIGGERING)

    async def __start_trigger_process(self):
        self.logger.debug('Signal trigger process start.')
        wait_triggers: OrderedDict = OrderedDict()
        trigger_waiter_tasks = set()
        async with self.__trigger_channel.read() as reader:
            async for trigger in reader:
                await self.__trigger_signal(trigger)
                if trigger.in_state(Trigger.States.TRIGGERING):
                    trigger_waiter_tasks.add(asyncio.ensure_future(trigger.wait()))
                    trigger.set_state(Trigger.States.TRIGGERED)
                elif not (trigger.in_state(Trigger.States.FINISHED) and trigger.in_state(Trigger.States.TRIGGERED)):
                    wait_triggers[hash(trigger)] = trigger
                self.__trigger_channel.ack(trigger)
                trigger_waiter_tasks = set(filterfalse(lambda task: task.done(), trigger_waiter_tasks))
        while len(wait_triggers) > 0:
            _, trigger = wait_triggers.popitem(last=False)
            await self.__trigger_signal(trigger=trigger)
            if trigger.in_state(Trigger.States.TRIGGERING):
               trigger_waiter_tasks.add(asyncio.ensure_future(trigger.wait()))
               trigger.set_state(Trigger.States.TRIGGERED)
            else:
                trigger.set_state(Trigger.States.FINISHED)
        if trigger_waiter_tasks:
           await asyncio.gather(*trigger_waiter_tasks)


    async def start(self):
        self.set_state(self.States.STARTED)
        if self.in_state(self.States.RUNNING):
           raise SignalManagerInvalidState("Invalid Signalmanager state, Signalmanager is running!")
        self.logger.debug('SingalManger start.')
        self.__running_tasks__ = [asyncio.ensure_future(self.__start_signal_process()), asyncio.ensure_future(self.__start_trigger_process())]
        self.set_state(self.States.RUNNING)
        try:
            await asyncio.gather(*self.__running_tasks__)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.exception(e)
        finally:
            await self.close()
        self.logger.debug('Singalmanager closed.')

    async def trigger(self, signal: Signal, source: object, object:object, wait=True):
        ret: Optional[Future] = None
        if self.in_state(self.States.CLOSING) or self.in_state(self.States.CLOSED):
            self.logger.warning(f'Singalmanager being closed, {str(signal)} triggerred be ignored!')
            return
        else:
            trigger: Trigger = Trigger(signal)
            ret = trigger.trigger(source, object, wait=wait)
            await self.__trigger_channel.write(trigger)
            self.logger.debug('Triggered signal <{signal}> with: {trigger}'.format(signal=signal.name, trigger= trigger.name))
        return ret

    def register(self, signal, handle, default_object=None):
       if self.in_state(self.States.CLOSING):
          self.logger.warning(f'Singalmanager being closed, {str(signal)} registered with {handle} be ignored!')
          return
       signal_handler = self.__add_signal_handle(signal, handle, default_object=default_object)
       self.logger.debug('Registered signal <{signal}>  with {signal_handler}.'.format(signal=signal.name, signal_handler=signal_handler))
    
    async def __closing_warn_running_signal_handler(self):
        for _ in range(self.__signal_handle_notify_channel.size()):
            signal_handle_notify = await self.__signal_handle_notify_channel.get()
            if not signal_handle_notify:
               continue
            self.__signal_handle_notify_channel.ack(signal_handle_notify)
            for signal_handle in self.get_signal_handles(signal_handle_notify.signal):
                if not signal_handle_notify.pair(signal_handle):
                   continue
                self.logger.warning(f'{self.__class__.__name__} is being closed, but SignalHandler {signal_handle} still running!')
            await self.__signal_handle_notify_channel.write(signal_handle_notify)

    async def close(self):
        if self.in_state(self.States.CLOSING) or self.in_state(self.States.CLOSED):
            return
        self.set_state(self.States.CLOSING)
        await self.join()
        await self.__signal_handle_notify_channel.close()
        await self.__signal_notify_channel.close()
        await self.__trigger_channel.close()
        self.set_state(self.States.CLOSED)
        for task in self.__running_tasks__:
            task.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self.__running_tasks__)
        except Exception as e:
            self.logger.exception(e)
        self.__signal_handles.clear()
        self.__running_tasks__.clear()
        self.clear_states()
        self.logger.debug('Singalmanager being closed.')


def register(signal, handle, default_object=None):
    signalmanager.register(signal, handle, default_object)

def trigger(signal: Signal, source: object = None,  object: object = None, wait:bool=True):
    return signalmanager.trigger(signal, source, object, wait=wait)

async def close():
    await signalmanager.close()

async def start():
    await signalmanager.start()


def set_signalmanager(_signalmanager):
    global signalmanager
    signalmanager = _signalmanager

def get_signalmanager():
    return signalmanager


signalmanager = SignalManager()   # SignalManager
export_signals(Signals, sys.modules[__name__])
