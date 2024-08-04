import logging
import inspect
from asyncio import Queue
from enum import IntEnum 
from araneid.util._async import ensure_asyncfunction, CountdownLatch

class Stream:
    logger = None
  
    class Oprator(IntEnum):
        MAP = 0

    class STREAM_END:
        pass

    def __init__(self, name=None, confirm_ack=False) -> None:
        self.logger = logging.getLogger(__name__)
        if name:
           self._name = name
        else:
           self._name = id(self)
        self._closed = False
        self._confirm_ack = confirm_ack
        self._operators = []
        self._exception = None
    
    @classmethod
    async def create(cls, name=None, confirm_ack=False):
        instance = cls(name=name, confirm_ack=confirm_ack)
        instance._buffer = Queue()
        instance._readers = CountdownLatch()
        return instance

    
    async def set_exception(self, exception):
        assert isinstance(exception, Exception)
        self._exception = exception
        await self.join()
        await self.close()

    def is_closed(self):
        return self._closed
    
    def read(self):
        return self
    
    def size(self):
        return self._buffer.qsize()

    async def __aenter__(self):
        #reader_name = '::'.join([str(inspect.stack()[1][1]).split('/')[-1],str(inspect.stack()[1][3])])
        self._readers.increment()
        #self.logger.debug(f'Reader {reader_name} of Stream {self._name} start')
        return self
    
    def __aiter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        #reader_name = '::'.join([str(inspect.stack()[1][1]).split('/')[-1],str(inspect.stack()[1][3])])
        self._readers.decrement()
        #self.logger.debug(f'Reader {reader_name} of Stream {self._name} end')

    async def __anext__(self):
        #reader_name = '::'.join([str(inspect.stack()[1][1]).split('/')[-1],str(inspect.stack()[1][3])])
        if not (self._closed and self._buffer.empty()):
            data = await self._buffer.get()
            #self.logger.debug(f'Reader {reader_name} of Stream {self._name} read {data}')
            self._auto_ack(data)
            if isinstance(data, self.STREAM_END):
               #self.logger.debug(f'Reader {reader_name} of Stream {self._name} read STREAM_END')
               await self._buffer.put(data)
               if self._exception:
                  raise self._exception
               else:
                  raise StopAsyncIteration
            data = await self.__process_operators(data)
            return data
        if self._exception:
           raise self._exception
        raise StopAsyncIteration

    async def get(self):
        if self._closed:
           return
        async with self.read() as reader:
            async for item in reader:
                return item
    
    async def write(self, data):
        if self._closed:
           return
        await self._buffer.put(data)
    
    def map(self, map_func):
        map_func = ensure_asyncfunction(map_func)
        self._operators.append((Stream.Oprator.MAP, map_func))
    
    def _auto_ack(self, stream_data):
        if not isinstance(stream_data, self.STREAM_END) and self._confirm_ack:
           return
        if self._buffer._unfinished_tasks < 1:
            return
        self._buffer.task_done()
    
    def ack(self, stream_data):
        if self._closed:
            return
        if not self._confirm_ack:
            return
        if self._buffer._unfinished_tasks < 1:
            return
        self._buffer.task_done()
    
    def idle(self):
        if self._closed:
            return True
        return self._buffer._finished.is_set()
    
    async def join(self):
        if self._closed:
           return
        await self._buffer.join()
    
    async def __process_operators(self, data):
        for op_type, operator in self._operators:
            if op_type == Stream.Oprator.MAP:
               data = await operator(data)
        return data
    
    def _clear_buff(self):
        while not self._buffer.empty():
            item = self._buffer.get_nowait()
            if isinstance(item, self.STREAM_END):
               self._buffer.put_nowait(item)
               self._buffer.task_done()
               break
            self._buffer.task_done()
        if self._buffer._unfinished_tasks < 1:
           return
        # mark task done except the STREAM_END
        for _ in range(self._buffer._unfinished_tasks-self._buffer.qsize()):
            self._buffer.task_done()
    
    async def close(self):
        if self._closed:
           return
        await self._buffer.put(self.STREAM_END())
        self._closed = True
        #self.logger.debug(f'Stream {self._name} closed, wait for readers process.')
        await self._readers.wait()
        self._clear_buff()
        self._operators.clear()
        #self.logger.debug(f'Stream {self._name} closed.')

class DelayStream(Stream):
    pass