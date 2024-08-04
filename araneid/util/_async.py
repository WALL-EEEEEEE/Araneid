import inspect
import collections
import functools
from asyncio import Event
import types
import asyncio
from asyncio import base_futures
from asyncio.coroutines import CoroWrapper
from inspect import  isasyncgenfunction as isasyncgenfunction_, iscoroutinefunction, isasyncgen
from araneid.util.iter import is_iterable

class sync_to_async_iterable:

    def __init__(self, gen_func):
        self.gen = gen_func
        self.__qualname__ = self.gen.name if getattr(self.gen, 'name', None) else getattr(self.gen, '__qualname__', None)

    async def __wrapper(self, *args, **kwargs):
        ret = self.gen(*args, **kwargs)
        if not inspect.isgenerator(ret):
            if inspect.isawaitable(ret):
                ret = await ret
            yield  ret
        else:
            for item in ret:
                if inspect.isawaitable(item):
                   ret= await item
                   yield ret
                else:
                    yield item
   
    def __call__(self, *args, **kwargs):
        return self.__wrapper(*args, **kwargs)


def sync_to_async(func):
    if inspect.iscoroutinefunction(func):
        # In Python 3.5 that's all we need to do for coroutines
        # defined with "async def".
        return func

    if inspect.isgeneratorfunction(func):
        coro = func
    else:
        @functools.wraps(func)
        def coro(*args, **kw):
            res = func(*args, **kw)
            if (base_futures.isfuture(res) or inspect.isgenerator(res) or
                    isinstance(res, CoroWrapper)):
                res = yield from res
            else:
                # If 'res' is an awaitable, run it.
                try:
                    await_meth = res.__await__
                except AttributeError:
                    pass
                else:
                    if isinstance(res, collections.abc.Awaitable):
                        res = yield from await_meth()
            return res

    coro = types.coroutine(coro)
    coro._is_coroutine = object()
    return coro

async def sync_to_asyncgen(object):
    if not is_iterable(object):
        yield object
        return
    for item in object:
        yield item

def isasyncgenfunction(object):
    if isinstance(object, sync_to_async_iterable):
        return True
    return  isasyncgenfunction_(object)

def ensure_asyncgenfunction(object):
    if isasyncgenfunction(object):
        return object
    return sync_to_async_iterable(object)

def ensure_asyncfunction(object):
    if iscoroutinefunction(object):
        return object
    return sync_to_async(object)

def ensure_asyncgen(object):
    if isasyncgen(object):
        return object
    return sync_to_asyncgen(object)


class itertools(object):

    @classmethod
    async def merge(cls, *gens):
        pending = gens
        pending_tasks = { asyncio.ensure_future(g.__anext__()): g for g in pending }
        while len(pending_tasks) > 0:
            done, _ = await asyncio.wait(pending_tasks.keys(), return_when="FIRST_COMPLETED")
            for d in done:
                try:
                    result = d.result()
                    yield result
                    dg = pending_tasks[d]
                    pending_tasks[asyncio.ensure_future(dg.__anext__())] = dg
                except StopAsyncIteration:
                    pass
                finally:
                    del pending_tasks[d]

class CountdownLatch:
    def __init__(self)-> None:
        self._event = Event()
        self._count = 0

    def increment(self) -> int:
        self._count += 1
        self._event.clear()
        return self._count

    def decrement(self) -> int:
        assert self._count > 0, "Count cannot go below zero"
        self._count -= 1
        if self._count == 0:
            self._event.set()
        return self._count

    async def wait(self) -> None:
        if self._count == 0:
            return
        await self._event.wait()

    @property
    def count(self) -> int:
        return self._count

class BoundedCountdownLatch:
    def __init__(self, count=1)-> None:
        self._event = Event()
        self._count = count

    def decrement(self) -> int:
        assert self._count > 0, "Count cannot go below zero"
        self._count -= 1
        if self._count == 0:
            self._event.set()
        return self._count

    async def wait(self) -> None:
        await self._event.wait()

    @property
    def count(self) -> int:
        return self._count

class CascadeCountdownLatch(CountdownLatch):

    def __init__(self) -> None:
        super().__init__()
        self.__cascades = {}

    def cascade(self, object):
        cascade_id = id(object)
        self.__cascades[cascade_id] = CountdownLatch()
    
    def increment(self) -> int:
        self._count += 1
        self._event.clear()
        return self._count

    def decrement(self) -> int:
        assert self._count > 0, "Count cannot go below zero"
        self._count -= 1
        if self._count == 0:
            self._event.set()
        return self._count
    

