#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.06.18
"""
from araneid.item import JsonPipeline, Item
from typing import Generator
from collections.abc import Iterable, Generator
from asyncio.coroutines import CoroWrapper
from asyncio import base_futures
import collections
import types
import inspect
import itertools


def is_exhausted(generator: Generator):
    _exhausted = object()
    __next = next(generator, _exhausted)
    if __next is _exhausted:
        return True, None
    return False, itertools.chain([__next], generator)


def flatten(it):
    for x in it:
        if isinstance(x, Iterable) and not isinstance(x, str) and not isinstance(x,JsonPipeline) and  not isinstance(x, Item):
            yield from flatten(x)
        else:
            yield x


def to_gen(gen):
    if isinstance(gen, Generator):
        yield from gen
    else:
        yield gen

def coroutine_sync_to_async(func):
    """Decorator to mark coroutines.

    If the coroutine is not yielded from before it is destroyed,
    an error message is logged.
    """
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

def asyncgenerator_sync_to_async(func):
    """Decorator to mark async generator.

    If the coroutine is not yielded from before it is destroyed,
    an error message is logged.
    """
    if inspect.isasyncgenfunction(func):
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

