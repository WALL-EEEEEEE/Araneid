import inspect
from typing import Any, AnyStr, Callable, List, Optional
from aiostream import pipe, stream
from araneid.util._async import ensure_asyncgenfunction
from araneid.data.item import Item
from araneid.core.exception import StaterUnbound, InvalidStarter


class Starter(object):
    name: AnyStr
    start_func: Callable
    context: Optional[Any]
    items: Item

    def __init__(self, name=None, item=None):
        if not ((inspect.isclass(item) and issubclass(item, Item)) or item is None):
           raise InvalidStarter("item of starter {starter} must be a subclass of Item".format(starter=name))
        self.name = name
        self.start_func = None
        self.context = None
        if not item: 
           item = Item(name=name)
        self.items = item

    def bind(self, start_func= None, spider= None):
        assert isinstance(start_func, (Callable, type(None))), "parse_func must be a callable"
        if start_func is not None:
           self.start_func = start_func
        if spider is not None:
           self.context = spider

    def __check__(self):
        if self.start_func is not None and self.context is not None:
           return 
        raise StaterUnbound(f'Starter {self} must be bound to a start_func and a spider')
    
    def __isbound__(self, func):
        bound_instance = getattr(func, '__self__', None)
        if bound_instance:
           return True
        return False

    def __call__(self, *args, **kwargs):
        async def __(*args, **kwargs):
            self.__check__()
            if self.__isbound__(self.start_func):
               start_func = ensure_asyncgenfunction(self.start_func)
               async_start = start_func(*args,  **kwargs)
            else:
                start_func = ensure_asyncgenfunction(self.start_func)
                async_start = start_func(self.context, *args,  **kwargs)
            async for ret in async_start:
                yield ret
        return __(*args, **kwargs)

    def __str__(self):
        return f'Starter(name={self.name})'
    

class StreamStarter(Starter):
    __operators :List[Callable]

    def __init__(self, name=None):
        super().__init__(name=name)
        self.__operators = []

    def __call__(self, *args, **kwargs):
        starter_func = super().__call__(*args, **kwargs)
        async def __():
            start_stream = stream.iterate(starter_func)
            for operator in self.__operators:
                start_stream = start_stream | operator
            async with start_stream.stream() as streamer:
                async for ret in streamer:
                    yield ret
        return __()
    
    def filter(self, filter: Callable):
        self.__operators.append(pipe.filter(filter))
        return self
    
    def flatmap(self, flatcallback: Callable):
        self.__operators.append(pipe.flatmap(flatcallback))   

    @classmethod
    def from_starter(cls, starter: Starter):
        stream_starter = cls(name=starter.name)
        stream_starter.bind(starter.start_func)
        return stream_starter