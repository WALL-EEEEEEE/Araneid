
import inspect
from typing import AnyStr, Callable, List, Optional, Union
from aiostream import stream, pipe
from araneid.core.context import ParserContext
from araneid.util._async import ensure_asyncgenfunction
from araneid.core.exception import InvalidParser, ParserUnbound
from araneid.item import Item

class Parser(object):
    name: AnyStr 
    url: Union[AnyStr, List[AnyStr]]
    regex: Union[AnyStr, List[AnyStr]]
    context: Optional[ParserContext]
    parse_func: Optional[Callable]
    items: Item

    def __init__(self, name=None, url=None, regex=None, item=None):
        if not (isinstance(url, (str, list)) or url is None):
            raise InvalidParser("url of parser  {parser} must be a url or a list of url".format(parser=name))
        if not (isinstance(regex, (str, list)) or regex is None):
            raise InvalidParser("regex of parser  {parser} must be a regex or a list of regex".format(parser=name))
        if not ((inspect.isclass(item) and issubclass(item, Item)) or item is None):
            raise InvalidParser("item of parser  {parser} must be a subclass of Item".format(parser=name))

        if isinstance(url, str):
           url = [url]
        if isinstance(regex, str):
           regex = [regex]
        self.url = url
        self.regex = regex
        self.name = name
        self.parse_func = None
        self.context = None
        if not item: 
           item = Item(name=name)
        else:
           item = item()
        self.items = item

    def bind(self, parse_func = None, spider=None):
        assert isinstance(parse_func, (Callable, type(None))), "parse_func must be a callable"
        if  parse_func is not None:
            self.parse_func = parse_func
        self.context = ParserContext(self, spider)
    
    def __check__(self):
        if self.parse_func is not None:
           return 
        raise ParserUnbound(f'Parser {self} must be bound to a parser_func')
    
    def __isbound__(self, func):
        bound_instance = getattr(func, '__self__', None)
        if bound_instance:
           return True
        return False

    def __call__(self, *args, **kwargs):
        async def __(*args, **kwargs):
            self.__check__()
            if self.__isbound__(self.parse_func):
                async_parse_func = ensure_asyncgenfunction(self.parse_func) 
                async_parse = async_parse_func(*args,  **kwargs)
            else:
                async_parse_func = ensure_asyncgenfunction(self.parse_func) 
                async_parse = async_parse_func(self.context, *args,  **kwargs)
            async for ret in async_parse:
                yield ret
        return __(*args, **kwargs)
    

    def __str__(self):
        str_prt = self.name
        match = []
        if self.url:
            match.append('url='+str(self.url)+'')
        if self.regex:
            match.append('regex='+str(self.regex)+'')
        str_prt += ' - match('+', '.join(match)+')'
        return str_prt
    
    def add_field(self, field):
        self.items.add_field(field)

    def add_item(self, item, rule):
        self.add_field(item)


class StreamParser(Parser):
    __operators :List[Callable]

    def __init__(self, name=None, url=None, regex=None):
        super().__init__(name=name, url=url, regex=regex)
        self.__operators = []

    def __call__(self, *args, **kwargs):
        parse_func = super().__call__(*args, **kwargs)
        async def __():
            parse_stream = stream.iterate(parse_func)
            for operator in self.__operators:
                parse_stream = parse_stream | operator
            async with parse_stream.stream() as streamer:
                async for ret in streamer:
                    yield ret
        return __()
    
    def filter(self, filter: Callable):
        self.__operators.append(pipe.filter(filter))
        return self
    
    def flatmap(self, flatcallback: Callable):
        self.__operators.append(pipe.flatmap(flatcallback))
    
    @classmethod
    def from_parser(cls, parser: Parser):
        stream_parser = cls(name=parser.name, url=parser.url, regex=parser.regex)
        stream_parser.items = parser.items
        stream_parser.bind(parser.parse_func)
        return stream_parser