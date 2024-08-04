import logging
from typing import Any, AnyStr, Generator, List, Set, Union
from araneid.core.exception import ParserNotFound, StarterNotFound
from araneid.core.request import Request
from araneid.core.response import Response
from araneid.crawler import Crawler
from araneid.item import ItemPipeline
from araneid.util._async import ensure_asyncfunction
from araneid.network.http import HttpRequest
from .parser import Parser
from .starter import Starter

class Spider(Crawler):
    """爬虫类,所有的爬虫都需要实现该类. 爬虫具有两个模式: ``同步模式`` 和 ``异步模式`` , 两个模式下爬虫处理生成请求方式不一样.

        1. 同步处理模式

           爬虫处理的时候,会给每个请求都加一个锁,只有当爬虫的当前请求处理完成 (即当前请求的对应的解析器被调用完成)才会处理下一个请求.

        2. 异步处理模式

           爬虫处理的时候, 所有的请求进行异步处理, 不会有同步锁来保证请求的同步处理.
    """
    logger = None

    def __init__(self, starter='default', **kwargs):
        self.logger = logging.getLogger(__name__)
        self.metas = {}
        self.stats = None
        self.__init_starter()
        self.__init_parser()
        self.__start_starter = starter
        self.__async = False
        self.name = self.__class__.__name__ if not self.name  else  self.name

    def __get_starter_methods(self):
        __methods = [ getattr(self, m) for m in dir(self) if callable(getattr(self, m))]
        for __m in __methods:
            if not  isinstance(__m, Starter):
                continue
            self.add_starter(__m)
    
    def __get_parser_methods(self):
        __methods = [ getattr(self, m) for m in dir(self) if callable(getattr(self, m))]
        for __m in __methods:
            if not  isinstance(__m, Parser):
                continue
            self.add_parser(__m)
    
    def __init_starter(self):
        self.__get_starter_methods()
        starter = self.metas.get('starter', {})
        for s in starter.values():
            s.bind(spider=self)
        # init default starter
        if not starter:
           default_starter = Starter('default')
           default_starter.bind(self.__class__.start_requests, spider=self)
           starter['default'] = default_starter
        self.metas['starter'] = starter

    def __init_parser(self):
       self.__get_parser_methods()
       parser = self.metas.get('parser', {})
       for p in parser.values():
           p.bind(spider=self)

    def add_starter(self, starter: Starter):
        """给爬虫添加启动器

        Args:
            starter (Starter): 添加的启动器
        """
        if 'starter' not in self.metas:
            self.metas['starter'] = {}
        self.metas['starter'][starter.name] = starter

    def add_parser(self, parser:Parser):
        """给爬虫添加解析器

        Args:
            parser (Parser): 添加的解析器
        """
        if 'parser' not in self.metas:
            self.metas['parser'] = {}
        self.metas['parser'][parser.name] = parser
    
  

    def get_starter(self, name: AnyStr=None) -> Starter:
        """根据名字获取指定的启动器
        Args:
            name (AnyStr): 需要获取启动器的名字, 当名字未指定, 默认获取 ``default`` 启动器

        Returns:
            Starter : 根据名字获取到的启动器对象
        """
        starters = self.metas.get('starter', {})
        return starters.get(name)

    def set_starter(self, name: AnyStr, starter: Starter):
        self.metas['starter'][name] = starter
    
    def get_start_starter(self, name: AnyStr=None) -> Starter:
        assert name is None or isinstance(name, str), 'name must be None or a string'
        if not name:
           name = self.__start_starter
        start_starter = self.get_starter(name)
        if start_starter is None:
            raise StarterNotFound('Starter '+name+' not found in Spider '+self.name)
        return start_starter

    def set_start_starter(self, name: AnyStr):
        """设置爬虫启动调用的启动器, 默认调用 ``default`` 启动器

        Args:
            starter (AnyStr): 启动器的名字
        """
        assert isinstance(name, str), 'name must be  a string'
        start_starter = self.get_starter(name)
        if start_starter is None:
           raise StarterNotFound('Starter '+name+' not found in Spider '+self.name)
        _old = self.__start_starter
        self.__start_starter = name
        return _old 

    def set_parser(self, name, parser:Parser):
        self.metas['parser'][name] = parser

    def get_parser(self, name: AnyStr=None) -> Parser:
        """根据名字获取指定的解析器

        Args:
            name (AnyStr, optional): 解析器的名字, 默认为 ``None`` 
        Raises:
            ParserNotFound: 解析器未找到异常

        Returns:
            Parser: 获得的解析器实例
        """
        parsers = self.metas.get('parser', {})
        return parsers.get(name, None) 

    def get_starters(self) -> List[Starter]:
        """获取爬虫所有的启动器

        Returns:
            List[Starter]: 爬虫所有的启动器列表
        """
        return self.metas.get('starter', {}).values()

    def get_parsers(self) -> List[Parser]:
        """获取爬虫所有的解析器

        Returns:
            List[Parser]: 爬虫所有的解析器列表
        """
        return self.metas.get('parser', {}).values()
    
    def is_sync(self) -> bool:
        """爬虫是否是同步处理模式

        Returns:
            bool: ``True`` 表示爬虫处于同步模式, ``False`` 表示爬虫处于异步模式 
        """
        return not self.__async

    def set_sync(self):
        """设置爬虫为同步处理模式
        """
        self.__async = False
    
    def set_async(self):
        """设置爬虫为异步处理模式
        """
        self.__async = True
    
    @staticmethod
    def create(cls, *args, **kwargs):
        return cls(*args, **kwargs)
    
    def start_urls(self)->str:
        if self.urls is None:
            return None
        if type(self.urls) is str:
            self.urls = self.urls.split(',')
        for url in self.urls:
            yield url

    def start_requests(self)-> HttpRequest:
        for url in self.start_urls():
            request = HttpRequest(url, callbacks=[self.parse])
            yield request
    
    def parse(self, response: Response) -> Generator[Union[Request, ItemPipeline], None, None]:
        pass

    async def start(self):
        started_func = getattr(self, 'started', None)
        if started_func is None:
           return
        if not callable(started_func):
           self.logger.warning(f'started property of {self} must be a callable!')
           return
        started_func = ensure_asyncfunction(started_func)
        await started_func(self)

    async def close(self):
        closed_func = getattr(self, 'closed', None)
        if closed_func is None:
           return
        if not callable(closed_func):
           self.logger.warning(f'closed property of {self} must be a callable!')
           return
        closed_func= ensure_asyncfunction(closed_func)
        await closed_func(self)


    
    def __str__(self):
        spider_name = self.name if self.name else self.__class__.__name__
        return spider_name
