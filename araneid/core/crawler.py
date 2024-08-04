#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.05.07
"""
import logging
import types
from abc import ABC, abstractmethod
from enum import IntEnum, Enum
from .response import Response
from .request import Request
from araneid.spider import Starter 
from araneid.spider.exception import InvalidCrawler
from functools import partial


class ParserContext:
    __PARSER_EXPORT__ = ['items']

    def __init__(self, parser, spider):
        object.__setattr__(self, 'parser', parser)
        object.__setattr__(self, 'spider', spider)

    def __getattribute__(self, name):
        try:
            spider = object.__getattribute__(self, 'spider')
            return getattr(spider, name)
        except AttributeError:
            parser = object.__getattribute__(self, 'parser')
            paser_export = object.__getattribute__(self, '__PARSER_EXPORT__')
            if name not in paser_export:
                raise AttributeError
            return getattr(parser, name)
    
    def __setattr__(self, name, value):
        spider = object.__getattribute__(self, 'spider')
        return object.__setattr__(spider, name, value)


class Crawler(ABC):
    """
        @class:  araneid.core.Crawler
        @description:
           Basic ancestor class for all crawler classes
    """
    logger = None 

    class Status(Enum):
        START = 0
        RUNNING = 1
        CLOSE = 2

    __engine = None
    urls = []
    name = None
    __status = Status.START  # status  of current crawler, 0 is start, 1 is running , 2 is closed

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, status: Status):
        self.__status = status

    def __check_spider_name(self):
        if type(self.name) is str and self.name:
            return
        raise InvalidCrawler('name is not set in '+self.__class__.__name__)

    def __init__(self):
        self.__check_spider_name()
        self.logger = logging.getLogger(__name__)


    @abstractmethod
    def start_urls(self) -> str:
        """
        all urls it yields or returns will be regarding as start urls  passed to scheduler
        :return List[str], a pile of urls
        :yield  str
        """
        pass

    @abstractmethod
    def start_requests(self) -> Request:
        """
        all request it yields or returns will be regarding as start request passed to scheduler
        :return List[Request], a pile of Request
        :yield  Request
        """
        raise NotImplementedError('start_requests method in Crawler isn\'t implemented')

    @abstractmethod
    def parse(self, response: Response):
        """
        Default callback to request, and can be customized in constructing Request
        :param response: response from target request
        :yield Request | Response  request yielded or returned will be scheduled and downloaded, and response yielded or returned will be piped
        :return List[Request] | List[Response] request yielded or returned will be scheduled and downloaded, and response yielded or returned will be piped
        """
        raise NotImplementedError('parse method in Crawler isn\'t implemented')

    def close(self):
        pass

    def __str__(self):
        return self.__class__.__module__ + '.' + self.__class__.__name__
