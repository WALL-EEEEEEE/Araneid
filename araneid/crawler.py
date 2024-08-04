"""
@author: Wall\'e
@mail:   
@date:   2019.05.07
"""
import logging
from abc import ABC, abstractmethod
from typing import Generator, Union
from araneid.item import ItemPipeline
from araneid.core.response import Response
from araneid.core.request import Request
from araneid.core.exception import InvalidCrawler
class Crawler(ABC):
    """爬虫抽象类，所有的爬虫都必须继承自该类
    """
    logger = None 
    settings = dict()
    urls = []
    name = None

    @classmethod
    def from_settings(cls, settings):
        inst_crawler = cls()
        inst_crawler.settings = settings
        return inst_crawler

    def __check_spider_name(self):
        if type(self.name) is str and self.name:
            return
        raise InvalidCrawler('name is not set in '+self.__class__.__name__)

    def __init__(self, starter='default'):
        super().__init__(starter)
        self.logger = logging.getLogger(__name__)
        self.__check_spider_name()
    
    @abstractmethod
    def start_urls(self) -> Generator[str, None, None]:
        """url初始化函数

        当爬虫中start_requests方法未被复写，爬虫启动时, 在默认的start_requests方法会调用该方法，获取初始化url，并根据初始化URL生成对应的初始化请求

        Yields:
            Generator[str, None, None]: 初始化URL
        """
        pass

    @abstractmethod
    def start_requests(self) -> Generator[Request, None, None]:
        """请求初始化函数

        Raises:
            NotImplementedError: 所有爬虫都必须实现该方法来生成初始化请求，否则会抛出该异常

        Yields:
           Generator[Request, None, None]: 初始化请求
        """
        raise NotImplementedError('start_requests method in Crawler isn\'t implemented')

    @abstractmethod
    def parse(self, response: Response) -> Generator[Union[Request, ItemPipeline], None, None]:
        """默认的解析器函数

        所有爬虫必须实现该抽象方法，来提供至少一个解析器函数。如果爬虫未实现该方法，将抛出NotImplementedError异常. 该方法返回一个生产Request或者ItemPipeline的生成器

        Args:
            response (Response): 解析器解析的请求响应

        Raises:
            NotImplementedError: 当子类中未实现该方法时抛出

        Yields:
            Generator[Union[Request, Response], None, None]: 该方法可以生成Request或者ItemPipeline对象，生成的Request对象，会被框架调度请求，生成的ItemPipeline会被框架收集里面的采集信息。
        """

        raise NotImplementedError('parse method in Crawler isn\'t implemented')


    def close(self) -> None:
       """爬虫关闭方法

       当爬虫所有的请求以及请求响应被处理完， 该方法会被调用, 爬虫可以实现改方法，对爬虫里面使用的一些资源进行关闭和释放操作

       Returns: 
            None
       """
       pass

    def __str__(self):
        return self.__class__.__module__ + '.' + self.__class__.__name__