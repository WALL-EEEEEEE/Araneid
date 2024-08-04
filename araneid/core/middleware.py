"""
@package: araneid.core.middleware
@author: Wall\'e
@mail:   
@date:   2019.09.29
"""
from abc import ABCMeta, abstractmethod
class Middleware(metaclass=ABCMeta):
    """
        @interface: Middleware
        @desc: middleware interface
    """
    def __init__(self):
        pass


    @classmethod
    @abstractmethod
    async def create(cls):
        raise NotImplementedError(f"create method in {cls.__name__} isn\'t implemented")


class SpiderMiddleware(Middleware):
    """[SpiderMiddleware]
    spidermiddleware conforming to scrapy spidermiddleware.
    """
    @classmethod
    def __subclasshook__(cls, subclass):
       has_process_spider_input = hasattr(subclass, 'process_spider_input') and callable(getattr(subclass, 'process_spider_input'))
       has_process_spider_output = hasattr(subclass, 'process_spider_output') and callable(getattr(subclass, 'process_spider_output'))
       has_process_spider_exception = hasattr(subclass, 'process_spider_exception') and callable(getattr(subclass, 'process_spider_exception'))
       has_process_start_requests = hasattr(subclass, 'process_spider_start_requests') and callable(getattr(subclass, 'process_spider_start_requests'))
       has_order = hasattr(subclass, 'order') and callable(getattr(subclass, 'order'))
       return (has_process_spider_input or has_process_spider_output or has_process_spider_exception or has_process_start_requests)  and has_order

    @classmethod
    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))

    @classmethod
    def from_crawler(cls, crawler):
        pass

class DownloaderMiddleware(Middleware):

    @classmethod
    def __subclasshook__(cls, subclass):
       has_process_request = hasattr(subclass, 'process_request') and callable(getattr(subclass, 'process_request'))
       has_process_response = hasattr(subclass, 'process_response') and callable(getattr(subclass, 'process_response'))
       has_process_exception = hasattr(subclass, 'process_exception') and callable(getattr(subclass, 'process_exception'))
       has_order = hasattr(subclass, 'order') and callable(getattr(subclass, 'order'))
       return (has_process_request or has_process_exception or has_process_response)  and has_order  

    @classmethod
    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))

    def from_crawler(self, spider):
        pass