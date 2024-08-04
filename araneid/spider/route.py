import logging
from abc import ABC, abstractmethod
from araneid.util.iter import is_iterable
from .spider import Parser, Starter

class RouteTable(object):
    logger= None

    def __init__(self):
        self.__routetable__ = {}
        self.logger = logging.getLogger(__name__)

    def __setitem__(self, key, value):
        assert isinstance(key, RouteRule), "key added to RouteTable must be a RouteRule"
        assert value is not None, "value must not be None" 
        self.__routetable__[key] = value
    
    def __getitem__(self, key):
        assert type(key) is str, "key searched in RouteTable must be a string"
        for rule in self.__routetable__.keys():
            if rule.match(key):
                return self.__routetable__[rule]
        return None
    
    def get(self, key):
        return self.__getitem__(key)
    
    def __str__(self):
        return str([ str(rule)  for rule in self.__routetable__ ])

class RouteRule(object):

    @abstractmethod
    def match(self, rule):
        raise NotImplementedError('method match() in RouteRule must be implemented!')

class Router(ABC):

    @abstractmethod
    def starter_route(self, rule):
        raise NotImplementedError('method starter_route() in Router must be implemented!')

    @abstractmethod
    def parser_route(self, rule):
        raise NotImplementedError('method parser_route() in Router must be implemented!')



