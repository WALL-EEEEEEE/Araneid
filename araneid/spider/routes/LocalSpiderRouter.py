import re
import logging
from araneid.util.iter import is_iterable
from ..route import RouteTable, RouteRule, Router
from ..spider import Parser, Starter

class RegexRouteRule(RouteRule):
    logger = None

    def __init__(self, regex):
        self.logger = logging.getLogger(__name__)
        self.__rule = re.compile(regex)

    def match(self, rule):
        return False if self.__rule.match(rule) is None else True
    
    def __str__(self):
        return 'RegexRouteRule(regex={regex})'.format(regex=self.__rule)

class NameRouteRule(RouteRule):
    def __init__(self, name):
        self.__rule = name

    def match(self, rule):
        return self.__rule == rule
    
    def __str__(self):
        return 'NameRouteRule(name={name})'.format(name=self.__rule)

class URLRouteRule(RouteRule):
    def __init__(self, url):
        self.__rule = url

    def match(self, rule):
        return self.__rule == rule
    
    def __str__(self):
        return 'URLRouteRule(url={url})'.format(url=self.__rule)

class LocalSpiderRouter(Router):
    logger = None

    def __init__(self, settings):
        self.logger = logging.getLogger(__name__)
        self.__route_tables = RouteTable()

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    def add_starter_route(self, rule, target):
        route_rule = NameRouteRule('starter.'+rule)
        self.__route_tables[route_rule] = target

    def add_parser_route(self, rule, target):
        # add parser name route
        parser_name_route = NameRouteRule('parser.'+rule)
        self.__route_tables[parser_name_route] = target
        # add parser url route
        if isinstance(target, Parser) and getattr(target,'url', None):
            for url in target.url:
                parser_url_match_rule = URLRouteRule(url)
                self.__route_tables[parser_url_match_rule] = target
        # add parser regex route
        if isinstance(target, Parser) and getattr(target, 'regex', None):
            for regex in target.regex:
                parser_url_regex_match_rule = RegexRouteRule(regex)
                self.__route_tables[parser_url_regex_match_rule] = target

    def starter_route(self, rules):
        assert type(rules) is list, "Rules must be a list in starter_route"
        if not rules:
            return 
        if not is_iterable(rules):
            rules = iter(rules)
        for  rule in rules:
            target = self.__route_tables.get(rule)
            if target:
               return target

    def parser_route(self, rules):
        assert type(rules) is list, "Rules must be a list in parser_route"
        if not rules:
            return 
        if not is_iterable(rules):
            rules = iter(rules)
        for  rule in rules:
            target = self.__route_tables.get(rule)
            if target:
               return target