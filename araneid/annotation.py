#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.07.04
"""
from __future__ import absolute_import
import functools
import inspect
import itertools
import sys
import logging
from typing import Any, Tuple, Callable
from crython import expression
from araneid.core.request import Request
from araneid.core.response import Response
from araneid.spider import Spider
from araneid.runners.crawler import CrawlerRunner as CrawlerRunner_
from araneid.runners.cli import CrawlerCliRunner as CrawlerCliRunner_
from araneid.runners import CrawlerCrontabRunner as CrawlerCrontabRunner_
from araneid.runners import SCRIPT_RUNNABLE
from araneid.util.cls import get_class_method_that_defined, get_class_that_defined_method
from araneid.util.gen import flatten
from araneid.util.cli import *
from .spider import Spider

arguments = []

def Sync(spider):
    def __(*args, **kwargs):
        spider(args, kwargs)
    spider.set_sync() 
    return __

def crontab(expr=None, **kwargs):
    expr = expression.CronExpression.new(expr, **kwargs)
    def __(cls):
        CrawlerCliRunner_.crontab = str(expr)
        return cls
    return __


def interval(interval):
    def __(cls):
        CrawlerCliRunner_.interval = interval
        return cls
    return __



class Crawler:
    crawler_wrapped_parser = ()

    def __init__(self, crawler: Tuple[Spider] = Spider, parser: Tuple[Callable] = ()):
        if type(crawler) is not tuple:
            self.crawler_wrapped = (crawler,)
        else:
            self.crawler_wrapped = crawler

        for c in self.crawler_wrapped:
            if parser is None or (type(parser) is tuple and len(parser) == 0):
                self.crawler_wrapped_parser += (c.parse,)
            else:
                if type(parser) is not tuple:
                    parser = (parser,)
                for p in parser:
                    if issubclass(c, get_class_that_defined_method(p)):
                        self.crawler_wrapped_parser += (p,)
                    else:
                        self.crawler_wrapped_parser += (c.parse,)

    def __call__(self, crawler_wrapper_: Spider= None):
        crawler_wrapper = crawler_wrapper_
        if crawler_wrapper is None:
            crawler_wrapper = self.crawler_wrapped[0]
            self.crawler_wrapped = (Spider, )

        inherit_class = (crawler_wrapper,) + self.crawler_wrapped
        crawler_wrapper = type(crawler_wrapper.__name__, inherit_class, {"__module__": crawler_wrapper.__module__})
        self.crawler_wrapper = crawler_wrapper
        self.crawler_wrapper.old_parser = self.crawler_wrapper.parse
        self.crawler_wrapper.parse = self.__parse_wrapper(tuple(c.parse for c in self.crawler_wrapped))
        if crawler_wrapper_ is None:
            return self.crawler_wrapper()
        return self.crawler_wrapper

    def __hook_callback(self, context):
        def __hook_wrapper(c):
            def process_by(creturn):
                if isinstance(creturn, Response):
                    return self.crawler_wrapper.old_parser.__get__(context, self.crawler_wrapper)(creturn)
                elif isinstance(creturn, Request):
                    return creturn

            def _wrapper(*args, **kwargs):
                global context
                if len(args) > 1:
                    response = args[1]
                    context = args[0]
                else:
                    response = args[0]
                yield from flatten(map(process_by, c(response)))

            return _wrapper

        def __filter_hooked_callbacks(callbacks, wrapped_parser):
            for c in callbacks:
                if get_class_method_that_defined(c) in wrapped_parser:
                    yield __hook_wrapper(c)

        def __hook(result):
            if isinstance(result, Response):
                return self.crawler_wrapper.old_parser.__get__(context, self.crawler_wrapper)(result)
            if isinstance(result, Request):
                callbacks_hooked = list(__filter_hooked_callbacks(result.callbacks, self.crawler_wrapped_parser))
                if callbacks_hooked:
                    result.callbacks = callbacks_hooked
            return result

        return __hook

    def __parse_wrapper(self, __crawler_wrapped_parser):
        @functools.wraps(self.crawler_wrapper.parse)
        def _wrapper(*args, **kwargs):
            global context
            if len(args) > 1:
                response = args[1]
                context = args[0]
            else:
                response = args[0]
            for cwrap in self.crawler_wrapped:
                map_result = map(self.__hook_callback(context), cwrap.parse(context, response))
                yield from flatten(map_result)

        context = self.crawler_wrapper
        is_abstract = len(list(itertools.filterfalse(lambda c_w: inspect.isabstract(c_w), self.crawler_wrapped))) == 0
        if is_abstract:
            return self.crawler_wrapper.parse
        else:
            return _wrapper


class Arachnia:
    logger = None
    crawler_injector = ()

    def __init__(self, crawler: Tuple[Spider], parser: Tuple[Callable]):
        self.logger = logging.getLogger(__name__)
        if type(crawler) is not tuple:

            self.crawler_injector = (crawler,)
        else:
            self.crawler_injector = crawler

        if type(parser) is not tuple:
            self.crawler_parser = (parser,)
        else:
            self.crawler_parser = parser

    def __call__(self, brood_crawler: Spider):
        inherit_class = (brood_crawler,) + self.crawler_injector
        self.brood_crawler = type(brood_crawler.__name__, inherit_class, {"__module__": brood_crawler.__module__})
        for parser_str in self.crawler_parser:
            parser = self.__get_paser(parser_str)
            setattr(self.brood_crawler, parser_str, self.__parse_wrapper(parser))
        return self.brood_crawler

    def __get_paser(self, parser_name):
        return getattr(self.brood_crawler, parser_name)

    def __hook_injector(self, context):
        def __hook_wrapper(__return):
            if isinstance(__return, Request):
                yield __return
            else:
                for c in self.crawler_injector:
                    inject_return = c.parse(context, __return)  
                    if inspect.isgenerator(inject_return):
                        yield from inject_return
                    else:
                        yield inject_return
        return __hook_wrapper

    def __parse_wrapper(self, __crawler_wrapped_parser):
        @functools.wraps(__crawler_wrapped_parser)
        def _wrapper(*args, **kwargs):
            global context
            if len(args) > 1:
                response = args[1]
                context = args[0]
            else:
                response = args[0]
            parser_return = __crawler_wrapped_parser(context, response)
            if parser_return is not None:
                ret = flatten(map(self.__hook_injector(context), parser_return))
                yield from ret
        context = self.brood_crawler
        return _wrapper


class CrawlerRunner:
    runner = None

    def __new__(cls, *args, **kwargs):
        if SCRIPT_RUNNABLE:
            return super(CrawlerRunner, cls).__new__(cls)
        else:
            crawler_class =args[0]
            if cls.runner:
                if issubclass(crawler_class, Spider):
                    crawler = crawler_class()
                    cls.runner.add_crawler(crawler)
                return cls.runner

    def __init__(self, crawler: Spider):
        assert issubclass(crawler, Spider)
        self.crawler_ = crawler()
        runner = CrawlerRunner_()
        runner.add_crawler(self.crawler_)
        runner.run()


class CrawlerCliRunner(CrawlerRunner):
    runner = None
    crawler = None

    def __init__(self, crawler: Spider):
        assert issubclass(crawler, Spider)
        self.__init_runner()
        CrawlerCliRunner.runner.add_argument('project_id', '__araneid_project_id', None, None, True)
        CrawlerCliRunner.runner.add_argument('job_id', '__araneid_job_id', None, None, True)
        CrawlerCliRunner.runner.add_argument('job_record_id', '__araneid_job_record_id', None, None, True)
        CrawlerCliRunner.runner.add_argument('task_id', '__araneid_task_id', None, None, True)
        CrawlerCliRunner.runner.add_argument('task_code', '__araneid_task_code', None, None, True)
        CrawlerCliRunner.runner.add_argument('url', 'urls', None, None, True)
        self.crawler = crawler()
        self.runner.add_crawler(self.crawler)

    def run(self):
        self.runner.run()
    
    @staticmethod
    def __init_runner():
        if not CrawlerCliRunner.runner:
            CrawlerCliRunner.runner = CrawlerCliRunner_()

    @staticmethod
    def crontab(expr=None, **kwargs):
        def __(cls):
           CrawlerCliRunner_.crontab = expr
           return cls

        return __

    @staticmethod
    def interval(interval):
        def __(cls):
            CrawlerCliRunner_.interval = interval
            return cls
        return __
    
    @staticmethod
    def runforever():
        def __(cls):
            CrawlerCliRunner_.forever = True
            return cls
        return __
 


    @staticmethod
    def debug():
        def __(cls):
            sys.argv.append('--loglevel=DEBUG')
            return cls

        CrawlerCliRunner.__init_runner()
        return __

    @staticmethod
    def argument(name, alias='', help='', default='', mapping=True, **kwargs):
        def __(cls):
            #CrawlerCliRunner.runner.add_argument(name, alias, help, default, mapping, **kwargs)
            return cls

        CrawlerCliRunner.__init_runner()
        return __

    @staticmethod
    def option(name, alias='', help='', default='', mapping=True, **kwargs):
        def __(cls):
            #CrawlerCliRunner.runner.add_option(name, alias, help, default, mapping, **kwargs)
            return cls
        CrawlerCliRunner.__init_runner()
        return __


class CrawlerCrontabRunner(CrawlerRunner):
    runner = None

    def __init__(self, crawler: Spider):
        assert issubclass(crawler, Spider)
        CrawlerCrontabRunner.__init__runner()
        self.run()

    @staticmethod
    def crontab(expr=None, **kwargs):
        def __(cls):
            CrawlerCrontabRunner.runner.add_crawler(cls(), expr, **kwargs)
            return cls

        CrawlerCrontabRunner.__init__runner()
        return __

    @staticmethod
    def __init__runner():
        if not CrawlerCrontabRunner.runner:
            CrawlerCrontabRunner.runner = CrawlerCrontabRunner_()

    def run(self):
        self.runner.run()