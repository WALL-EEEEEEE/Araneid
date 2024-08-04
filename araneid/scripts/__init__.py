import argparse
import asyncio
import os
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor
from importlib.machinery import SourceFileLoader
from itertools import filterfalse
from os.path import basename, splitext
from araneid.core import plugin as plugins
from araneid.core.exception import (InvalidSpider, InvalidStarter, PluginError,
                                    SpiderNotFound)
from araneid.runner import AsyncCronRunner, AsyncRunner
from araneid.spider import Spider
from araneid.spider.spider import Starter


def load_script_plugin():
    script_plugins = plugins.load(plugins.PluginType.SCRIPT)
    scripts = dict()
    for plugin in script_plugins:
        name = plugin.name
        script = plugin.load()
        try:
            scripts[name] = script
        except Exception as e:
            raise PluginError(f"Error occurred in while loading script {name}!") from e
    return scripts 

def auto_detect_script_format(script_path):
    ext = str.upper(splitext(script_path)[1])
    return ext.strip('.')


def check_if_valid_spider_script(script_path):
    suffix_python = os.path.splitext(basename(script_path))[1]
    spider_name = os.path.splitext(basename(script_path))[0]
    if suffix_python != '.py':
        return False
    return True


def get_spider_from_py(python_script, spider=''):
    from araneid.annotation import CrawlerRunner as CrawlerRunnerAnnotation 
    spider_module = SourceFileLoader("module.name", python_script).load_module()
    if not hasattr(spider_module, spider):
        raise  SpiderNotFound('Spider '+spider+' doesn\'t  exist in '+ python_script)
    spider_class = getattr(spider_module, spider)
    if isinstance(spider_class, CrawlerRunnerAnnotation):
        return spider_class
    else:
        inst_spider = spider_class()
        if isinstance(inst_spider, Spider):
            return inst_spider
    raise InvalidSpider('Spider '+spider+' isn\'t a valid defined spider')

def asyncio_run(main):
    def _cancel_all_tasks(loop):
        to_cancel = asyncio.all_tasks(loop)
        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        loop.run_until_complete(
            asyncio.gather(*to_cancel, loop=loop, return_exceptions=True))

        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                loop.call_exception_handler({
                    'message': 'unhandled exception during asyncio.run() shutdown',
                    'exception': task.exception(),
                    'task': task,
                })
    
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    #loop.set_debug(enabled=True)
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(main)
    finally:
        try:
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def start_spider(spiders, starter=None, **kwargs):
    if not isinstance(spiders, list):
        spiders = [spiders]
    if not starter:
        starter = 'default'
    crontab_expr = kwargs.get('crontab', None)
    async def __():
        nonlocal crontab_expr
        if crontab_expr is not None:
            runner = await AsyncCronRunner.create()
        else:
            runner = await AsyncRunner.create()
        for spider in spiders:
            spider.get_start_starter(starter)
            if crontab_expr is not None:
               runner.add_spider(spider, crontab_expr, starter=starter)
            else:
               runner.add_spider(spider, starter=starter)
        await runner.start()
    asyncio_run(__())

def resolve_dependences(dependence_path):
    # add framework path and extra imported module into search path
    if dependence_path not in sys.path:
        sys.path.append(dependence_path)

class DeprecateAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        warnings.warn("Argument %s is deprecated and is ignored." % self.option_strings)
        delattr(namespace, self.dest)

class DeprecateStoreTrueAction(DeprecateAction,argparse._StoreTrueAction):

    def __call__(self, parser, namespace, values, option_string=None):
        super(argparse._StoreTrueAction, self).__call__(parser, namespace, values, option_string)
        return super().__call__(parser, namespace, values, option_string)

 
    


def mark_deprecated_help_strings(parser, prefix="DEPRECATED"):
    for action in parser._actions:
        if isinstance(action, DeprecateAction):
            h = action.help
            if h is None:
                action.help = prefix
            else:
                action.help = prefix + ": " + h