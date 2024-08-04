"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import abc
import logging
import os
import re
import sys
import threading
import time
import traceback
import asyncio
import _thread
from abc import ABC
from argparse import ArgumentParser
from copy import copy
from itertools import filterfalse
from signal import signal as signal_
from crython.tab import CronTab
from araneid.core import signal 
from araneid.core.engine import Engine, Slot
from araneid.tools.cron import CrontabJob
from araneid.logger import config_log
from araneid.setting import settings as settings_loader
from araneid.spider import Starter, Spider
from araneid.scraper import Scraper
from araneid.util.platform import is_windows, is_linux, is_macos
from concurrent.futures import ThreadPoolExecutor

if not is_windows():
   from signal import pthread_kill,SIGURG, SIGQUIT
   from araneid.util._events import ThreadedChildWatcher
else:
   from signal import SIGINT, SIGTERM



SCRIPT_RUNNABLE = True

class CrawlerCrontabJob:
    logger = None
    default_job = CrontabJob()


    @property
    def cron_expression(self):
        return self.job.crontab_expression

    @property
    def ctx(self):
        return self.job.ctx

    @property
    def name(self):
        return self.job.name
    
    def __ignore_cancel_exception(self, loop, context):
        exception = context.get('exception')
        if isinstance(exception, asyncio.CancelledError):
            return
        loop.default_exception_handler(context)

    def set_max_asyncio_workers(self, settings):
        max_asyncio_workers = settings.get('MAX_ASYNCIO_WORKERS', None) 
        if max_asyncio_workers is None:
            max_asyncio_workers = min(32, os.cpu_count()+4)
        else:
            max_asyncio_workers = int(max_asyncio_workers)
        loop = asyncio.get_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=max_asyncio_workers))


    def __call__(self, *args, **kwargs):
        try:
            __event_loop__ = asyncio.get_event_loop()
        except RuntimeError:
            __event_loop__ = asyncio.new_event_loop()
        finally:
            __event_loop__.set_exception_handler(self.__ignore_cancel_exception)
            asyncio.set_event_loop(__event_loop__)
            if not is_windows():
               asyncio.set_child_watcher(ThreadedChildWatcher())
        self.set_max_asyncio_workers(self.settings)
        signalmanager = signal.SignalManager()
        signal.set_signalmanager(signalmanager)
        self.logger.debug('__call__ signalmanager: {id}'.format(id=id(signalmanager)))
        self.engine = Engine(self.settings)
        scraper = Scraper.from_settings(self.settings)
        scraper.add_spider(self.spider)
        slot = Slot()
        async def __():
            scraper.bind(slot)
            await self.engine.add_slot(slot)
            await asyncio.gather(self.engine.start(), scraper.start())
        try:
            run_container = asyncio.gather(__())
            self.__running_tasks__.append(run_container)
            __event_loop__.run_until_complete(run_container)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.exception(e)
            self.stop(loop=__event_loop__)
            pthread_kill(threading.main_thread().ident, SIGURG)
        except SystemExit as e:
            pthread_kill(threading.main_thread().ident, SIGINT)

    def __init__(self, spider, starter="default",  cron_job=None, cron_expr=None, settings=None, **kwargs):
        assert (type(cron_job) is CrontabJob or cron_job is None)
        assert (type(cron_expr) is str or cron_expr is None)
        self.logger = logging.getLogger(__name__)
        self.__stopped__ = False
        if cron_expr is not None:
            self.job = CrontabJob.from_expr(cron_expr)
        if cron_job is not None:
            self.job = cron_job
        if len(kwargs) > 0:
            self.job = CrontabJob(**kwargs)
        self.spider= spider
        self.settings = settings if settings is not None else settings_loader
        if starter and isinstance(spider, Spider):
           spider.set_start_starter(starter)
        self.__running_tasks__ = []
    
    def stop(self, loop=None):
        if self.__stopped__:
            return
        if getattr(self, 'engine', None) is None:
            return
        for task in self.__running_tasks__:
            task.cancel()
        self.__stopped__ = True
 
    def __str__(self):
        return '.'.join([self.__class__.__module__, self.__class__.__qualname__])



class BaseCrawlerRunner(ABC):

    logger = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.engine = None
        self.spiders= []
        self.closed = False

    def register_signal_handler(self):
        def __(sig_num, stack):
            self.close()
            sys.exit()
        try:
            if not is_windows():
               signal_(SIGQUIT, __)
            else:
               signal_(SIGTERM, __)
               signal_(SIGINT, __)
        except Exception as e:
            self.logger.warning('Failed to register signal onto araneid, quit while running may be not work due to this cause')


    @abc.abstractmethod
    def add_crawler(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    def close(self):
        if self.engine is not None:
            self.engine.close()
        self.engine = None
        self.spiders.clear()
        self.closed = True


class CrawlerRunner(BaseCrawlerRunner):
    """
      @class: CrawlerRunner
      @desc:  Runner to start crawlers
    """

    def __init__(self):
        super().__init__()
        self.__settings__ = settings_loader
        self.set_timezone(self.__settings__)
        self.set_max_asyncio_workers(self.__settings__)
        self.__running_tasks__ = []

    def set_timezone(self, settings):
        timezone = settings.get('TIME_ZONE', 'Asia/Shanghai')
        os.environ['TZ'] = timezone
        if not is_windows():
           time.tzset()

    def set_max_asyncio_workers(self, settings):
        max_asyncio_workers = settings.get('MAX_ASYNCIO_WORKERS', None) 
        if max_asyncio_workers is None:
            max_asyncio_workers = min(32,os.cpu_count()+4)
        else:
            max_asyncio_workers = int(max_asyncio_workers)
        loop = asyncio.get_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=max_asyncio_workers))

    def register_signal_handler(self):
        self.force_exit = False
        def __(sig_num, stack):
            if not self.force_exit:
                if sig_num == SIGINT:
                    self.logger.warning('RUNNER CLOSE DUE TO KEYBOARD TERMINATE, TRY IT AGAIN TO FORCE EXIT RUNNER.')
                    self.close()
                    self.force_exit = True
                if sig_num == SIGTERM or (not is_windows() and sig_num == SIGQUIT):
                    self.logger.warning('RUNNER CLOSE DUE TO KILLED, KILL SIGNAL IS ' + str(sig_num)+', TRY IT AGAIN TO FORCE EXIT RUNNER.')
                    self.close()
                    self.force_exit =  True 
            else:
                sys.exit()
        def __2(sig_num, stack):
            sys.exit()

        try:
            if not is_windows():
               signal_(SIGQUIT, __)
               signal_(SIGURG, __2)
            else:
               signal_(SIGTERM, __)
               signal_(SIGINT, __)
        except Exception as e:
            self.logger.warning('Failed to register signal onto araneid, quit while running may be not work due to this cause')



    def add_crawler(self, spider: Spider, starter: Starter = 'default'):
        """
        Add a crawler to be run
        :param crawler: Crawler,crawler to be run
        :return None
        """
        self.spiders.append((spider,starter))

    def __ignore_cancel_exception(self, loop, context):
        exception = context.get('exception')
        if isinstance(exception, asyncio.CancelledError):
            return
        loop.default_exception_handler(context)
   


    def run(self):
        """
        Run all crawlers
        :return: None
        """
        self.logger.debug("ACTIVE SPIDERS: " + str([ c[0].__class__.__qualname__ for c in self.spiders]))
        signalmanager = signal.SignalManager()
        signal.set_signalmanager(signalmanager)
        self.logger.debug('__call__ signalmanager: {id}'.format(id=id(signalmanager)))
        self.engine = Engine.from_settings(self.__settings__)
        scraper = Scraper.from_settings(self.__settings__)
        slot = Slot()
        async def __():
            scraper.bind(slot)
            await self.engine.add_slot(slot)
            await asyncio.gather(self.engine.start(), scraper.start())
        for c in self.spiders:
            spider=c[0]
            spider_starter = c[1]
            if spider_starter and isinstance(spider, Spider):
                   spider.set_start_starter(spider_starter)
            scraper.add_spider(spider)
        try:
            self.__event_loop__ = asyncio.get_event_loop()
        except RuntimeError:
            self.__event_loop__ = asyncio.new_event_loop()
            asyncio.set_event_loop(self.__event_loop__)
            asyncio.set_child_watcher(ThreadedChildWatcher())
        #self.__event_loop__.set_debug(True)
        self.__event_loop__.set_exception_handler(self.__ignore_cancel_exception)
        running_task = asyncio.gather(__())
        self.__running_tasks__.append(running_task)
        if self.__event_loop__.is_running():
            import nest_asyncio
            nest_asyncio.apply()
        try:
            self.__event_loop__.run_until_complete(running_task)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.close()
    
    def close(self):
        if self.closed:
            self.logger.debug("RUNNER CLOSED")
            return
        for task in self.__running_tasks__:
            task.cancel()
        self.engine = None
        self.closed = True
        self.spiders.clear()
        self.__running_tasks__.clear()
        self.logger.debug("RUNNER CLOSED")

    def __str__(self):
        return '.'.join([self.__module__, self.__class__.__qualname__])


class CrawlerCrontabRunner(CrawlerRunner):
    default_interval = 1
    spider_crontab_jobs = []

    def __init__(self):
        super().__init__()
        self.cron = CronTab('default')
   
    def add_crawler(self, spider: Spider, starter:Starter="default", crontab_expr='0 */1 * * * * *', **kwargs):
        job = CrawlerCrontabJob(spider, starter, settings=self.__settings__,  cron_expr=crontab_expr, **kwargs)
        self.spider_crontab_jobs.append(job)

    def run(self):
        for job in self.spider_crontab_jobs:
            self.cron.register(job.spider.__class__.__name__, job)
        self.cron.start()
        self.cron.join()

    def close(self):
        self.cron.stop_event.set()
        for job in self.spider_crontab_jobs:
            job.stop()
        self.spider_crontab_jobs.clear()
        self.cron.join()
        self.cron.stop()
 

class CrawlerCliRunner(CrawlerRunner):
    crontab = None
    interval = None
    ARG_MODE = False
    logger_level = "INFO"

    def __init__(self):
        self.__setup_parser()
        self.__setup_args()
        super(CrawlerRunner, self).__init__()
    

    def __setup_default_argument(self):

        def is_valid_logrotateformat(logrotate_format):
            logrotate_format_regex = '^((?!{year}|{month}|{date}|{hour}|{minute}|{second}|{logfile}).).*$'
            # print(re.findall(logrotate_format_regex, logrotate_format))
            if re.match(logrotate_format_regex, logrotate_format):
                self.__parser.error("invalid logrotate format")
            else:
                return logrotate_format

        def is_valid_log_file(log_file):
            abs_log_file = os.path.abspath(os.path.expanduser(log_file))
            abs_log_file_dir = os.path.dirname(abs_log_file)
            if len(log_file) <= 0:
                return log_file
            if not os.path.exists(abs_log_file_dir):
                self.__parser.error("directory  " + str(abs_log_file_dir) + ' not exists\n')
                self.__parser.exit()
            elif os.path.exists(abs_log_file) and not os.path.isfile(abs_log_file):
                self.__parser.error("log file " + str(abs_log_file) + ' is not a file\n')
                self.__parser.exit()
            elif not os.access(abs_log_file_dir, os.F_OK and os.W_OK):
                self.__parser.error("no sufficent permission  for log file " + str(abs_log_file) + '\n')
                self.__parser.exit()
            else:
                return abs_log_file

    def __setup_parser(self):
        self.__parser = ArgumentParser(conflict_handler='resolve')
        self.__setup_default_argument()

    def __setup_args(self):
        self.__customized_args = dict()

    def __setup_spider(self):
        # map option from cli into crawler
        for arg in self.__customized_args.values():
            arg_name = arg['name']
            arg_alias = arg['alias']
            if arg_alias:
                arg_mapped_name = arg_alias
            else:
                arg_mapped_name = arg_name
            arg_mapped_value = getattr(self.parsed_args, arg_name, None)
            for _spider in self.spiders:
                setattr(_spider[0], arg_mapped_name, arg_mapped_value)
    
    def set_loglevel(self, level):
        self.logger_level = level

    def __setup_logger(self):
        config_log(self.logger_level)

    def __parse_args(self):
        if self.ARG_MODE:
            self.parsed_args = self.__parser.parse_args()
        else:
            self.parsed_args = None

    def add_argument(self, arg_name, arg_alias='', arg_help='', arg_default='', arg_ifmap=True, **kwargs):
        if 'abrev_name' in kwargs:
            abrev_name = kwargs['abrev_name']
            del kwargs['abrev_name']
            self.__parser.add_argument(abrev_name, arg_name, help=arg_help, default=arg_default, **kwargs)
        else:
            self.__parser.add_argument(arg_name, help=arg_help, default=arg_default, **kwargs)

        if arg_ifmap:
            cli_args = {arg_name: {'name': arg_name, 'alias': arg_alias}}
            self.__customized_args.update(cli_args)
        if not self.ARG_MODE:
            self.ARG_MODE = True

    def add_option(self, option_name, option_alias='', option_help='', option_default='', option_ifmap=True, **kwargs):
        prefix_name = '--' + str(option_name)
        if 'abrev_name' not in kwargs:
            self.__parser.add_argument(prefix_name, help=option_help, default=option_default, **kwargs)
        else:
            prefix_abrev_name = '-' + str(kwargs['abrev_name'])
            del kwargs['abrev_name']
            self.__parser.add_argument(prefix_abrev_name, prefix_name, help=option_help, default=option_default,
                                       **kwargs)
        if option_ifmap:
            cli_args = {option_name: {'name': option_name, 'alias': option_alias}}
            self.__customized_args.update(cli_args)
        if not self.ARG_MODE:
            self.ARG_MODE = True
    

    def run(self):
        self.__parse_args()
        self.__setup_logger()
        self.__setup_spider()
        self.logger.debug('RUNNER: ' + self.__class__.__module__ + "." + self.__class__.__qualname__)
        self.logger.debug("RUNNER ENV: { " + "args: " + str(self.parsed_args) + "," + " custom args:" + str(
            self.__customized_args) + " }")
        self.logger.debug("RUNNER START")
        self._proxy_runner = None
        if self.crontab:
            self._proxy_runner  = CrawlerCrontabRunner()
            for c in self.spiders:
                self._proxy_runner.add_crawler(c[0],c[1],self.crontab)
            self.register_signal_handler()
            self._proxy_runner.run()
        elif self.interval:
            self.register_signal_handler()
            while True:
                try:
                    back_spiders= copy(self.spiders)
                    self._proxy_runner = CrawlerRunner()
                    for c in back_spiders:
                        self._proxy_runner.add_crawler(c[0], c[1])
                except Exception:
                    self.logger.exception(traceback.print_exc)
                    continue
                except RecursionError:
                    self.logger.exception(traceback.print_exc)
                    continue
                time.sleep(self.interval)
                self._proxy_runner.run()
        else:
            self.register_signal_handler()
            self._proxy_runner = CrawlerRunner()
            for c in self.spiders:
                self._proxy_runner.add_crawler(c[0], c[1])
            self._proxy_runner.run()
        self.close()
    
    def close(self):
        logger.finalize_logger()
        self._proxy_runner.close()
        self.__class__.ARG_MODE = False
        self.__class__.crontab = None
        self.__class__.interval = None
