import asyncio
import signal 
import logging
import functools
from inspect import isclass
from datetime import datetime
from typing import Any, List, Type
from abc import ABC, abstractclassmethod, abstractmethod
from asyncron import Executor
from argparse import ArgumentParser
from araneid.setting import settings as settings_loader
from araneid.core.slot import Slot
from araneid.core.engine import Engine
from araneid.core.signal import set_signalmanager, SignalManager, get_signalmanager
from araneid.scraper import Scraper
from araneid.spider.spider import Spider
from araneid.util.cli import Argument, Option


class AutoCall(type):
    def __init__(cls,name,bases,dct):
      def auto__call__init__(self, *a, **kw):
        for base in cls.__bases__:
          base.__init__(self, *a, **kw)
        cls.__init__child_(self, *a, **kw)
      cls.__init__child_ = cls.__init__
      cls.__init__ = auto__call__init__
    
class SpiderArgsSupport(object):

    __spider_arg_parser = ArgumentParser(conflict_handler='resolve')
    __spider_args = dict()
    __parsed_spider_args = None
   
    @classmethod
    def parse_spider_args(cls, spider: Spider):
        spider_args_def =  getattr(spider, '__araneid_script_args', [])
        if not spider_args_def:
           return
        for def_arg in spider_args_def:
           if isinstance(def_arg, Argument):
               cls._add_argument_def(def_arg.name, def_arg.alias, def_arg.help, def_arg.default, **def_arg.kwargs)
           if isinstance(def_arg, Option):
               cls._add_option_def(def_arg.name, def_arg.alias, def_arg.help, def_arg.default, **def_arg.kwargs)
        if not cls.__parsed_spider_args:
           cls.__parsed_spider_args = cls.__spider_arg_parser.parse_args()
        spider_args = {}
        for arg in cls.__spider_args.values():
            arg_name = arg['name']
            arg_alias = arg['alias']
            if arg_alias:
                arg_mapped_name = arg_alias
            else:
                arg_mapped_name = arg_name
            arg_mapped_value = getattr(cls.__parsed_spider_args, arg_name, None)
            spider_args[arg_name] = {'name': arg_name, 'alias': arg_mapped_name, 'value': arg_mapped_value}
        return spider_args
 
    @classmethod
    def _add_argument_def(cls, arg_name, arg_alias='', arg_help='', arg_default='', arg_ifmap=True, **kwargs):
        if 'abrev_name' in kwargs:
            abrev_name = kwargs['abrev_name']
            del kwargs['abrev_name']
            cls.__spider_arg_parser.add_argument(abrev_name, arg_name, help=arg_help, default=arg_default, **kwargs)
        else:
            cls.__spider_arg_parser.add_argument(arg_name, help=arg_help, default=arg_default, **kwargs)

        if arg_ifmap:
            spider_arg = {arg_name: {'name': arg_name, 'alias': arg_alias}}
            cls.__spider_args.update(spider_arg)


    @classmethod
    def _add_option_def(cls, option_name, option_alias='', option_help='', option_default='', option_ifmap=True, **kwargs):
        prefix_name = '--' + str(option_name)
        if 'abrev_name' not in kwargs:
            cls.__spider_arg_parser.add_argument(prefix_name, help=option_help, default=option_default, **kwargs)
        else:
            prefix_abrev_name = '-' + str(kwargs['abrev_name'])
            del kwargs['abrev_name']
            cls.__spider_arg_parser.add_argument(prefix_abrev_name, prefix_name, help=option_help, default=option_default,
                                       **kwargs)
        if option_ifmap:
            spider_arg = {option_name: {'name': option_name, 'alias': option_alias}}
            cls.__spider_args.update(spider_arg)

class BaseRunner(SpiderArgsSupport, metaclass=AutoCall):
    logger = None 
    __slot__ = ['__spiders__']

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def start(self)-> Any:
        """启动爬虫

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError('start() method must be implemented by Runner')

    @abstractclassmethod
    async def create(cls)-> Any:
        """创建爬虫运行器

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError('async def create() method must be implemented by Runner')
 
    
    @abstractmethod
    def add_spider(self, spider:Type[Spider], starter=None)-> None:
        """添加爬虫

        Args:
            spider (Union[Type[Spider], AnyStr]): spider可以是一个:py:obj:`araneid.core.signal.Signal.EngineClose`

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError('add_spider() method must be implemented by Runner')
    
class AsyncRunner(BaseRunner):
    __spiders: List[Spider]
    __settings = None
    __signalmanager = None

    def __init__(self, **kwargs) -> None:
        self.__settings = kwargs.get('settings', None)
        self.__signalmanager = kwargs.get('signalmanager', None)
        self.__spiders = []
    
    @classmethod
    async def create(cls, **kwargs):
        instance = cls(**kwargs)
        if not instance.__signalmanager:
           signalmanager = await SignalManager.create()
        else:
           signalmanager = instance.__signalmanager
        set_signalmanager(signalmanager)
        engine = await Engine.create(settings=instance.__settings)
        scraper =  await Scraper.create(instance.__settings)
        slot = await Slot.create()
        scraper.bind(slot)
        await engine.add_slot(slot)
        instance.engine = engine
        instance.scraper = scraper
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda sig: asyncio.create_task(instance.shutdown(sig, loop)))
        loop.add_signal_handler(signal.SIGQUIT, lambda sig: asyncio.create_task(instance.shutdown(sig, loop)))
        return instance
    
    async def shutdown(self, sig, loop):
        self.logger.debug('caught {0}'.format(sig.name))
        tasks = [task for task in asyncio.Task.all_tasks() if task is not
                asyncio.tasks.Task.current_task()]
        list(map(lambda task: task.cancel(), tasks))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.debug('finished awaiting cancelled tasks, results: {0}'.format(results))
        loop.stop()
 
    async def start(self) -> None:
        for spider in self.__spiders:
            self.scraper.add_spider(spider)
        await asyncio.gather(self.engine.start(), self.scraper.start())


    def add_spider(self, spider_cls:Type[Spider], starter=None)-> None:
        assert isinstance(spider_cls, Spider) or issubclass(spider_cls, Spider), "spider argument must be an instance of Spider."
        if isinstance(spider_cls, Spider):
          spider_inst = spider_cls
        elif issubclass(spider_cls, Spider):
          spider_inst = Spider.create(spider_cls)
        if starter is not None:
          spider_inst.set_start_starter(starter)
        spider_args = self.parse_spider_args(spider_inst)
        spider_inst.metas['args'] = spider_args
        self.__spiders.append(spider_inst)

class AsyncCronRunner(BaseRunner):

    def __init__(self, **kwargs) -> None:
        self.settings = kwargs.get('settings', None)
        self.__spider_job_stats = {}
    
    @classmethod
    async def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.executor = await Executor.create()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, functools.partial(asyncio.ensure_future, instance.shutdown(signal.SIGTERM, loop)))
        loop.add_signal_handler(signal.SIGQUIT, functools.partial(asyncio.ensure_future, instance.shutdown(signal.SIGQUIT, loop)))
        return instance
    
    async def shutdown(self, sig, loop):
        print('caught {0}'.format(sig.name))
        tasks = [task for task in asyncio.Task.all_tasks() if task is not
                asyncio.tasks.Task.current_task()]
        list(map(lambda task: task.cancel(), tasks))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print('finished awaiting cancelled tasks, results: {0}'.format(results))
        loop.stop()
      
    async def start(self) -> None:
        await self.executor.start()
    
    async def _execute_spider(self, spider_cls, starter):
        assert isinstance(spider_cls, Spider) or (isclass(spider_cls) and issubclass(spider_cls, Spider)), f"{spider_cls} must be a instance or subclass of Spider"
        if isinstance(spider_cls, Spider):
          spider_inst = spider_cls
        else:
          spider_inst = Spider.create(spider_cls)
        if starter is not None:
           spider_inst.set_start_starter(starter)
        spider_inst.metas['args'] = self.parse_spider_args(spider_inst)
        spider_name = spider_inst.name
        signalmanager = await SignalManager.create()
        set_signalmanager(signalmanager)
        engine = await Engine.create(settings=self.settings)
        scraper =  await Scraper.create(self.settings)
        slot = await Slot.create()
        scraper.bind(slot)
        await engine.add_slot(slot)
        scraper.add_spider(spider_inst)
        stats = self.__spider_job_stats[spider_name]
        last= "" if not stats.get("last_start", None) or not stats.get("last_end", None)  else  f'{int(stats["last_end"].timestamp() - stats["last_start"].timestamp())}s ([{stats["last_start"].strftime("%Y-%m-%d %H:%M:%S")}] - [{stats["last_end"].strftime("%Y-%m-%d %H:%M:%S")}])'   
        self.logger.debug(f'Spider {spider_name} start (crontab: {stats.get("crontab", "")}, last: {last}, times: {stats.get("times", 0)})')
        stats['last_start'] = datetime.now()
        stats['times'] = stats.get('times', 0) + 1
        await asyncio.gather(scraper.start(), engine.start())
        stats['last_end'] = datetime.now()

    def obsolete_crontab_sytle_convert(self, crontab_expr):
        if len(crontab_expr.split(' ')) >= 7:
           old_crontab_expr = crontab_expr
           crontab_expr = ' '.join(old_crontab_expr.split(' ')[1:-1])
           self.logger.warning(f'"{old_crontab_expr}" -> "{crontab_expr}" (crython crontab style is deprecated, please change it to standard crontab expression!)')
        return crontab_expr
    

    def add_spider(self, spider_cls:Type[Spider], expression, starter=None)-> None:
        assert isinstance(spider_cls, Spider) or issubclass(spider_cls, Spider), "spider argument must be an instance of Spider."
        assert expression is not None and isinstance(expression, str), "expression must be a valid crontab expression string"
        spider_name = spider_cls.name
        if spider_name in self.__spider_job_stats:
           self.logger.warning(f'Spider {spider_name} had been scheduled.')
           return
        expression = self.obsolete_crontab_sytle_convert(expression)
        self.__spider_job_stats[spider_name] = {'crontab': expression, 'last_start': None, 'last_end': None, 'times': 0}
        self.logger.debug(f'Spider {spider_cls.name} is scheduled (crontab: {expression}).')
        asyncio.ensure_future(self.executor.execute(expression, self._execute_spider, args=(spider_cls, starter), at_start=True))