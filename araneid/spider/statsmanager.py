from distutils.util import change_root
import logging
from typing import Set
from collections import ChainMap
from araneid.core.exception import ConfigError, NotConfigured,StatsNotFound, StatsError
from araneid.util._async import ensure_asyncfunction
from araneid.core import plugin as plugins
from araneid.stats import Stats, StatsCollector, ChainStats
from araneid.util._import import import_class

class StatsManager:
    logger = None

    def __init__(self, settings):
        self.logger = logging.getLogger(__name__)
        self.__stats_collector: Set = set()
        self._stats  = ChainStats()
        self._spider_stats = ChainStats()

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        inst = cls(settings)
        statses = inst.__load_plugin(settings=settings)
        for name, stats in statses.items():
            inst.add_stats(stats)
        return inst

    @classmethod
    def from_settings(cls, settings):
        inst = cls(settings)
        statses = inst.__load_plugin(settings)
        for name, stats in statses.items():
            inst.add_stats(stats)
        return inst

    def __load_plugin(self, settings):
        status_collector_plugins = plugins.load(plugins.PluginType.STATUS)
        status_collectors = dict()
        for plugin in status_collector_plugins:
            name = plugin.name
            status_collector = plugin.load()
            try:
                status_collectors[name] = status_collector.from_settings(settings)
            except NotConfigured as e:
                self.logger.warning("StatusCollector {name} is not configured, skipped load.")
                continue
            except Exception as e:
                raise StatsError("Error occurred in while loading StatusCollector {name}!") from e
            self.logger.debug(f'Loaded StatusCollector: {name}.')
        return status_collectors
    
    def add_stats(self, stats):
        assert isinstance(stats, StatsCollector)
        self.__stats_collector.add(stats)
        self._stats.chain(stats.stats)
        self._spider_stats.chain(stats.spider_stats)
    
    def get_stats(self, spider=None):
        if spider:
           spider_stats = self._spider_stats.get(f'spider_{id(spider)}', default={})
           stats = StatsCollector.from_dict(spider_stats)
        else:
           stats = self._stats
        return stats
    
    def get_value(self, key, default=None, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
           stats = self._stats 
        return stats.get(key, default=default, sep=sep)


    def set_value(self, key, value, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
            stats = self._stats
        stats.set(key,value, sep=sep)

    def inc_value(self, key, count=1, start=0, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
           stats = self._stats
        stats.set(key, stats.get(key, sep=sep, default=start)+count, sep=sep)


    def max_value(self, key, value, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
           stats = self._stats
        stats.set(key, max(stats.get(key, default=value, sep=sep), value), sep=sep)


    def min_value(self, key, value, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
           stats = self._stats
        stats.set(key, min(stats.get(key, default=value, sep=sep), value), sep=sep)
    
    async def close_spider(self, spider, scraper):
        for stats in self.__stats_collector:
            close_spider_fn = getattr(stats, 'close_spider', None)
            if not close_spider_fn:
               continue
            close_spider_fn = ensure_asyncfunction(close_spider_fn)
            await close_spider_fn(spider, scraper)

    async def start_spider(self, spider, scraper):
        for stats in self.__stats_collector:
            start_spider_fn = getattr(stats, 'start_spider', None)
            if not start_spider_fn:
               continue
            start_spider_fn = ensure_asyncfunction(start_spider_fn)
            await start_spider_fn(spider, scraper)

    def list_stats(self):
        return [ getattr(stats,'__module__', 'module')+'.'+getattr(getattr(stats, '__class__', object()),'__qualname__', '') for stats in self.__stats_collector]
    
    def clear_stats(self):
        self._stats.clear()
        self._spider_stats.clear()
    

