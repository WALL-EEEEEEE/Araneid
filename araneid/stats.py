import logging
from typing import ChainMap, Dict
from araneid import setting
from araneid import spider
from araneid.spider import Spider

class Stats:
    def __init__(self) -> None:
        self._stats__ = {}

    
    @classmethod
    def from_dict(cls, d):
        stats = cls()
        stats._stats__ = d
        return stats
    
    def get(self, key, sep='.', default=None):
        return self.__getitem__(key, sep=sep, default=default)
    
    def set(self, key, value, sep='.'):
        self.__setitem__(key, value, sep=sep)
    
    def __contains__(self, key, sep='.'):
        if isinstance(key, str):
            struct_keys  = key.split(sep)
        else:
            struct_keys  = [key]
        exists = True
        stats = self._stats__
        for struct_key in struct_keys:
            exists =  exists and (struct_key in stats)
        return exists
    
    def __getitem__(self, key, sep='.', default=None):
        if isinstance(key, str):
            struct_keys  = key.split(sep)
        else:
            struct_keys  = [key]
        stats = self._stats__
        for struct_key in struct_keys[:-1]:
            stats = stats.get(struct_key, {})
        return stats.get(struct_keys[-1], default)
    
    def __delitem__(self, key, sep='.'):
        if isinstance(key, str):
            struct_keys  = key.split(sep)
        else:
            struct_keys  = [key]
        stats = self._stats__
        for struct_key in struct_keys[:-1]:
            stats = stats.get(struct_key, {})
        del stats[struct_keys[-1]] 

    def __setitem__(self, key, value, sep='.'):
        if isinstance(key, str):
            struct_keys  = key.split(sep)
        else:
            struct_keys  = [key]
        stats = self._stats__
        for struct_key in struct_keys[:-1]:
            if struct_key not in stats:
               stats[struct_key] = {}
            stats = stats.get(struct_key)
        stats[struct_keys[-1]] = value
    
    def clear(self):
        self._stats__.clear()

class ChainStats(Stats):

    def __init__(self) -> None:
        self._stats__ = ChainMap() 
    
    def chain(self, stats: Stats):
        self._stats__ = ChainMap(*self._stats__.maps, stats)

class StatsCollector:
    logger = None 

    def __init__(self, settings, stats=None, spider_stats=None):
        self.settings = settings 
        self.logger = logging.getLogger(__name__)
        if not stats:
           stats = Stats()
        if not spider_stats:
           spider_stats = Stats()
        self._stats = stats
        self._spider_stats = spider_stats
    
    @property
    def stats(self):
        return self._stats
    
    @property
    def spider_stats(self):
        return self._spider_stats
    
    @classmethod
    def from_crawler(cls, spider: Spider):
        settings = spider.settings
        return cls(settings)

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)
    
    def get_value(self, key, default=None, spider=None, sep='.'):
        if spider:
           stats = self._spider_stats
           key = f'spider_{id(spider)}{sep}{key}'
        else:
           stats = self._stats 
        return stats.get(key, default=default, sep=sep)

    def get_stats(self, spider=None):
        if spider:
           stats = Stats.from_dict(self._spider_stats.get(f'spider_{id(spider)}', default={}))
        else:
           stats = self._stats
        return stats

    def set_value(self, key, value, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
            stats = self._stats
        stats.set(key,value, sep=sep)

    def set_stats(self, stats, spider=None):
        assert isinstance(stats, Stats)
        if spider:
            self._spider_stats[f'spider_{id(spider)}'] = stats._stats__
        else:
            self._stats = stats


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
    
    def clear_value(self, key, spider=None, sep='.'):
        if spider:
           key = f'spider_{id(spider)}{sep}{key}'
           stats = self._spider_stats
        else:
           stats = self._stats
        del stats[key]
 
    def clear_stats(self, spider=None):
        if spider:
           del self._spider_stats[f'spider_{id(spider)}']
        else:
           self._stats.clear()

    def start_spider(self, spider):
        raise NotImplementedError('StatusCollector method start_spider must be implemented!')

    def close_spider(self, spider):
        self.clear_stats(spider)
    
    def __str__(self) -> str:
        ptr='StatusCollector(stats={stats}, spider_stats={spider_stats})'.format(stats=self._stats, spider_stats=self._spider_stats)
        return ptr