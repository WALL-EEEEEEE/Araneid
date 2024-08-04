import asyncio
import pytest
import logging
from unittest.mock import Mock
from araneid.spider import Spider, Starter
from araneid.setting import settings as setting_loader
from araneid.core.plugin import PluginType
from araneid.core.signal import register, spider_closed
from araneid.spider import statsmanager
from araneid.spider.statsmanager import StatsManager
from araneid.stats import ChainStats, StatsCollector, Stats
from .plugins.TStats import TStats


logger = logging.getLogger(__name__)

@pytest.fixture()
def stats_collector():
    stats = StatsCollector.from_settings(settings={})
    yield stats
    stats.clear_stats()

@pytest.fixture()
def stats_manager():
    stats = StatsManager.from_settings(settings={})
    yield stats
    stats.clear_stats()

@pytest.fixture()
def stats():
    stats = Stats()
    yield stats
    stats.clear()

@pytest.fixture()
def chain_stats():
    stats = ChainStats()
    yield stats
    stats.clear()



async def timeout( coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)

def operators():
    def set_get_value(stats, spider=None):
        stats.set_value('total', 100, spider=spider)
        stats.set_value('dict.key1', 20, spider=spider)
        return  {'total': 100, 'dict.key1': 20, 'dict': {"key1": 20}}

    def inc_value(stats, spider=None):
        stats.inc_value('total', 1, start=0, spider=spider)
        stats.inc_value('total', 2, spider=spider)
        stats.inc_value('dict.total', 1, start=0, spider=spider)
        stats.inc_value('dict.total', 2, spider=spider)
        return  {'total': 3, 'dict.total': 3, 'dict': {"total": 3}}

    def max_value(stats, spider=None):
        stats.max_value('total', 3, spider=spider)
        stats.max_value('total', 2, spider=spider)
        stats.max_value('total', 5, spider=spider)
        stats.max_value('dict.total', 3, spider=spider)
        stats.max_value('dict.total', 2, spider=spider)
        stats.max_value('dict.total', 5, spider=spider)
        return  {'total': 5, 'dict.total': 5, 'dict': {'total': 5}}

    def min_value(stats, spider=None):
        stats.set_value('total', 1, spider=spider)
        stats.min_value('total', -1, spider=spider)
        stats.min_value('dict.total', 1, spider=spider)
        stats.min_value('dict.total', -1, spider=spider)
        return  {'total': -1, 'dict.total': -1, 'dict': {'total': -1}}

    operators = { key : value for key, value in locals().items()}
    return operators

def stats_operators():
    def set_and_get(stats):
        stats.set('test', 'value1')
        stats.set('dict.test',  'value2')
        assert stats.get('test') == 'value1'
        assert stats.get('dict.test') == 'value2'
        assert stats.get('dict') == {'test':'value2'}
        return {'test': 'value1', 'dict.test': 'value2', 'dict': {'test': 'value2'}}

    def dict_get_and_set(stats):
        stats['test'] = 'value1'
        stats['dict.test'] =  'value2'
        assert stats['test'] == 'value1'
        assert stats['dict.test'] == 'value2'
        assert stats['dict'] == {'test':'value2'}
        return {'test': 'value1', 'dict.test': 'value2', 'dict': {'test': 'value2'}}

    def dict_in(stats):
        stats['test'] = 'value1'
        assert 'test' in stats

    operators = { key : value for key, value in locals().items()}
    return operators


test_stats_collector_group= {
    **{operator_name : (operator_func, False) for operator_name, operator_func in operators().items()},
    **{operator_name+'_spider_stats' : (operator_func, True) for operator_name, operator_func in operators().items()}
}
test_stats_manager_group= {
    **{operator_name : (operator_func, False, False) for operator_name, operator_func in operators().items()},
    **{operator_name+'_spider_stats' : (operator_func, False, True) for operator_name, operator_func in operators().items()},
    **{operator_name+"_with_collector" : (operator_func, True, False) for operator_name, operator_func in operators().items()},
    **{operator_name+"_spider_stats_with_collector" : (operator_func, True, True) for operator_name, operator_func in operators().items()}
}

test_chain_stats_group= {
    **{operator_name : (operator_func, False) for operator_name, operator_func in stats_operators().items()},
    **{operator_name+"_with_chain" : (operator_func, True) for operator_name, operator_func in stats_operators().items()},
}
test_stats_group= {
    **{operator_name : operator_func for operator_name, operator_func in stats_operators().items()},
}

def mock_empty_spider(name='test_spider'):
    spider = Mock(spec=Spider)
    spider.name = name
    starter = Starter('default')
    starter.bind(lambda _: '', spider)
    spider.get_starters = Mock(return_value=[starter])
    spider.get_parsers= Mock(return_value=[])
    spider.get_start_starter = Mock(return_value=starter)
    return spider


@pytest.mark.parametrize("operator, enable_chain", list(test_chain_stats_group.values()), ids=list(test_chain_stats_group.keys()))
def test_chain_stats(operator, enable_chain, chain_stats, stats):
    if not enable_chain:
       operator(chain_stats)
    else:
        chain_stats.chain(stats)
        result = operator(stats)
        if result is not None:
            for key, value in  result.items():
                assert chain_stats.get(key) == value


@pytest.mark.parametrize("operator", list(test_stats_group.values()), ids=list(test_stats_group.keys()))
def test_stats(operator, stats):
    operator(stats)

@pytest.mark.parametrize("operator, spider_stats", list(test_stats_collector_group.values()), ids=list(test_stats_collector_group.keys()))
def test_stats_collector(operator, spider_stats, stats_collector):
    if spider_stats:
        spider = mock_empty_spider(name='test_spider')
    else:
        spider = None
    result = operator(stats_collector, spider)
    for key, value in result.items():
        assert stats_collector.get_value(key, spider=spider) == value
 

@pytest.mark.parametrize("operator, enable_collector, spider_stats", list(test_stats_manager_group.values()), ids=list(test_stats_manager_group.keys()))
def test_stats_manager(operator, enable_collector, spider_stats, stats_collector, stats_manager):
    if spider_stats:
       spider = mock_empty_spider(name='test_spider')
    else:
       spider = None

    if enable_collector:
       stats_manager.add_stats(stats_collector)
       result = operator(stats_collector, spider)
    else:
       result =  operator(stats_manager, spider)
    for key, value in result.items():
        assert stats_manager.get_value(key, spider=spider) == value
 


@pytest.mark.spider
@pytest.mark.asyncio
@pytest.mark.spider(name='test_spider')
@pytest.mark.entrypoints(entry_points={PluginType.STATUS: [TStats]}, mode='include')
async def test_spider_stats(spider, async_runner):
    stats = None
    spider_stats = None
    def __(signal, scraper, spider):
        nonlocal stats, spider_stats
        stats = spider.stats.get_value(TStats.stats_key)
        spider_stats = spider.stats.get_value(TStats.stats_key, spider=spider)

    register(spider_closed, __)
    spider_inst = spider()
    async_runner.add_spider(spider_inst)
    await timeout(async_runner.start())
    assert stats == TStats.stats_value
    assert spider_stats == TStats.stats_value