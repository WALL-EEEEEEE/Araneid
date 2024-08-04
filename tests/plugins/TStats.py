from copy import deepcopy
from araneid.stats import StatsCollector

class TStats(StatsCollector):
    stats_key = 'test'
    stats_value = 'test_value'

    def start_spider(self, spider, scraper):
        self.set_value(self.stats_key, self.stats_value)
        self.set_value(self.stats_key, self.stats_value, spider=spider)
    
    def close_spider(self, spider, scraper):
        return super().close_spider(spider)
