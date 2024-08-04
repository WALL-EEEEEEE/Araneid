from araneid.crawlers.default import DefaultCrawler
from araneid.util.annotation import CrawlerCliRunner


@CrawlerCliRunner
@CrawlerCliRunner.argument('urls')
@CrawlerCliRunner.argument('parentTaskId')
class old_spider_cli_variable(DefaultCrawler):

    def start_requests(self):
        self.logger.info(self.urls)

    def parse(self):
        pass