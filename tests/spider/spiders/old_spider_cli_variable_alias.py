from araneid.crawlers.default import DefaultCrawler
from araneid.util.annotation import CrawlerCliRunner


@CrawlerCliRunner
@CrawlerCliRunner.argument('urls', arg_alias='p_skey')
@CrawlerCliRunner.argument('parentTaskId', help='Task id of list, which will be reported with subtask', alias='__id')
class old_spider_cli_variable_alias(DefaultCrawler):

    def start_requests(self):
        self.logger.info(self.urls)

    def parse(self):
        pass