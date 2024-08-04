from  araneid.core.spidermiddlewares import SpiderMiddleware
import logging

class DepthMiddleware(object):
    logger = logging.getLogger(__name__)

    def process_spider_input(self, response , spider):
        self.logger.info('process_spider_input in '+str(self.__class__.__qualname__))
    
    """
    def process_spider_exception(self, response, exception, spider):
        self.logger.info('process_spider_exception in '+str(self.__class__.__qualname__))
    """
