import logging

class BroadMiddleware(object):
    logger = logging.getLogger(__name__)

    def process_spider_input(self, response, spider):
        self.logger.info('process_spider_input in '+self.__class__.__qualname__)
   
    """
    def process_spider_exception(self, response, exception, spider):
        self.logger.info('process_spider_exception in '+self.__class__.__qualname__)
    """

    def process_start_reqeusts(self, start_requests, spider):
        pass
