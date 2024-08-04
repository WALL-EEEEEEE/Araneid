import logging

class HttpProxyDownloaderMiddleware(object):
    logger = logging.getLogger(__name__)

    def process_request(self, request, spider):
        self.logger.info('process_request in '+str(self.__class__.__qualname__))