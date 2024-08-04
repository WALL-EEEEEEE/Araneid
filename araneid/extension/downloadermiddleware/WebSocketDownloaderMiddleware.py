import logging
from araneid.downloader.aiowebsocket import WebSocketClose
from araneid.util._async import ensure_asyncfunction

class WebSocketDownloaderMiddleware(object):
    logger = logging.getLogger(__name__)

    async def process_exception(self, request, exception, spider):
        # process WebSocketClose exception
        self.logger.info(request)
        self.logger.info(exception)
        if isinstance(exception, WebSocketClose):
            on_close = request.on_close
            if on_close:
                return await ensure_asyncfunction(on_close)(request, spider)
            self.logger.info("{request} has closed.".format(request))
            
    