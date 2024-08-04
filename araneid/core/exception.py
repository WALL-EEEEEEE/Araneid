class StatsNotFound(Exception):
    pass
class StatsError(Exception):
    pass
class SlotError(Exception):
    pass
class SlotUnbound(SlotError):
    pass
class SlotNotFound(Exception):
    pass
class RouteError(Exception):
    pass
class RouterError(Exception):
    pass
class NotConfigured(Exception):
    pass
class ConfigError(Exception):
    pass
class IgnoreRequest(Exception):
    pass
class RouterNotFound(Exception):
    pass

class SchedulerError(Exception):
    pass
class SchedulerNotFound(SchedulerError):
    pass

class InvalidScheduler(SchedulerError):
    pass
class SchedulerRuntimeException(SchedulerError):
    pass
class SchedulerRuntimeException(Exception):
    pass
class DownloaderNotFound(Exception):
    pass
class InvalidDownloader(Exception):
    pass
class DownloaderError(Exception):
    pass
class DownloaderWarn(Exception):
    pass

class DownloaderMiddlewareError(Exception):
    pass
class DownloaderMiddlewareNotFound(DownloaderMiddlewareError):
    pass
class InvalidDownloaderMiddleware(DownloaderMiddlewareError):
    pass

class SpiderMiddlewareError(Exception):
    pass
class SpiderMiddlewareNotFound(SpiderMiddlewareError):
    pass
class InvalidSpiderMiddleware(SpiderMiddlewareError):
    pass
class InvalidCrawler(Exception):
    pass
class InvalidSpider(InvalidCrawler):
    pass
class SpiderNotFound(Exception):
    pass
class SpiderException(Exception):
    pass
class ParseException(SpiderException):
    pass
class StartException(SpiderException):
    pass
class RequestException(Exception):
    pass
class ParserError(Exception):
    pass
class InvalidParser(ParserError):
    pass
class ParserNotFound(ParserError):
    pass
class ParserUnbound(ParserError):
    pass
class StarterError(Exception):
    pass
class InvalidStarter(StarterError):
    pass
class StarterNotFound(StarterError):
    pass
class StaterUnbound(StarterError):
    pass
class InvalidItem(InvalidSpider):
    pass
class InvalidRequestErrback(RequestException):
    pass
class HttpRequestProxyError(RequestException):

    def __init__(self, request, exception=None) -> None:
        self.__request__ = request
        self.__exception__ = exception

    def __str__(self):
        return 'HttpRequest {request} proxy failed ( exception: {exception}) ! '.format(request=self.__request__,  exception=self.__exception__)

class PluginError(Exception):
    pass

class SettingError(Exception):
    pass

