import logging
from araneid.core.exception import RouteError, RouterError, NotConfigured
from araneid.spider.spider import Parser, Starter
from araneid.core import plugin as plugins

class RouterManager(object):
    logger = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('RouterManager init.')
        self.__routers= []

    @classmethod
    def from_settings(cls, settings):
        inst = cls()
        routers = inst.__load_plugin(settings)
        for _, router in routers.items():
            inst.add_router(router)
        return inst
    
    def __load_plugin(self, settings):
        router_plugins = plugins.load(plugins.PluginType.ROUTER)
        routers = dict()
        for plugin in router_plugins:
            name = plugin.name
            router = plugin.load()
            try:
                if hasattr(router, 'from_settings'):
                    routers[name] = router.from_settings(settings)
                else:
                    routers[name] = router()
            except NotConfigured as e:
                self.logger.warning("Router {name} is not configured, skipped load.")
                continue
            except Exception as e:
                raise RouterError("Error occurred in while loading Router {name}!") from e
            self.logger.debug(f'Loaded Router: {name}.')
        return routers
    
    def add_router(self, router):
        self.__routers.append(router)

    def list_router(self):
        return [ getattr(router,'__module__', 'module')+'.'+getattr(getattr(router, '__class__', object()),'__qualname__', '') for router in self.__routers]
    
    def add_parser_route(self, rule, parser):
        assert isinstance(parser, Parser), "Target for add_parser_route must be a Parser!"
        for router in self.__routers:
            if not hasattr(router,'add_parser_route'):
                continue
            router.add_parser_route(rule, parser)

    def add_starter_route(self, rule, starter):
        assert isinstance(starter, Starter), "Target for add_start_route must be a Starter!"
        for router in self.__routers:
            if not hasattr(router,'add_starter_route'):
                continue
            router.add_starter_route(rule, starter)
    
    def starter_route(self, rule):
        for router in self.__routers:
            target = router.starter_route(rule)
            if not target:
                continue
            if not isinstance(target, Starter):
                raise RouteError('Route target for rule {rule} is not a starter.'.format(rule=rule))
            return target
    
    def parser_route(self, rule):
        for router in self.__routers:
            target = router.parser_route(rule)
            if not target:
                continue
            if not isinstance(target, Parser):
                raise RouteError('Route target for rule {rule} is not a parser.'.format(rule=rule))
            return target