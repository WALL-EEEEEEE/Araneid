import logging
import types
import araneid
from araneid.util._import import import_class
from types import ModuleType
from importlib.abc import MetaPathFinder, SourceLoader
from importlib.machinery import PathFinder, ModuleSpec
from importlib.util import find_spec
from os.path import join, exists, isdir, dirname
import sys
from importlib import import_module

logger = logging.getLogger(__name__)
__compat_class__ =  {
    'araneid.util.deduplicate': 'araneid.util.filter.deduplicate',
    'araneid.spider.Rule': 'araneid.spider.rule.Rule',
    'araneid.spider.XpathRule': 'araneid.spider.rule.XpathRule',
    'araneid.spider.CssRule': 'araneid.spider.rule.CssRule',
    'araneid.crawlers.default.DefaultCrawler': 'araneid.compat.crawlers.default.DefaultCrawler',
    'araneid.crawlers.socket.SocketCrawler': 'araneid.compat.crawlers.socket.SocketCrawler',
    'araneid.crawlers.websocket.WebSocketCrawler': 'araneid.compat.crawlers.websocket.WebSocketCrawler',
    'araneid.basic.network.http.HttpRequest': 'araneid.compat.basic.network.requests.http.HttpRequest',
    'araneid.basic.network.socket.SocketRequest': 'araneid.compat.basic.network.requests.socket.SocketRequest',
    'araneid.basic.network.websocket.WebSocketRequest': 'araneid.compat.basic.network.requests.websocket.WebSocketRequest',
    'araneid.basic.network.requests.http.HttpRequest': 'araneid.compat.basic.network.requests.http.HttpRequest',
    'araneid.basic.network.requests.socket.SocketRequest': 'araneid.compat.basic.network.requests.socket.SocketRequest',
    'araneid.basic.network.requests.websocket.WebSocketRequest': 'araneid.compat.basic.network.requests.websocket.WebSocketRequest',

}
__compat_module__ = {
   'araneid.crawlers': 'araneid.compat.crawlers',
   'araneid.crawlers.default': 'araneid.compat.crawlers.default',
   'araneid.crawlers.socket': 'araneid.compat.crawlers.socket',
   'araneid.crawlers.websocket': 'araneid.compat.crawlers.websocket',
   'araneid.basic': 'araneid.compat.basic',
   'araneid.basic.network': 'araneid.compat.basic.network',
   'araneid.basic.network.http': 'araneid.compat.basic.network.http',
   'araneid.basic.network.socket': 'araneid.compat.basic.network.socket',
   'araneid.basic.network.websocket': 'araneid.compat.basic.network.websocket',
   'araneid.basic.network.requests': 'araneid.compat.basic.network',
   'araneid.basic.network.requests.http': 'araneid.compat.basic.network.http',
   'araneid.basic.network.requests.socket': 'araneid.compat.basic.network.socket',
   'araneid.basic.network.requests.websocket': 'araneid.compat.basic.network.websocket',
 
}
__compat_prefix__ = dirname(araneid.__file__)

class CompatFinder(object):

    def find_spec(self, fullname, path=None, target=None):
        if fullname not in __compat_class__ and fullname not in __compat_module__:
            return None
        if fullname in __compat_class__:
            path = join(__compat_prefix__,*__compat_class__[fullname].split('.')[1:-2],__compat_class__[fullname].split('.')[-2]+'.py' )
            if not exists(path):
                path = join(__compat_prefix__,*__compat_class__[fullname].split('.')[1:-1], '__init__.py')
        else:
            path = join(__compat_prefix__,*__compat_module__[fullname].split('.')[1:-1], '__init__.py')
        return ModuleSpec(fullname, self, origin=path)


    def create_module(self, spec):
        name = spec.name
        path = spec.origin
        module = ModuleType(name)
        with open(path, 'r+') as reader:
            code = reader.read()
            exec(code, module.__dict__)
        if name in __compat_module__:
            module.__package__ = name
            module.__path__ = []
        else:
            cls = __compat_class__[name].split('.')[-1]
            module = module.__dict__[cls]
        return module
    
    def exec_module(self, module):
        pass

sys.meta_path.append(CompatFinder())

