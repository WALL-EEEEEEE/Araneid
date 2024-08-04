import logging
import asyncio
from araneid.util._async import ensure_asyncfunction
from araneid.core import plugin as plugins
from araneid.core.exception import NotConfigured

class InvalidExtension(Exception):
    pass
class ExtensionError(Exception):
    pass

class ExtensionManager(object):
    logger = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('ExtensionManager init.')
        self.__extensions = {}
    

    @classmethod
    async def create(cls, settings = None):
        instance = cls()
        plugin_extensions = await instance.__load_plugin(settings)
        for _, extensions in plugin_extensions.items():
            for name, extension in extensions.items():
                instance.add_extension(name, extension)
        return instance
   
    async def __load_plugin(self, settings):
        extension_plugins = plugins.load(plugins.PluginType.EXTENSION)
        extensions = dict()
        for plugin in extension_plugins:
            name = plugin.name
            try:
                extension = plugin.load()
                if hasattr(extension, 'from_settings'):
                    inst_extension = await extension.create(settings)
                else:
                    inst_extension = await extension.create()
                try:
                    order = float(inst_extension.order())
                except ValueError:
                   raise InvalidExtension(f'Extension {name} order method must return a number!') 
                order_extensions = extensions.get(order, {})
                order_extensions[name] = inst_extension
                extensions[order] = order_extensions
            except NotConfigured as e:
                self.logger.warning(f"Extension {name} is not configured, skipped load.")
                continue
            except Exception as e:
                raise ExtensionError(f"Error occurred in while loading Extension {name}!") from e
            self.logger.debug(f'Loaded Extension: {name}.')
        return dict(sorted(extensions.items()))
    
    def add_extension(self, name, extension):
        self.__extensions[name] = extension

    
    
    async def close(self):
        wait_closes = []
        for extension in self.__extensions.values():
            if getattr(extension, 'close', None) != None:
               async_close = ensure_asyncfunction(extension.close)
               wait_closes.append(async_close())
        if wait_closes:
           await asyncio.gather(*wait_closes)
        self.logger.debug('ExtensionManager closed.')
