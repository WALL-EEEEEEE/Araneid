import math
from pprint import pformat
from .core import plugin as plugins
from .core.exception import PluginError, SettingError


def merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                # replace
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a

def __load_plugin():
    global settings
    setting_plugins = plugins.load(plugins.PluginType.SETTING)
    setting_plugins = { plugin.name: plugin.load() for plugin in setting_plugins}
    sorted_setting_plugins = sorted(setting_plugins.items(), key=lambda item: getattr(item[1], 'order', math.inf))
    for name, plugin in sorted_setting_plugins:
        settings.add_setting_plugin(name, plugin)
    
class Setting:

    def __init__(self) -> None:
        self._modules = {}
        self._settings = {}

    def __load_setting_from_module(self, module):
        setting = { attr: getattr(module, attr) for attr in dir(module) if '__' not in attr and 'order' != attr }
        return setting

    def from_dict(settings: dict):
        pass
    
    def add_setting_dict(self, name, dict):
        pass

    def add_setting_plugin(self, name, plugin):
        try:
            setting = self.__load_setting_from_module(plugin)
        except Exception  as e:
            raise SettingError(f'Falied to load setting from setting plugin {name}!')  from e
        self._modules[name] = plugin
        self._settings = merge(self._settings, setting)
    
    def __getitem__(self, key):
        return self._settings.get(key, None)
    
    def get(self, key, default=None):
        return self._settings.get(key, default)

        
    
    def __str__(self) -> str:
        return pformat(self._settings)
        


settings = Setting()
__load_plugin()