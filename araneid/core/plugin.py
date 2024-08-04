import inspect
from typing import Any, List
try:
   from setuptools._vendor.importlib_metadata import EntryPoint as __EntryPoint
except:
   __EntryPoint  = None
from importlib_metadata import entry_points, EntryPoint as _EntryPoint
if __EntryPoint is not None:
   EntryPoint = (__EntryPoint, _EntryPoint)
else:
   EntryPoint = _EntryPoint

from enum import Enum

registered_plugins = set() 
disabled_plugins = set()

class PluginType(Enum):
    DOWNLOADER = 'araneid.downloader'
    SCHEDULER = 'araneid.scheduler'
    DOWNLOADMIDDLEWARE = 'araneid.downloadmiddleware'
    SPIDERMIDDLEWARE  = 'araneid.spidermiddleware'
    EXTENSION = 'araneid.extension'
    SETTING = 'araneid.setting'
    SCRIPT = 'araneid.script'
    RUNNER = 'araneid.runner'
    STATUS = 'araneid.status'
    SPIDER = 'araneid.spider'
    ROUTER = 'araneid.spider.router'
    PARSER = 'araneid.spider.parser'
    STARTER = 'araneid.spider.starter'
    ITEM = 'araneid.item'
    SCHEMA = 'araneid.schema'

class PluginEntry:
    name: str
    value: Any
    group: str

    def __init__(self, name, value, group='None') -> None:
        self.name = name
        self.value = value
        self.group = group

    @classmethod
    def from_class(cls, plugin_cls, name='', group=''):
        assert inspect.isclass(plugin_cls)
        if not name:
           name = plugin_cls.__name__
        pe = cls(name, plugin_cls, group=group)
        return pe

    @classmethod
    def from_entrypoint(cls, entrypoint):
        #assert isinstance(entrypoint, EntryPoint)
        pe = cls(entrypoint.name, entrypoint, group=entrypoint.group)
        return pe

    @classmethod
    def from_schema(cls, schema, name='', group=''):
        assert isinstance(schema, str)
        assert ':' in schema, 'schema must be module:class format, example: package.test:TestClass'
        if not name:
           name = schema.split(':')[-1]
        pe = cls(name, schema, group=group)
        return pe
 
    
    def __eq__(self, _o: object) -> bool:
        if not isinstance(_o, self.__class__):
           return False
        ischema = ''
        if isinstance(self.value , EntryPoint):
           ischema = self.value.value
        elif inspect.isclass(self.value):
           ischema = self.value.__module__+':'+ self.value.__name__
        elif isinstance(self.value, str):
           ischema = self.value
        oschema  = ''
        if isinstance(_o.value , EntryPoint):
           oschema = _o.value.value
        elif inspect.isclass(_o.value):
           oschema = _o.value.__module__+':'+ _o.value.__name__

        elif isinstance(_o.value, str):
           oschema = _o.value

        if oschema and ischema and oschema == ischema:
            return True
        return False

    def __hash__(self) -> int:
        schema = ''
        if isinstance(self.value , EntryPoint):
           schema = self.value.value
        elif inspect.isclass(self.value):
           schema = self.value.__module__+':'+ self.value.__name__
        elif isinstance(self.value, str):
           schema = self.value
        if schema:
           return hash(schema)
        return id(self)

    def load(self):
        if isinstance(self.value, EntryPoint):
            return self.value.load()
        return  self.value

supported_plugin_types: str = ','.join([ str(plugin_type) for plugin_type in PluginType])


def __load_entry_points(plugin_type: PluginType):
    eps = entry_points().get(plugin_type.value, [])
    return [ PluginEntry.from_entrypoint(entrypoint) for entrypoint in  eps]

def __load_register_plugins(plugin_type: PluginType):
    return list(filter(lambda plugin: plugin.group  == plugin_type, registered_plugins))

def list_active_plugins(plugin_types: List[PluginType] = None):
    assert plugin_types is None or isinstance(plugin_types, list)
    if not plugin_types:
       plugin_types = [ plugin_type.value for plugin_type in PluginType]
    else:
       plugin_types = [ plugin_type.value for plugin_type in plugin_types  if plugin_type in PluginType]
    plugins = [ PluginEntry.from_entrypoint(ep) for group, group_eps in  entry_points().items() if group in plugin_types for ep in group_eps]
    return plugins

def register(type: PluginType, plugin):
    assert type in PluginType
    assert inspect.isclass(plugin) or isinstance(plugin, EntryPoint) or isinstance(plugin, PluginEntry)
    global registered_plugins
    if inspect.isclass(plugin):
       plugin_entry =  PluginEntry.from_class(plugin, group=type.value)
    elif isinstance(plugin, EntryPoint):
       plugin_entry = PluginEntry.from_entrypoint(plugin)
    else:
       plugin_entry = plugin
    registered_plugins.add(plugin_entry)

def disable(type: PluginType, plugin):
    assert type in PluginType
    assert inspect.isclass(plugin) or isinstance(plugin, EntryPoint) or isinstance(plugin, PluginEntry) or isinstance(plugin, str)
    global disabled_plugins
    if inspect.isclass(plugin):
       plugin_entry = PluginEntry.from_class(plugin, group=type.value)
    elif isinstance(plugin, str):
       plugin_entry = PluginEntry.from_schema(plugin, group=type.value)
    elif isinstance(plugin, EntryPoint):
       plugin_entry = PluginEntry.from_entrypoint(plugin, group=type.value)
    else:
       plugin_entry = plugin
    disabled_plugins.add(plugin_entry)

def clear():
    global registered_plugins, disabled_plugins
    registered_plugins.clear()
    disabled_plugins.clear()


def load(plugin_type: PluginType) -> object:
    assert plugin_type in PluginType, f"Plugin type {plugin_type} is not support, it only support {supported_plugin_types}"
    ep_plugins  = __load_entry_points(plugin_type)
    rg_plugins = __load_register_plugins(plugin_type)
    plugins = ep_plugins + rg_plugins
    plugins = set([ plugin for plugin in plugins if plugin not in disabled_plugins  ])
    return  plugins