import warnings
from araneid.core.exception import InvalidItem
from araneid.item import *
from .spider import *


def item(name, rule=None, type=None, require=True):
    warnings.warn(
            "@item is deprecated, use @field instead",
            DeprecationWarning
    )
    return field(name, type=type, require=require)

def field(name, type = Type.TEXT, require=True):
    field = Field(name=name, type=type, require=require)
    def __(fn):
        if not isinstance(fn, Parser):
           raise InvalidItem(
               fn.__qualname__ + ' is not a valid Parser, Item can\'t live without a Parser')
        else:
            fn.add_field(field)
        return fn
    return __

def starter(name=None):
    starter = Starter(name)
    def __(fn):
        starter.bind(fn)
        return starter
    return __

def parser(name=None, url=None, regex=None, item=None):
    parser = Parser(name, url, regex, item)
    def __(fn):
        parser.bind(fn)
        return  parser
    return __
