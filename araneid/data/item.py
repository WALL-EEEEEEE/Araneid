from abc import ABCMeta, abstractmethod
from copy import deepcopy
from enum import IntEnum, auto
from typing import Any, List

class Type(IntEnum):
    INT = auto()
    STRING = auto()
    TEXT = auto()
    FILE = auto()
    URL = auto()
    DICT = auto()

class FieldNotFound(Exception):
    pass


class Field(metaclass=ABCMeta):
    name : str
    type: Type
    value: Any
    require: bool
    desc: str

    def __init__(self, type: Type=Type.TEXT, default: Any = None, require: bool = False, name: str = None, desc="") -> None:
        self.name = name
        self.type = type
        self.value = default
        self.require = require
        self.desc = desc
    
    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, Field) and self.name == __o.name and self.type == __o.type  and self.require == __o.require and self.value == __o.value

    def __str__(self):
        return f'Field(name={self.name}, value={self.value}, type={self.type}, require={self.require})'
    
    def copy(self):
        cls = self.__class__
        return cls(self.name, self.value, self.type, self.require)



class ItemMeta(type):

    def __new__(cls, name, bases, attrs):
        if  name == 'BaseItem':
            return type.__new__(cls, name, bases, attrs)
        fields = dict()
        for k, v in attrs.items():
            if not isinstance(v, Field):
               continue
            v.name = k
            fields[k] = v
        for name in fields.keys():
            attrs.pop(name)
        attrs['__define_fields__'] = fields 
        return type.__new__(cls, name, bases, attrs)

class BaseItem(metaclass=ItemMeta):
    name: str

    def __init__(self, name=None) -> None:
        self.__fields__ = deepcopy(self.__define_fields__)
        if not name:
           name = self.__class__.__qualname__
        self.name = name

    @property
    def fields(self):
        return list(self.__fields__.values())
    
    def add_field(self, field: Field):
        assert isinstance(field, Field)
        self.__fields__[field.name] = field
    
    def get(self, key, default=None):
        if key not in self.__fields__:
           return default
        return self.__fields__[key].value
    
    def update(self, item):
        assert isinstance(item, self.__class__)
        fields: List[Field] = item.fields
        for field in fields:
            self.__fields__[field.name] = field

    def __getitem__(self, key):
       if key not in self.__fields__:
          raise FieldNotFound(f"Field {key} not found in Item {self.name}!")

       return self.__fields__[key]

    def __setitem__(self, key, value):
       if key not in self.__fields__:
          raise FieldNotFound(f"Field {key} not found in Item {self.name}!")
       if not isinstance(value, Field):
          self.__fields__[key].value =  value
       else:
          self.__fields__[key] = value

    def __iter__(self):
       for field in self.__fields__.values():
           yield field
    
   
class Item(BaseItem):

    def to_dict(self):
        d = {}
        for name, field in self.__fields__.items():
            d[name]  = field.value
        return d
    
    def __str__(self) -> str:
        ptr = f'Item(name={self.name}, fields={self.to_dict()})'
        return ptr
