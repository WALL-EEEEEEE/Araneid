import math
from typing import Dict, List, Optional
from araneid.data.check import Checker, CheckException, get_checkermanager, register as checker_register
from araneid.data.check.rule import Rule
from araneid.data.item import Item 

class Schema(Checker):
    pass

class ItemFieldSchemaCheckerMeta(type):
    def __new__(cls, name, bases, attrs):
        rules = dict()
        for k, v in attrs.items():
            if not isinstance(v, Rule):
               continue
            rules[k] = v
        for name in rules.keys():
            attrs.pop(name)
        attrs['__rules__'] = rules# 保存属性和列的映射关系
        return type.__new__(cls, name, bases, attrs)


class ItemFieldSchemaChecker(Schema, metaclass=ItemFieldSchemaCheckerMeta):

    def check(self, item: Item)-> Optional[CheckException]:
        assert isinstance(item, Item)
        rules: Dict[str, Rule] = getattr(self, '__rules__')
        errors :List[str] =[]
        for field in item:
            rule: Optional[Rule] = rules.get(field.name)
            if not rule:
                continue
            error = rule.check(field)
            if not error:
               continue
            errors.append(str(error))
        if not errors:
           return None
        errors_str = ', '.join([ str(error) for error in errors])
        return CheckException(f'{item.__class__.__qualname__} {{ {errors_str} }}') 

 
class ItemSchemaChecker(ItemFieldSchemaChecker):

    @property
    def rules(self):
        return self.__rules__.values()

    def order(self):
        return math.inf

    def support(self, item: Item) -> bool:
        return True

ItemSchema = ItemSchemaChecker
    

def register(schema: Schema):
    assert issubclass(schema, Schema)
    return checker_register(schema)

schemas = get_checkermanager()