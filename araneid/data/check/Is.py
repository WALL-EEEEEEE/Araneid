from typing import Optional
from .rule import Rule, RuleException
from ..item import Field


class NotEmptyRule(Rule):
    __message__ = 'must be not empty'

    def check(self, object)-> Optional[RuleException]:
        error: Optional[RuleException] = None
        if not isinstance(object, Field):
           return error
        field_name = object.name
        field_value = object.value
        if field_value is None:
           error_info = f'{field_name}: {self.__message__}'
           error = self.newError(error_info)
        return error

class IntRule(Rule):
    __message__ = 'must be a int'

    def check(self, object)-> Optional[RuleException]:
        if not isinstance(object, Field):
           return  None
        field_name = object.name
        field_value = object.value
        error = None
        if not isinstance(field_value, int):
           error_info = f'{field_name}: {self.__message__}'
           error = self.newError(error_info)
        return error


class DictRule(Rule):
    __message__ = 'must be a dict'

    def check(self, object)-> Optional[RuleException]:
        if not isinstance(object, Field):
           return  None
        field_name = object.name
        field_value = object.value
        error = None
        if not isinstance(field_value, dict):
           error_info = f'{field_name}: {self.__message__}'
           error = self.newError(error_info)
        return error


class StringRule(Rule):
    __message__ = 'must be a string'

    def check(self, object)-> Optional[RuleException]:
        if not isinstance(object, Field):
           return  None
        field_name = object.name
        field_value = object.value
        error = None
        if not isinstance(field_value, str):
           error_info = f'{field_name}: {self.__message__}'
           error = self.newError(error_info)
        return error

class TextRule(Rule):
    __message__ = 'must be a text (bytes or string or bytearray)'

    def check(self, object)-> Optional[RuleException]:
        if not isinstance(object, Field):
           return  None
        field_name = object.name
        field_value = object.value
        error = None
        if not isinstance(field_value, (str, bytes, bytearray)):
           error_info = f'{field_name}: {self.__message__}'
           error = self.newError(error_info)
        return error

class UrlRule(Rule):
    pass

class FileRule(Rule):
    pass


NotEmpty = NotEmptyRule()
Required = NotEmpty
Int = IntRule()
Url = UrlRule()
Dict = DictRule()
String = StringRule()
File = FileRule()
Text = TextRule()
