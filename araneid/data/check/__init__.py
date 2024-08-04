import inspect
import itertools
import logging
from abc import abstractmethod
from typing import Any, Dict, List, Optional
from araneid.core import plugin as plugins
from araneid.core.exception import PluginError
from .rule import RuleException
class CheckException(RuleException):
    pass

class CheckerInvalid(Exception):
    pass

class CheckerError(Exception):
    pass
class Checker:
    errors: Dict[str, List[str]]

    def __init__(self) -> None:
        self.errors = {}

    @abstractmethod
    def order(self):
        raise NotImplementedError
    
    @abstractmethod
    def check(self, object:Any) -> CheckException:
        raise NotImplementedError
    
    @abstractmethod
    def support(self, object: Any) -> bool:
        raise NotImplementedError


class CheckerManager:
    logger = None
    __checkers__: Dict

    def __init__(self) -> None:
        self.__checkers__: Dict[str, Checker] = {}
        self.logger = logging.getLogger(__name__)
    
    def load_plugin(self):
        checker_plugins = plugins.load(plugins.PluginType.SCHEMA)
        checkers = dict()
        for plugin in checker_plugins:
            name = plugin.name
            try:
                checker = plugin.load()
                self.register(checker)
            except Exception as e:
                self.logger.exception(e)
                raise PluginError(f"Error occurred in while loading Checker {name}!") from e
            self.logger.debug(f'Loaded Checker: {name}.')
        return checkers 
    
    def __contains__(self, name_or_checker):
        assert isinstance(name_or_checker, (str,)) or (inspect.isclass(name_or_checker) and  issubclass(name_or_checker, Checker))
        if isinstance(name_or_checker, str):
           condition = lambda checker: checker.name == name_or_checker
        else:
           condition = lambda checker:  isinstance(checker, name_or_checker)

        for _, checkers in self.__checkers__.items():
            for _, checker in checkers.items():
                if condition(checker):
                   return True
        return False


    def register(self, checker: Checker):
        assert issubclass(checker, Checker)
        try:
            checker_inst: Checker= checker()
        except Exception as e:
            raise CheckerError from e
        name = checker_inst.__class__.__qualname__
        order = checker_inst.order()
        order_checkers = self.__checkers__.get(order, {})
        order_checkers[name] = checker_inst
        self.__checkers__[order] = order_checkers
        self.__checkers__ = dict(sorted(self.__checkers__.items()))
        

    def check(self, obj: object) -> Optional[CheckException]:
        error: Optional[CheckException] = None
        for _, checkers in self.__checkers__.items():
            supported_checkers = filter(lambda checker: checker.support(obj), checkers.values())
            for checker in supported_checkers:
                result = checker.check(obj)
                if result:
                   error = result
                   break
        return error

checkers = CheckerManager()

def register(checker):
    checkers.register(checker)
    return checker

def set_checkermanager(_signalmanager):
    global checkers
    checkers = _signalmanager

def get_checkermanager():
    global checkers
    checkers.load_plugin()
    return checkers