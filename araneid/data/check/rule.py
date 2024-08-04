from typing import Dict, Optional

class RuleException(Exception):

    def __init__(self, message) -> None:
        self.__message =  message

    def __str__(self) -> str:
        return self.__message

class RuleErrors:

    def Error(self, message):
        self.__message__ = message
        return self
    
    @classmethod
    def newError(cls, message):
        return RuleException(message)

class Rule(RuleErrors):

    @property
    def name(self):
        return  self.__class__.__name__

    def __init__(self) -> None:
        super().__init__()

    def check(cls, object) -> Optional[RuleException]: 
        pass

    def __add__(self, others):
        pass

    def __not__(self):
        pass

    def __or__(self, other):
        assert isinstance(other, Rule)
        group = RuleGroup()
        return group | self | other 
    
    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, Rule) and self.name == __o.name


class RuleGroup(Rule):
    __rules__: Dict[str, Rule]

    @property
    def name(self):
        name = '|'.join(self.__rules__.keys())
        return f'{name}'

    def __init__(self) -> None:
        super().__init__()
        self.__rules__ = {}

    def __or__(self, other):
        assert isinstance(other, (Rule, RuleGroup))
        if isinstance(other, Rule):
           self.__rules__[other.__class__.__name__] = other
        elif isinstance(other, RuleGroup):
           rules = getattr(other, '__rules__')
           self.__rules__.update(**rules)
        return self
    
    def check(self, object) -> Optional[RuleException]:
        error: Optional[RuleException] = None
        for rule in self.__rules__.values():
            result = rule.check(object=object)
            if result:
               error = result
               break
        return error
    
    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, RuleGroup) and self.name == __o.name
