from abc import ABC, abstractmethod
from araneid.util.selector import CssSelector, RegexSelector, XpathSelector

class Rule(ABC):
    @abstractmethod
    def parse(self, text):
        pass

class XpathRule(Rule):
    def __init__(self, expr):
        self.expr = expr

    def parse(self, text):
        result = []
        try:
            result  = XpathSelector(text).select(self.expr).extract_all()
        except Exception:
            pass
        finally:
            return result


class CssRule(Rule):
    def __init__(self, expr):
        self.expr = expr

    def parse(self, text):
        result = []
        try:
            result = CssSelector(text).select(self.expr).extract_all()
        except Exception:
            pass
        finally:
            return result

class RegexRule(Rule):
    def __init__(self, expr):
        self.expr = expr

    def parse(self, text):
        result = []
        try:
            result = RegexSelector(text).select(self.expr).extract_all()
        except Exception:
            pass
        finally:
            return result
