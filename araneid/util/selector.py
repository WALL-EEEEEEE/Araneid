"""
@author: Wall\'e
@mail:   
@date:   2019.11.09
"""
import sys
from abc import ABC, abstractmethod


class Selector(ABC):

    def __init__(self,  content, encoding='UTF-8'):
        self.selected = None
        self.content = content
        self.encoding = encoding
    

    @abstractmethod
    def select(self, expr):
        pass

    def extract_first(self):
        selected = ''
        if not self.selected:
            return ''
        if type(self.selected) is list:
            if len(self.selected) > 0:
               selected = self.selected[0]
        else:
            selected = self.selected
        return selected

    def extract_all(self):
        if not self.selected:
            return []
        if not type(self.selected) is list:
            return [self.selected]
        return self.selected

    def __str__(self):
        if type(self.selected) is list:
            return self.__class__.__name__ + '([' + ','.join(self.selected) + '])'
        else:
            selected = ''
            if self.selected:
                selected = self.selected
            return self.__class__.__name__ + '(' + str(selected) + ')'


class CssSelector(Selector):
    pass


class XpathSelector(Selector):
    __parser = None 


    def __init_parser__(self):
        from lxml import html, etree
        self.__parser = html
        self.etree = etree

    def __init__(self, html, encoding='UTF-8'):
        super().__init__(content=html, encoding=encoding)
        if not self.__parser:
            self.__init_parser__()
        self.__doc = self.__parser.fromstring(self.content)

    def select(self, expr):
        # convert returned object from  xpath into Selectors for concatenate selecting
        returned = self.__doc.xpath(expr)
        self.selected = returned
        return self
    
    def extract_first(self):
        self.selected = super().extract_first()
        if isinstance(self.selected, self.etree.ElementBase):
            self.selected = VolatileSelector(self.__parser.tostring(self.selected))
        return self.selected
    
    def extract_all(self):
        self.selected = super().extract_all()
        for index, s in enumerate(self.selected):
            if not isinstance(s, self.etree.ElementBase):
                continue
            s = VolatileSelector(self.__parser.tostring(s))
            self.selected[index] = s
        return self.selected
       

class RegexSelector(Selector):
    __parser = None

    def __init_parser__(self):
        import re
        self.__parser = re

    def __init__(self, content, encoding='UTF-8'):
        super().__init__(content=content, encoding=encoding)
        if not self.__parser:
            self.__init_parser__()

    def select(self, expr):
        compiler = self.__parser.compile(expr)
        if self.content is not str:
            self.content = str(self.content, encoding=self.encoding)
        returned = compiler.findall(self.content)
        self.selected = returned
        return self


class JsonSelector(Selector):
    pass


class QuerySelector(Selector):
    pass


class VolatileSelector(Selector, ABC):

    def json(self, expr):
        self.selected = JsonSelector(self.selected).select(expr)
        return self

    def re(self, expr):
        if not self.selected:
            selected = RegexSelector(self.content).select(expr).extract_all()
            selector = VolatileSelector(selected)
            selector.selected = selected
            return selector
        if type(self.selected) is not list:
            self.selected = RegexSelector(self.selected).select(expr).extract_all()
        else:
            selected = []
            for s in self.selected:
                sed = RegexSelector(s).select(expr).extract_all()
                if type(sed) is list:
                    selected.extend(sed)
                else:
                    selected.append(sed)
            self.selected = selected
        return self

    def css(self, expr):
        self.selected = CssSelector(self.selected).select(expr)
        return self

    def query(self, expr):
        self.selected = QuerySelector(self.selected).select(expr)
        return self

    def xpath(self, expr):
        if not self.selected:
            selected = XpathSelector(self.content).select(expr).extract_all()
            selector = VolatileSelector(selected)
            selector.selected = selected
            return selector
        if type(self.selected) is not list:
            self.selected = XpathSelector(self.selected).select(expr).extract_all()
        else:
            selected = []
            for s in self.selected:
                sed = XpathSelector(s).select(expr).extract_all()
                if type(sed) is list:
                    selected.extend(sed)
                else:
                    selected.append(sed)
            self.selected = selected
        return self

    def select(self, expr):
        return self.selected


class SelectorList(VolatileSelector):

    def __init__(self,  content):
        self.selectors = []
        for c in content:
            self.selectors.append(VolatileSelector(c))
        self.selected = []
        self.content = content
    
    def re(self, expr):
        if not self.selected:
            for s in self.selectors:
                selected = s.re(expr).extract_all()
                self.selected.extend(selected)
            return self
        if type(self.selected) is not list:
            self.selected = self.selected.re(expr).extract_all()
        else:
            selected = []
            for s in self.selected:
                sed = s.re(expr).extract_all()
                if type(sed) is list:
                    selected.extend(sed)
                else:
                    selected.append(sed)
            self.selected = selected
        return self
    
    def extract_all(self):
        if not self.selected:
            for s in self.selectors:
                selected = s.extract_all()
                self.selected.extend(selected)
        return self.selected

    def __str__(self):
        if type(self.selected) is list:
            return self.__class__.__name__ + '([' + ','.join(self.selected) + '])'
        else:
            return self.__class__.__name__ + '(' + str(self.selected) + ')'
