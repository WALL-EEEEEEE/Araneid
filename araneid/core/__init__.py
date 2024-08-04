#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.05.07
"""
import araneid
from abc import ABC, abstractmethod
from collections.abc import Coroutine
from .exception import SlotError


class Epollable(Coroutine):

    def __await__(self):
        pass

    def send(self, value):
        pass

    def throw(self, typ, val=None, tb=None):
        pass

    def close(self):
        pass
"""
    @abstractmethod
    def fileno(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self):
        pass
"""

class Slotable(ABC):
    __slots__ = ['__slot__', '__completed__']

    @property
    def slot(self):
        return self.__slot__

    def __init__(self):
        self.__slot__ = None
        self.__completed__ = False
    
    def set_completed(self, completed):
        assert isinstance(completed, bool)
        self.__completed__ = completed

    def complete(self):
        self.__completed__ = True
    
    def is_completed(self):
        return self.__completed__

    def bind(self, slot, force=False):
        assert isinstance(slot, araneid.core.slot.Slot) and isinstance(force, bool)
        if self.__slot__ and not force:
            raise SlotError('Request {request} has bound to a slot {slot}, can\'t bind to another slot again!'.format(request=self, slot=id(slot)))
        self.__slot__ = slot