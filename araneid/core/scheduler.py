#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.06.12
"""
import asyncio
from inspect import iscoroutine, getmodule
from abc import ABC, abstractclassmethod, abstractmethod
from araneid.core.request import Request
from araneid.core.response import Response


class Scheduler(ABC):
    """
       @class: Scheduler
       @desc:  scheduler interface that all schedulers must implement
    """
    __reqeusts = []

    @abstractclassmethod
    async def create(cls, settings=None):
        raise NotImplementedError

    @abstractmethod
    def idle(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add_request(self, request: Request) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_response(self, request: Response) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def get_request(self) -> Request:
        raise NotImplementedError
    
    @abstractmethod
    def get_response(self) -> Response:
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

