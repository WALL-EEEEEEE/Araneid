#  Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

"""
@author: Wall\'e
@mail:   
@date:   2019.06.06
"""

from abc import abstractclassmethod, abstractmethod
from araneid.core.request import Request
from araneid.core import Epollable


class Downloader(Epollable):
    """
    @class: Downloader
    @param: Request request , request to be download
    @desc:  Downloader Interface that all downloaders must implement
    """
    logger=None 

    def __init__(self) -> None:
        super().__init__()
    
    @abstractmethod
    def download(self, request: Request):
        raise NotImplementedError(f"download method in {self.__class__.__name__} isn\'t implemented")
    
    @classmethod
    @abstractmethod
    async def create(cls, settings=None):
        raise NotImplementedError(f"download method in {cls.__name__} isn\'t implemented")
    
    def __str__(self):
        return '.'.join([self.__class__.__module__, self.__class__.__name__])
    
    def close(self):
        pass
