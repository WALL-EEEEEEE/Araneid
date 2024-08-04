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

from contextlib import suppress
from araneid.core.scheduler import Scheduler
from araneid.core.request import Request
from araneid.core.response import Response
from araneid.core.stream import Stream
import logging
import asyncio


class DefaultScheduler(Scheduler):
    """
      @class: DefaultScheduler
      @desc:  Default scheduler used by engine
    """
    logger = None


    def __init__(self):
      self.logger = logging.getLogger(__name__)
      self.__closed = False
      self.__running_tasks = set()
    
    @classmethod
    async def create(cls):
       instance = cls()
       instance.__request_channel = await Stream.create()
       instance.__response_channel = await Stream.create()
       return instance
 
    
    def idle(self):
      return self.__request_channel.idle() and self.__response_channel.idle()

    """
      @method: add
      @desc:   Add new request into scheduler
      @param:  Request request, request to be added into scheduler
      @return  void
    """
    async def add_request(self, request):
        assert isinstance(request, Request)
        await self.__request_channel.write(request)
        self.logger.debug('Put request to scheduler: '+str(request))

    async def add_response(self, response):
        assert isinstance(response, Response)
        await self.__response_channel.write(response)
        self.logger.debug('Put response to scheduler: '+str(response))

    async def get_response(self):
      async with self.__response_channel.read() as response_reader:
        async for resp in response_reader:
          self.logger.debug('Get response from scheduler: '+str(resp))
          return resp
    
    async def get_request(self):
      async with self.__request_channel.read() as request_reader:
        async for req in request_reader:
          self.logger.debug('Get request from scheduler: '+str(req))
          return req

    """
      @method: start
      @desc:   Start scehduler
      @return: Request request be scheduled 
    """
    async def run(self, schedule_channel: Stream):
        try:
          resp_task = asyncio.ensure_future(self.get_response())
          req_task = asyncio.ensure_future(self.get_request())
          while not self.__closed:
              self.__running_tasks = {resp_task, req_task}
              schedule_done, __= await asyncio.wait(self.__running_tasks, return_when=asyncio.FIRST_COMPLETED)
              [t.result() for t in schedule_done]
              if resp_task.done():
                resp_task = asyncio.ensure_future(self.get_response())
              if req_task.done():
                req_task = asyncio.ensure_future(self.get_request())
              for item in schedule_done:
                 await schedule_channel.write(item.result())
        finally:
           await self.close()
    
    async def close(self):
      if self.__closed:
         return
      self.__closed = True
      await self.__request_channel.close()
      await self.__response_channel.close()
      with suppress(asyncio.CancelledError):
        await asyncio.gather(*self.__running_tasks)
      self.logger.debug(f'{self.__class__.__name__} closed')
