"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import logging
import asyncio
from typing import Dict
from araneid.core.request import Request
from araneid.core.response import Response




class WebSocketRequest(Request):

    @property
    def callbacks(self):
        return self.__callbacks__
    
    @property
    def callback(self):
        return self.__callback__
    
    @property
    def cookies(self):
        return self.__cookies__

    @property
    def headers(self):
        return self.__headers__

    @property
    def on_open(self):
        return self.__on_open__

    @property
    def ping_interval(self):
        return self.__ping_interval__

    @property
    def on_ping(self):
        return self.__on_ping__

    @property
    def proxy(self):
        return self.__proxy__
    
    def set_proxy(self, proxy):
        self.__proxy__ = proxy

    @property
    def closeback(self):
        return self.__closeback__
   
    @property
    def errback(self):
        return self.__errback__
    
    @property
    def method(self):
        return self.__method__
    

    def __init__(self, url,  downloader='WebSocket', **kwargs):
        max_retries = kwargs.get('max_retries', 3)
        attach = kwargs.get('attach', None)
        meta = kwargs.get('meta', None)
        super().__init__(url, downloader=downloader, attach=attach, max_retries=max_retries, meta=meta)
        self.__timeout__ = kwargs.get('timeout', None)
        self.__headers__ = kwargs.get('headers', {})
        self.__proxy__ = kwargs.get('proxy', None) if kwargs.get('proxy', None)  else kwargs.get('proxies', None)
        self.__cookies__ = kwargs.get('cookies', None)
        self.__callbacks__ = kwargs.get('callbacks', None)
        self.__callback__ = kwargs.get('callback', None)
        self.__errback__ = kwargs.get('errback', None)
        self.__closeback__ = kwargs.get('closeback', None)
        self.__on_open__ = kwargs.get('on_open', None)
        self.__on_ping__ = kwargs.get('on_ping', None)
        self.__proxies__ = kwargs.get('proxies', None)
        self.__ping_interval__ = kwargs.get('ping_interval', 30)
        self.__method__ = kwargs.get('method', 'GET')

    async def wait(self, state=Request.States.slot):
        await self.wait_state(state)

    def __str__(self):
        rep = 'WebSocketRequest<url={url}>'.format(url=self.url)
        return rep


class WebSocketResponse(Response):

    def __init__(self, content, **kwargs):
        super().__init__(content=content)
        self.encoding = kwargs.get('encoding', 'UTF_8')
        self.__length__ =  0 if content is None else len(content)
        self.__cookies__ = kwargs.get('cookies', {})
        self.__headers__ = kwargs.get('headers', {})
        self.__request__ = None

    @property
    def length(self):
        return self.__length__

    def __str__(self):
        url = self.request.url if  self.request  else ''
        return 'WebSocketResponse<size={size} bytes, url={url}> '.format(size=self.length, url=url)

    @classmethod
    def from_request(cls, request, **kwargs):
        assert isinstance(request, WebSocketRequest)
        content = kwargs.get('content', b'')
        resp = cls(content)
        resp.bind(request.slot)
        # copy __meta_data__
        _req_data =  getattr(request, '__odata__', None)
        if _req_data:
            setattr(resp, '__odata__', _req_data)
        setattr(resp, '__request__', request)
        return resp
