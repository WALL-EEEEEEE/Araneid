"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
from araneid.core.request import Request
from araneid.core.response import Response


class SocketRequest(Request):
    connection = None
    __slots__ = ['__callbacks__', '__callback__', '__errback__', '__closeback__','__on_open__', '__on_ping__', '__ping_interval__', '__proxy__', '__proxies__', '__timeout__']

    @property
    def callbacks(self):
        return self.__callbacks__
 
    @property
    def callback(self):
        return self.__callback__

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
    def closeback(self):
        return self.__closeback__


    @property
    def errback(self):
        return self.__errback__


    def __init__(self, url, downloader='Socket', **kwargs):
        max_retries = kwargs.get('max_retries', 3)
        attach = kwargs.get('attach', None)
        meta = kwargs.get('meta', None)
        context = kwargs.get('context', None)
        super().__init__(url, downloader=downloader, attach=attach, max_retries=max_retries, meta=meta, context=context)
        self.__timeout__ = kwargs.get('timeout', None)
        self.__proxy__ = kwargs.get('proxy', None) if kwargs.get('proxy', None)  else kwargs.get('proxies', None)
        self.__callbacks__ = kwargs.get('callbacks', None)
        self.__callback__ = kwargs.get('callback', None)
        self.__errback__ = kwargs.get('errback', None)
        self.__closeback__ = kwargs.get('closeback', None)
        self.__on_open__ = kwargs.get('on_open', None)
        self.__on_ping__ = kwargs.get('on_ping', None)
        self.__proxies__ = kwargs.get('proxies', None)
        self.__ping_interval__ = kwargs.get('ping_interval', 30)
    
    async def wait(self, state=Request.States.slot):
        await self.wait_state(state)


    def __str__(self):
        rep = 'SocketRequest<url={url}>'.format(url=self.url)
        return rep




class SocketResponse(Response):
    __slots__ = ['__length__',  '__request__']

    def __init__(self, content, **kwargs):
        super().__init__(content=content, **kwargs)
        self.__length__ =  0 if content is None else len(content)
        self.__request__ = None

    @property
    def length(self):
        return self.__length__

    def __str__(self):
        url = self.request.url if  self.request  else ''
        return 'SocketResponse<size={size} bytes, url={url}> '.format(size=self.length, url=url)

    @classmethod
    def from_request(cls, request, **kwargs):
        assert isinstance(request, SocketRequest)
        content = kwargs.get('content', b'')
        resp = cls(content)
        # copy __meta_data__
        resp.bind(request.slot)
        _req_data =  getattr(request, '__odata__', None)
        if _req_data:
            setattr(resp, '__odata__', _req_data)
        setattr(resp, '__request__', request)
        return resp