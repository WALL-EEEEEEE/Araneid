"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
from enum import auto
from araneid.core.request import Request
from araneid.core.response import Response
from araneid.util.selector import VolatileSelector
from araneid.state import States, State

class HttpHeaderException(Exception):
    pass



class HttpRequest(Request):
    __slots__ = ['__uri__', '__method__', '__data__', '__json__', '__files__', '__callbacks__', '__callback__', '__errback__', '__proxy__', '__cookies__', '__headers__', '__timeout__', '__proxy__']


    @property
    def url(self):
        return self.__uri__

    @property
    def method(self):
        return self.__method__

    @property
    def data(self):
        return self.__data__

    @property
    def json(self):
        return self.__json__

    @property
    def files(self):
        return self.__files__

    @property
    def callbacks(self):
        return self.__callbacks__
    
    @property
    def callback(self):
        return self.__callback__

    @property
    def proxy(self):
        return self.__proxy__
    
    @property
    def cookies(self):
        if not self.__cookies__:
           return self.__cookies__
        cookies = { cookie_key if isinstance(cookie_key, str) else str(cookie_key, 'utf-8') : cookie_value if isinstance(cookie_value, str) else str(cookie_value, 'utf-8')  for (cookie_key, cookie_value) in self.__cookies__.items() }
        return cookies

    @property
    def headers(self):
        if not self.__headers__:
            return self.__headers__
        str_headers = {}
        for header_key, header_value in self.__headers__.items():
            try:
                if not isinstance(header_key, str):
                    header_key = str(header_key, 'utf-8') if isinstance(header_key, (bytes, bytearray))  else str(header_key)
                if not isinstance(header_value, str):
                    header_value = str(header_value, 'utf-8') if isinstance(header_value, (bytes, bytearray))  else str(header_value)
            except Exception:
                raise HttpHeaderException('Header {header_key}: {header_value} can\'t convert to string.'.format(header_key=header_key, header_value=header_value))
            else:
                str_headers[header_key] = header_value
        return str_headers
    
    
    @property
    def errback(self):
        return self.__errback__

    def set_header(self, key, value):
        self.__headers__[key] = value
    
    def set_proxy(self, proxy):
        self.__proxy__ = proxy
    
    
    def __init__(self, url, method='GET', downloader='Http', **kwargs):
        max_retries = kwargs.get('max_retries', 3)
        attach = kwargs.get('attach', None)
        meta = kwargs.get('meta', None)
        context = kwargs.get('context', None)
        timeout = kwargs.get('timeout', 30)
        super().__init__(url, downloader=downloader, attach= attach, max_retries=max_retries, meta=meta, context=context, timeout=timeout)
        self.__method__ = method
        self.__callbacks__ = kwargs.get('callbacks', None)
        self.__callback__ = kwargs.get('callback', None)
        self.__data__ = kwargs.get('data', None)
        self.__headers__ = kwargs.get('headers', {})
        self.__json__ = kwargs.get('json', None)
        self.__files__ = kwargs.get('files', None)
        self.__proxy__ = kwargs.get('proxy', None) if kwargs.get('proxy', None)  else kwargs.get('proxies', None)
        self.__cookies__ = kwargs.get('cookies', {})
        self.__errback__ = kwargs.get('errback', None)
    
   
    def __str__(self):
        rep = 'HttpRequest<url={url}>'.format(url=self.url)
        return rep



class HttpResponse(Response):

    __slots__ = ['__headers__', '__reason__', '__ok__', '__status__', '__length__', '__cookies__', '__history__','encoding']

    @property
    def headers(self):
        return self.__headers__

    @property
    def reason(self):
        return self.__reason__

    @property
    def ok(self):
        return self.__ok__
    
    @property
    def status(self):
        return self.__status__
    @property
    def status_code(self):
        return self.__status__
    
    @property
    def length(self):
        return self.__length__

    @property
    def cookies(self):
        if not self.__cookies__:
           return self.__cookies__
        cookies = { cookie_key if isinstance(cookie_key, str) else str(cookie_key, 'utf-8') : cookie_value if isinstance(cookie_value, str) else str(cookie_value, 'utf-8')  for (cookie_key, cookie_value) in self.__cookies__.items() }
        return cookies

    def __init__(self, status, content, **kwargs):
        assert (type(status) is int)
        super().__init__(content=content)
        self.encoding = kwargs.get('encoding', 'UTF_8')
        self.__status__ = status
        self.__length__ =  0 if content is None else len(content)
        self.__cookies__ = kwargs.get('cookies', {})
        self.__history__ = kwargs.get('history', [])
        self.__headers__ = kwargs.get('headers', {})
        self.__reason__ = kwargs.get('reason', '')
        self.__ok__ = kwargs.get('ok', '')
        self.__request__ = None

    def xpath(self, expr):
        if self.content:
            selector = VolatileSelector(self.content)
            return selector.xpath(expr)
        return None

    def re(self, expr, encoding='UTF-8'):
        if self.content:
            selector = VolatileSelector(self.content, encoding=encoding)
            return selector.re(expr)
        return None

    def css(self, expr):
        if self.content:
            selector = VolatileSelector(self.content)
            return selector.css(expr)
        return None
    
    def __str__(self):
        url = self.request.url if  self.request  else ''
        return 'HttpResponse<code={status},size={size} bytes, url={url}> '.format(status=self.status,size=self.length, url=url)
    
    @classmethod
    def from_request(cls, request, **kwargs):
        assert isinstance(request, HttpRequest)
        resp = cls(**kwargs)
        # copy __meta_data__
        resp.bind(request.slot)
        _req_data =  getattr(request, '__odata__', None)
        if _req_data:
            setattr(resp, '__odata__', _req_data)
        setattr(resp, '__request__', request)
        return resp