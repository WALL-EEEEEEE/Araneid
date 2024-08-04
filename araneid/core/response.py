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
import json
import logging
from enum import auto
from araneid import core as interface
from araneid.state import States, State
from .context import RequestContext

class ResponseStates(States):
    start = auto()
    slot = auto()
    schedule = auto()
    parse = auto()
    complete = auto()

class Response(interface.Slotable, State):
    """
        @class: Response
        @desc:  Abstract response class of downloader
    """
    logger = None
    __slots__ = ['__content__', '__request__', '__odata__', 'encoding']
    States = ResponseStates

    @property
    def content(self):
        return self.__content__
    @property
    def request(self):
        return self.__request__

    def __init__(self, content=None, encoding='UTF-8'):
        self.logger = logging.getLogger(__name__)
        interface.Slotable.__init__(self)
        State.__init__(self)
        self.__odata__ = {}
        self.__content__ = content
        self.__request__ = None
        self.encoding = encoding
    
    @classmethod
    def from_request(cls, request, content=None):
        resp = cls(content)
        # copy __data__ from request
        req_data = getattr(request, '__odata__', None)
        resp.bind(request.slot)
        if req_data:
            setattr(resp, '__odata__', req_data)
        setattr(resp, '__request__', request)

    @property
    def attach(self):
        attach = self.__odata__.get('attach', None)
        return attach
    

    @attach.setter
    def attach(self, attach):
        self.__odata__['attach'] = attach


    @property
    def context(self):
        context = self.__odata__.get('context', None)
        return context

    @context.setter
    def context(self, context):
        assert context is None or isinstance(context, RequestContext)
        self.__odata__['context'] = context

    @property
    def text(self):
        try:
            if type(self.content) is not str:
                return str(self.content, self.encoding, 'ignore')
            else:
                return self.content
        except Exception as e:
            self.logger.debug('content from '+str(self.request.uri)+' can\'t be converted to text by utf-8 encode')
            self.logger.debug(e)
            return None

    @property
    def json(self):
        try:
            return json.loads(self.text)
        except Exception as e:
            self.logger.debug('content from '+str(self.request.uri)+' is not a valid json')
            self.logger.debug(e)
    
    def clear(self):
        self.__content__ = None

    def __str__(self):
        url = '' if  not self.request else self.request.url
        return 'Response({url})'.format(url=url)