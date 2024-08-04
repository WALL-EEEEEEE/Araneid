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

from enum import auto
import logging
from araneid import core as interface
from araneid.state import State, States
from .context import RequestContext

class Meta:
    def __init__(self, key=None, value=None, name=None) -> None:
        if name == None:
           self.name = f'{self.__class__.__name__}_{id(self)}'
        else:
           self.name = name
        self.__meta__  = {}
        if key is not None and  value is not None:
           self.__meta__[key] = value
    
    def get(self, name, default=None):
        return self.__meta__.get(name, default)
    
    def set(self, value):
        self.__meta__[self.name] = value
        return self
    
    def __getitem__(self, name):
        return self.__meta__.get(name)

    def __setitem__(self, key, item):
        self.__meta__[key] = item

    def __repr__(self):
        return repr(self.__meta__)

    def __len__(self):
        return len(self.__meta__)

    def __delitem__(self, key):
        del self.__meta__[key]

    def clear(self):
        return self.__meta__.clear()

    def copy(self):
        return self.__meta__.copy()

    def has_key(self, k):
        return k in self.__meta__

    def __and__(self, other):
        self.update(other)
        return self
    
    def update(self, other):
        assert isinstance(other, Meta)
        other_meta = getattr(other, '__meta__', {})
        self.__meta__.update(other_meta)
    
    def __str__(self):
        return str(self.__meta__)

class RequestStates(States):
    start = auto()
    slot = auto()
    schedule = auto()
    download = auto()
    parse = auto()
    complete = auto()

class Request(interface.Slotable, State):
    """请求抽象类，所有请求必须实现该类

    Args:
        interface (interface.Slotable) 
    """
    __slots__ = ['__downloader__', '__max_retries__', '__meta__', '__uri__', '__odata__', 'retries']
    States = RequestStates
    logger = None


    async def wait(self, state=States.parse):
        await self.wait_state(state)

    @property
    def downloader(self):
        return self.__downloader__

    @property
    def max_retries(self):
        return self.__max_retries__

    @property
    def uri(self):
        return self.__uri__
    
    @property
    def url(self):
        return self.__uri__
    
    @property
    def meta(self):
        return self.__meta__
    
    @meta.setter
    def meta(self, meta):
        assert isinstance(meta, Meta)
        self.__meta__ = meta

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
    def timeout(self):
        return self.__timeout__


    def __init__(self, uri: str, max_retries: int=0, downloader=[], attach=None, meta=None, context=None, timeout=None):
        """构造器

        Args:
            uri ([type]): [description]
            max_retries (int, optional): [description]. Defaults to 0.
            downloader (list, optional): [description]. Defaults to [].
            attach ([type], optional): [description]. Defaults to None.
            meta ([type], optional): [description]. Defaults to None.
            context ([type], optional): [description]. Defaults to None.
            timeout ([type], optional): [description]. Defaults to None.
        """
        interface.Slotable.__init__(self)
        State.__init__(self)
        assert meta is None or isinstance(meta, Meta)
        self.__odata__ = {}
        self.__uri__ = uri
        self.__downloader__ = downloader
        self.__max_retries__ = max_retries
        self.__meta__ = Meta(name='default') 
        if meta is not None:
           self.__meta__.update(meta)
        self.__meta__ = Meta(name='default') if meta is None else meta
        self.__timeout__ = timeout
        self.context = context
        if attach:
           self.__odata__['attach'] = attach
        self.retries = 0
    
    @classmethod
    def from_request(cls, request):
        props ={name: getattr(request, name) for name in  dir(request) if not name.startswith('__')} 
        return cls(**props)

    def __str__(self):
        return 'Request<url={url}>'.format(url=self.url)
    

