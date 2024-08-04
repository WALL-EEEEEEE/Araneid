"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import warnings

from araneid.network.websocket import WebSocketResponse as WebSocketResponse_

warnings.warn('araneid.basic.network.responses.http.HttpResponse is deprecated, and expected to be removed, please use araneid.network.websocket.WebSocketResponse instead', DeprecationWarning, stacklevel=2)
WebSocketResponse = WebSocketResponse_