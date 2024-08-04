"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import warnings

from araneid.network.websocket import WebSocketRequest as WebSocketRequest_

warnings.warn('araneid.basic.network.responses.websocket.WebSocketResponse is deprecated, and expected to be removed. please use araneid.network.websocket.WebSocketRequest', DeprecationWarning, stacklevel=2)
WebSocketRequest = WebSocketRequest_