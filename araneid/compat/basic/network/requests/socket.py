"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import warnings

from araneid.network.socket import SocketRequest as SocketRequest_

warnings.warn('araneid.basic.network.requests.socket.SocketRequest is deprecated, and expected to be removed. please use araneid.network.socket.Socketquest', DeprecationWarning, stacklevel=2)
SocketRequest = SocketRequest_