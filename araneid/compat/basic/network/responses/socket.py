"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import warnings

from araneid.network.socket import SocketResponse as SocketResponse_

warnings.warn('araneid.basic.network.responses.socket.SocketResponse is deprecated, and expected to be removed, please use araneid.network.socket.SocketResponse instead.', DeprecationWarning, stacklevel=2)
SocketResponse_ = SocketResponse_