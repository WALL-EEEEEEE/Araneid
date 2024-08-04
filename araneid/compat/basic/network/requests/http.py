"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import warnings

from araneid.network.http import HttpRequest as HttpRequest_

warnings.warn('araneid.basic.network.requests.http.HttpRequest is deprecated, and expected to be removed. please use araneid.network.http.HttpRequest', DeprecationWarning, stacklevel=2)
HttpRequest=HttpRequest_
