"""
@author: Wall\'e
@mail:   
@date:   2019.11.04
"""
import warnings
from araneid.network.http import HttpResponse as HttpResponse_

warnings.warn('araneid.basic.network.responses.http.HttpResponse is deprecated, and expected to be removed. please use araneid.network.http.HttpResponse instead', DeprecationWarning, stacklevel=2)
HttpResponse = HttpResponse_
