"""
@author: Wall\'e
@mail:   
@date:   2019.08.07
"""
from abc import ABC, abstractmethod


class UIntArray(ABC):
    buff = []

    def __init__(self, len):
        assert (len >= 0)
        self.buff = [0 for _ in range(0, len)]

    def __str__(self):
        return str(self.buff)

    def __len__(self):
        return len(self.buff)


class UInt8Array(UIntArray):
    pass


class UInt16Array(UIntArray):
    pass


class UInt32Array(UIntArray):
    pass


class UInt64Array(UIntArray):
    pass


class ArrayBuffer:
    def __init__(self, len):
        pass
