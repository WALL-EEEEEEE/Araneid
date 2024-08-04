"""
@author: Wall\'e
@mail:   
@date:   2019.09.20
"""


class Dict:

    def __init__(self, d):
        assert (type(d) is dict)
        self.__dict = d

    def get(self, key, default_value):
        get_value = self.__dict.get(key, default_value)
        if isinstance(get_value, dict) and not isinstance(get_value, Dict):
            get_value = Dict(get_value)
        if isinstance(default_value, dict) and not isinstance(default_value, Dict):
            default_value = Dict(default_value)
        return get_value or default_value


class MIDict(object):

    def __init__(self, **kwargs):
        self._keys = {}
        self._data = {}
        for k, v in kwargs.items():
            self[k] = v

    def __getitem__(self, key):
        try:
            return self._data[key]
        except KeyError:
            return self._data[self._keys[key]]

    def __setitem__(self, key, val):
        try:
            self._data[self._keys[key]] = val
        except KeyError:
            if isinstance(key, tuple):
                if not key:
                    raise ValueError(u'Empty tuple cannot be used as a key')
                key, other_keys = key[0], key[1:]
            else:
                other_keys = []
            self._data[key] = val
            for k in other_keys:
                self._keys[k] = key

    def __contains__(self, item):
        try:
            value = self.__getitem__(item)
            return True
        except KeyError:
            return False

    def iter(self, item):
        try:
            return iter(item)
        except TypeError:
            return [item]

    def __str__(self):
        dict_str = '{%s}'
        pairs = []
        for key, value in self._data.items():
            key_str = str(key)
            invert_keys = dict((v, k) for k, v in self._keys.items())
            if key in invert_keys:
                key_str = '(' + key_str + ',' + ','.join([str(i) for i in self.iter(invert_keys[key])]) + ')'
            value_str = str(value)
            pair_str = key_str + ': ' + value_str
            pairs.append(pair_str)
        formatted_dict_str = dict_str % ', '.join(pairs)
        return formatted_dict_str

    def get(self, key, *args, **kwargs):
        try:
            value = self.__getitem__(key)
            return value
        except KeyError:
            if 'default' not in kwargs:
                raise
            else:
                return kwargs['default']

    def add_keys(self, to_key, new_keys):
        if to_key not in self._data:
            to_key = self._keys[to_key]
        for key in new_keys:
            self._keys[key] = to_key

    @classmethod
    def from_dict(cls, dic):
        result = cls()
        for key, val in dic.items():
            result[key] = val
        return result
