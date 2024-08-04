from araneid.util.iter import is_iterable
from araneid.spider import Parser
from itertools import filterfalse
from copy import deepcopy
import urllib
import logging
import time
import functools

class DeduplicateError(Exception):
    pass

def is_valid_url(url, qualifying=('scheme', 'netloc')):
    try:
        token = urllib.parse.urlparse(url)
        return all([getattr(token, qualifying_attr) for qualifying_attr in qualifying])
    except:
        return False

class deduplicate:
    logger = logging.getLogger(__name__)
    duplicate_map= {}

    def __init__(self, expires=-1, strict=False):
        self.expires = expires;
        self.strict = strict

    def update_duplicates(self, id):
        if id in self.duplicate_map:
            now = int(time.time())
            old_duplicate_start_time = self.duplicate_map[id]['start']
            duplicate_eslaped = int(now - old_duplicate_start_time)
            if self.expires > 0 and duplicate_eslaped >= self.expires:
                del self.duplicate_map[id]
            else:
                self.duplicate_map[id]['elapsed'] = duplicate_eslaped

    def add_duplicates(self, id):
        if id in self.duplicate_map:
            return
        self.duplicate_map.update({id: {'start': int(time.time()), 'elapsed': 0}})
    
    def clear_expired_duplicates(self):
        clear_items = []
        for id, dup_info in dict.items(self.duplicate_map):
            elapsed = int(time.time()) - dup_info.get('start')
            if self.expires > 0 and elapsed >= self.expires: 
                clear_items.append(id)
        for item in clear_items:
            del self.duplicate_map[item]

    def is_duplicate(self, roomid):
        if roomid in self.duplicate_map and  (self.expires > 0 and (int(time.time()) - self.duplicate_map[roomid]['start']) < self.expires):
            return True
        return False

    def dedup(self, item):
        self.update_duplicates(item)
        if self.is_duplicate(item):
            return None
        else:
            return item
        self.clear_expired_duplicates()

    def __call__(self, func):

        def __iter_dedup(gen):
            if not is_iterable(gen):
                raise DeduplicateError('@dedupliate annotation must be decorated on generator')
            else:
                try:
                    item =  next(gen)
                    while item:
                        if type(item) is set or type(item) is tuple or type(item) is list:
                            dedup_items = list(filterfalse(lambda i: i is None, map(lambda i: self.dedup(i), item)))
                            dup_items = [i  for i in item if i not in dedup_items ]
                            if dup_items:
                                self.logger.warning('Items '+str(dup_items)+' is duplicated in '+str(item)+', ignored')
                                if self.strict:
                                    item = gen.send(dedup_items)
                                    continue
                            if dedup_items:
                                list(map(lambda i: self.add_duplicates(i), dedup_items))
                                yield dedup_items
                        else:
                            dedup_item = self.dedup(item)
                            if not dedup_item:
                                self.logger.warning('Item '+str(item)+ ' is duplicated, ignored')
                            else:
                                yield dedup_item
                                self.add_duplicates(dedup_item)
                        item = next(gen)
                except StopIteration as e:
                    pass

        @functools.wraps(func)
        def __(*args, **kwargs):
            gen = func(*args, **kwargs)
            yield from __iter_dedup(gen)

        def __parse_func__(parser):
            parse_func = parser.parse_func
            @functools.wraps(parse_func)
            def __(*args, **kwargs): 
                gen = parse_func(*args, **kwargs)
                yield from __iter_dedup(gen)
            return __
 
        if isinstance(func, Parser):
            func.bind(__parse_func__(func))
            return func
        else:
            return __
