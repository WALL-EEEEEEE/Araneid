from json import JSONEncoder
import json
from typing import MutableMapping
from .item import Item


class ItemPipeline(MutableMapping):

   def __init__(self, pipe=None, name=None):
       self.__items = {}
       self.__pipe = pipe
       self.name = name
    
   def flush(self, keySet=set()):
       assert(type(keySet) is set)
       if keySet:
           for key in  keySet:
               if key not in self:
                   continue
               self[key].value = None
       else:
            for item, rule in self.__items.values():
                if rule is None:
                    continue
                item.value = None

   def set_name(self, name):
       self.name = name

   def __delitem__(self, key):
       pass

   def __setitem__(self, key, value):
       assert(key is not None and isinstance(value, Item))
       self.__items[key] = (value, None)

   def update(self, pipeline):
       for item_name, item in pipeline:
           self.__items[item_name] = (item.copy(), None)

   def add(self, item, rule=None):
       self.__items[item.name] = (item, rule)

   def __getitem__(self, key):
       if key not in self.__items:
           return None
       item = self.__items[key][0]
       if not item.value:
          item.value = self.__parse(item.name)
       return item

   def __iter__(self):
       for name in self.__items:
           yield (name,self.__getitem__(name))

   def __len__(self):
       return len(self.__items)

   def __parse(self, name):
       item, rule = self.__items[name]
       result = []
       if not rule:
           return item.value
       if not (type(rule) is list or type(rule) is tuple or type(rule) is set):
           parsed_item = rule.parse(self.__pipe)
           if type(parsed_item) is list:
               result.extend(parsed_item)
           else:
               result.append(parsed_item)
           return result
       for r in rule:
           parsed_item = r.parse(self.__pipe)
           if not parsed_item:
               continue
           if type(parsed_item) is list:
               result.extend(parsed_item)
           else:
               result.append(parsed_item)
       return result

   def set_pipe(self, pipe=None):
        self.__pipe = pipe
   
   def filter(self, func):
       pass

   def pipe(self, pipe=None):
        self.set_pipe(pipe)

   def __str__(self):
       ptr = 'ItemPipeline(name={name}, items={items})'
       items_ptr = '['+', '.join([str(item[0]) for item in self.__items.values()])+']'
       return ptr.format(name=self.name, items=items_ptr)

class JsonPipeline(ItemPipeline):

    class AnyJsonEncoder(JSONEncoder):
        def default(self, o):
            if isinstance(o, (bytes, bytearray)):
                encoded = str(o, 'utf-8')
            else:
                encoded = super().default(o)
            return  encoded

    def pipe(self, pipe=None):
        super().pipe(pipe)
        pipe_output = {}
        for item_name, item in self:
           if  not item.require:
               continue
           name_parts = item.name.split('.')
           if len(name_parts) > 1:
               branch = pipe_output
               for name in name_parts[:-1]:
                   branch = branch.setdefault(name, {})
               branch[name_parts[-1]] =  item.value
           else:
               pipe_output[item_name] = item.value
        return json.dumps(pipe_output, cls=self.AnyJsonEncoder)

