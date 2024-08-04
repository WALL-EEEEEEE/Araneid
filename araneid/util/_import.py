from importlib import import_module

def import_class(class_path):
    assert(type(class_path) is str and class_path)
    cls_path, module_path = class_path.split('.')[-1], '.'.join(class_path.split('.')[:-1])
    try:
       _module= import_module(module_path)
       _class = getattr(_module, cls_path)
       return _class
    except ImportError:
        raise ImportError("Failed to import class "+class_path)
