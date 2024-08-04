class Option:

    def __init__(self, name, alias='', help='', default='', **kwargs):
        self.name = name
        self.alias = alias
        self.help = help
        self.default = default
        self.kwargs = kwargs

class Argument:
    def __init__(self, name, alias='', help='', default='', **kwargs):
        self.name = name
        self.alias = alias
        self.help = help
        self.default = default
        self.kwargs = kwargs

def option(name, alias='', help='', default='', **kwargs):
    def __(cls):
        script_args = getattr(cls, '__araneid_script_args', None)
        if not script_args:
            args = []
            args.append(Option(name, alias, help, default, **kwargs))
            setattr(cls, '__araneid_script_args', args)
        else:
            script_args.append(Option(name, alias, help, default, **kwargs))
        return cls
    return __

def argument(name, alias='', help='', default='', **kwargs):
    def __(cls):
        script_args = getattr(cls, '__araneid_script_args', None)
        if not script_args:
            args= []
            args.append(Argument(name, alias, help, default, **kwargs))
            setattr(cls, '__araneid_script_args', args)
        else:
            script_args.append(Argument(name, alias, help, default, **kwargs))
        return cls
    return __
