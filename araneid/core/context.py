class Context(object):
    pass


class RequestContext(Context):

    def __init__(self, caller=None, spider=None, scraper=None):
        self.caller = caller
        self.spider = spider
        self.scraper = scraper
   
    def __str__(self) -> str:
        pstr = 'RequestContext( caller= {caller}, spider= {spider}, scraper={scraper} )'.format(caller=self.caller, spider=self.spider, scraper=self.scraper)
        return  pstr

class ParserContext(Context):
    __PARSER_EXPORT__ = ['items']

    def __init__(self, parser, spider):
        object.__setattr__(self, 'parser', parser)
        object.__setattr__(self, 'spider', spider)


    def __getattribute__(self, name):
        try:
            spider = object.__getattribute__(self, 'spider')
            return getattr(spider, name)
        except AttributeError:
            paser_export = object.__getattribute__(self, '__PARSER_EXPORT__')
            if name not in paser_export:
                raise AttributeError
            parser = object.__getattribute__(self, 'parser')
            return getattr(parser, name)
    
    def __setattr__(self, name, value):
        spider = object.__getattribute__(self, 'spider')
        object.__setattr__(spider, name, value)
    
    def __str__(self):
        pstr = 'ParserContext( parser= {parser}, spider= {spider} )'.format(parser=self.parser, spider=self.spider)
        return pstr