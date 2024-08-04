import os
import json
import sys
from os.path import basename
from json.decoder import JSONDecodeError
from argparse import ArgumentParser
from araneid.spider.spider import Spider
from araneid.core.exception import InvalidCrawler, SpiderNotFound
from araneid.tools.package import get_spider_from_wheel
from araneid.scripts import auto_detect_script_format, get_spider_from_py

def json_output(spider, **kwargs):
    spider_status = {}
    for name, value in kwargs.items():
        spider_status[name]= value
    if isinstance(spider, Spider):
        spider_starter = [ starter for starter in spider.get_starters() ] 
        spider_parser =  [ parser for parser in spider.get_parsers()]
        spider_item  =  [ item for parser in spider_parser for item in parser.items]
        spider_status['name'] = spider.name
        spider_status['parser'] = [ {"name": parser.name, "url": parser.url, "regex": parser.regex } for parser in spider_parser ]
        spider_status['starter'] = [ starter.name for starter in spider_starter]
        spider_status['item'] = [{'name': item.name, 'type': str(item.type)} for item in spider_item ]
    return json.dumps(spider_status)

def text_output(spider, **kwargs):
    spider_status = ''
    for name, value in kwargs.items():
        spider_status += name+':\n\t'+value+'\n'
    if isinstance(spider, Spider):
        spider_starter = [ starter for starter in spider.get_starters() ] 
        spider_parser =  [ parser for parser in spider.get_parsers()]
        spider_item  =  [ item for parser in spider_parser for item in parser.items]
        spider_item_ptr = '\n\t'.join([ str(item.name)+' - '+str(item.type) for item in spider_item ])
        spider_starter_ptr = '\n\t'.join([ str(s) for s in spider_starter])
        spider_parser_ptr =  '\n\t'.join([ str(p) for p in spider_parser])
        spider_status += 'starter:\n\t'+spider_starter_ptr+'\n'+'parser:\n\t'+spider_parser_ptr+'\n'+'item:\n\t'+spider_item_ptr
    return spider_status


def araneid_status_exec(parsed_args, unused_args):
    spider_script = parsed_args.spiderscript
    spider_spider = parsed_args.spider
    script_format = parsed_args.format
    output_format = parsed_args.output
    extra_info = parsed_args.extra
    if extra_info:
        try:
            extra_info = json.loads(extra_info)
        except JSONDecodeError:
            extra_info = {}
    else:
        extra_info = {}
    if not os.path.exists(spider_script):
       parser.error(spider_script + ' doesn\'t exists.')
    if not os.path.isfile(spider_script):
       parser.error(spider_script + ' isn\'t a valid file. Executable spiderscript must be a file')
    if not os.access(spider_script, os.R_OK):
       parser.error('you haven\'t sufficent privilege to access '+spider_script)
    if not spider_spider:
        spider_spider = os.path.splitext(basename(spider_script))[0]
    if not script_format:
        script_format = auto_detect_script_format(spider_script)
    try:
        if script_format == 'PY':
            spider = get_spider_from_py(spider_script, spider_spider)
        elif script_format == 'WHL':
            spider = get_spider_from_wheel(spider_script, spider_spider)
        elif script_format == 'EGG':
            pass
        elif script_format == 'ZIP':
            pass
        elif script_format == 'TAR':
            pass
        if output_format == 'json':
            spider_status = json_output(spider, name=spider_spider, **extra_info)
        elif output_format == 'text':
            spider_status = text_output(spider, name=spider_spider, **extra_info)
        parser._print_message(spider_status, sys.stdout)
    except (InvalidCrawler, SpiderNotFound) as  e:
        parser.error(e)
    except Exception as e:
        raise(e)

# status subparser
parser = ArgumentParser(prog='status', description='show a spider status', add_help=False)
parser.add_argument('--output', '-o', default='text', required=False, choices=['text', 'json'], help='output format, default to text.')
parser.add_argument('--format', default=None, required=False, choices=['zip', 'tar', 'wheel', 'egg', 'py'], help='file format to be run, default to be python script')
parser.add_argument('--spider', required=False, default=None, help='name of spider to be viewed status. if none is specified, crawler name conincided with script name, without extension suffix,  will be used')
parser.add_argument('--extra', required=False, default='', help='extra info should presented with status info of spider.')
parser.add_argument('spiderscript', help='spider script status of which to be showed')
parser.set_defaults(exec=araneid_status_exec)