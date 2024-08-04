#!/usr/bin/env python3
# --encode: utf-8--
import sys
import argparse
from os.path import realpath, dirname

from araneid.core.plugin import load 
from . import resolve_dependences


ARANEID_ROOT = dirname(dirname(realpath(__file__)))
resolve_dependences(ARANEID_ROOT)
import pkg_resources
import araneid
from . import load_script_plugin
#from .test import parser as test_parser
from .status import parser as status_parser
from .run    import parser as run_parser
#from .crawl  import parser as crawl_parser


araneid.runners.SCRIPT_RUNNABLE = False

def get_version():
    version = pkg_resources.require("araneid")[0].version
    return version

def araneid_exec(parsed_args, unused_args):
    if parsed_args.version:
        parser.exit(get_version())
    parser.print_help()

parser = argparse.ArgumentParser(description='Araneid framework cli tools')
parser.add_argument('-i', '--import', required=False, help='Extra module need to be imported')
parser.add_argument('-v', '--version', required=False, action='store_true',  help='Print version of araneid.')
parser.set_defaults(exec=araneid_exec)
subparsers = parser.add_subparsers(help='cmd')
subparsers.add_parser(name='status', parents=[status_parser])
subparsers.add_parser(name='run', parents=[run_parser])
#subparsers.add_parser(name='crawl', parents=[crawl_parser])
plugin_parsers = load_script_plugin()
for name, plugin_parser in plugin_parsers.items():
    subparsers.add_parser(name=name, parents=[plugin_parser])

def main():
    all_args = parser.parse_known_args()
    parsed_args = all_args[0]
    unused_args = all_args[1]
    extra_imported_module = getattr(parsed_args, 'import')
    if extra_imported_module is not None:
        resolve_dependences(extra_imported_module)
    parsed_args.exec(parsed_args, unused_args)
