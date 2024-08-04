from argparse import ArgumentParser


def araneid_parse_exec(parsed_args, unused_args):
    parser.print_help()

# parse subparser
parser = ArgumentParser(prog='parse', description='invoke a parser in a spider', add_help=False)
parser.add_argument('--format', default=None, required=False, choices=['zip', 'tar', 'wheel', 'egg', 'py'], help='file format to be run, default to be python script')
parser.add_argument('--spider', required=False, help='spider in spider script used')
parser.add_argument('--parser', required=False, help='parser in spider used')
parser.add_argument('spiderscript', help='spider script need to be run')
parser.add_argument('content', help='content to be parsed')
parser.set_defaults(exec=araneid_parse_exec)