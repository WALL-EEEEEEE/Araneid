from argparse import ArgumentParser


def araneid_start_exec(parsed_args, unused_args):
    parser.print_help()

# start subparser
parser = ArgumentParser(prog='start', description='start a crawler', add_help=False)
parser.set_defaults(exec=araneid_start_exec)

