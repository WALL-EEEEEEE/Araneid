import os
import sys
import araneid
import logging
from datetime import datetime
from argparse import ArgumentParser 
from os.path import basename
from araneid.logger import config_log
from araneid.core.exception import InvalidCrawler, SpiderNotFound, StarterNotFound
from araneid.runner import SpiderArgsSupport
from araneid.spider.spider import Spider, Starter
from araneid.annotation import Argument, CrawlerCliRunner as CrawlerRunnerAnnotation, Option
from araneid.tools.package import get_spider_from_wheel, install_package
from araneid.scripts import DeprecateAction, DeprecateStoreTrueAction, mark_deprecated_help_strings,  auto_detect_script_format, get_spider_from_py, start_spider


def araneid_run_exec(parsed_args, unused_args):
    araneid.runners.SCRIPT_RUNNABLE = True
    spider_script = parsed_args.spiderscript
    spider_starter = parsed_args.starter
    spider_spider = parsed_args.spider
    # run timer settings
    run_crontab = parsed_args.crontab
    # log settings
    log_level = parsed_args.loglevel
    script_args = unused_args
    script_format = parsed_args.format
    script_args.insert(0, spider_script)
    sys.argv = script_args
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
    # apply third-party install for py script
    package_require = parsed_args.package_require
    if package_require:
       package_require = package_require.split(',')
       install_package(package_require)
    try:
        if script_format == 'PY':
            spider = get_spider_from_py(spider_script, spider_spider)
        elif script_format == 'WHL':
            spider = get_spider_from_wheel(spider_script)
        elif script_format == 'EGG':
            pass
        elif script_format == 'ZIP':
            pass
        elif script_format == 'TAR':
            pass
        else:
            spider = get_spider_from_py(spider_script, spider_spider)
        # support running while loading spider from script in old spider template 
       # get spider arguments
        spider_script_args = SpiderArgsSupport.parse_spider_args(spider)
        if isinstance(spider, Spider):
           meta_vars =  { arg_name: arg_value.get('value', '') for arg_name, arg_value in  spider_script_args.items() }
           meta_vars['spider'] = spider.name
           meta_vars['current_time'] = datetime.now()
        else:
           meta_vars = {}
        config_log(level=log_level, metavars=meta_vars)
        # spider logger will not record on handlers because it is initialized before log set up.
        spider.logger = logging.getLogger(spider.name)
        if isinstance(spider, CrawlerRunnerAnnotation):
            spider.run()
        elif isinstance(spider, Spider):
            start_spider(spider, spider_starter, crontab=run_crontab)
    except StarterNotFound:
        error_msg = 'starter '+spider_starter+' doesn\'t exist in Spider '+spider_spider+'\n\nAvailable starter:\n  ' + '\n  '.join([ s.name for s in spider.get_starters()])
        parser.error(error_msg)
    except (InvalidCrawler, SpiderNotFound)  as e:
        parser.error(e)
    except Exception as e:
        raise(e)

# run subparser
parser = ArgumentParser('run', description='run a executable spider script', add_help=False)
parser.add_argument('-L', '--loglevel', default='INFO', type=str, help='log level', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])
parser.add_argument('--format', default=None, required=False, choices=['zip', 'tar', 'wheel', 'egg', 'py'], help='file format to be run, default to be python script')
parser.add_argument('--package_require', required=False)
parser.add_argument('--spider', required=False, default=None, help='name of crawler to be run. if none is specified, crawler name conincided with script name, without extension suffix,  will be used')
parser.add_argument('--starter', required=False, default="default", help='name of starter to be run. if none is specified, start_requests starter will be run')
parser.add_argument('--crontab', type=str, help='crontab format string')
# deprecated arguments
parser.add_argument('--interval', type=int, action=DeprecateAction, help='interval time for run, deprecated')
parser.add_argument('--logfile', default='', type=str, action=DeprecateAction, help='log file')
parser.add_argument('--logrotate', action=DeprecateStoreTrueAction, help='log rotate interval')
parser.add_argument('--logrotatetype', default='time', type=str, action=DeprecateAction, help='log rotate type', choices=['time', 'size', 'time_latest'])
parser.add_argument('--logrotatetime', default='H', type=str, action=DeprecateAction, help='log rotate time, only action when logrotatetype set to time', choices=['S', 'M', 'H', 'D', 'midnight'])
parser.add_argument('--logrotatedir', default='', type=str, action=DeprecateAction, help='log rotate directory')
parser.add_argument('--logrotateformat', default='', type=str, action=DeprecateAction, help='log rotate file format, several placeholders is supported, {year}, {month}, {day}, {hour}, {minute}, {second}, {logfile}')
parser.add_argument('spiderscript', help='a executable spider script')
mark_deprecated_help_strings(parser)
parser.set_defaults(exec=araneid_run_exec)