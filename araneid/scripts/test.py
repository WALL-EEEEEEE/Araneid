import argparse
import os
import sys
import araneid
from os.path import basename
from araneid.logger import config_log
from araneid.core.exception import InvalidCrawler, SpiderNotFound, StarterNotFound
from araneid.spider.spider import Spider, Starter
from araneid.tools.package import get_spider_from_wheel, install_package
from araneid.annotation import CrawlerRunner as CrawlerRunnerAnnotation, Argument, Option
from araneid.runners import CrawlerCliRunner
from . import auto_detect_script_format, get_spider_from_py, start_spider 

def araneid_test_exec(parsed_args, unused_args):
    araneid.runners.SCRIPT_RUNNABLE = True
    spider_script = parsed_args.spiderscript
    spider_starter = parsed_args.starter
    spider_spider = parsed_args.spider
    # run timer settings
    run_interval = parsed_args.interval
    run_crontab = parsed_args.crontab
    # log settings
    log_level = parsed_args.loglevel
    log_file = parsed_args.logfile
    log_rotate = parsed_args.logrotate
    log_rotate_type = parsed_args.logrotatetype
    log_rotate_time = parsed_args.logrotatetime
    log_rotate_size = parsed_args.logrotatesize
    log_rotate_dir = parsed_args.logrotatedir
    log_rotate_format = parsed_args.logrotateformat
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

    CrawlerCliRunner.crontab = run_crontab
    CrawlerCliRunner.interval = run_interval
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
        if isinstance(spider, CrawlerRunnerAnnotation):
            spider.run()
        elif isinstance(spider, Spider):
            runner = CrawlerCliRunner()
            start_spider(spider, spider_starter, runner)
    except StarterNotFound:
            error_msg = 'starter '+spider_starter+' doesn\'t exist in crawler '+spider_spider+'\n\nAvailable starter:\n  ' + '\n  '.join([ s.name for s in spider.get_starters()])
            parser.error(error_msg)
    except (InvalidCrawler, SpiderNotFound)  as e:
        parser.error(e)
    except Exception as e:
        raise(e)

# test subparser
parser = argparse.ArgumentParser(prog="test", add_help=False, description="test run spiderscript.")
parser.add_argument('--format', default=None, required=False, choices=['zip', 'tar', 'wheel', 'egg', 'py'], help='file format to be test, default to be python script')
parser.add_argument('--package_require', required=False)
parser.add_argument('--spider', required=False, default=None, help='name of crawler to be test. if none is specified, crawler name conincided with script name, without extension suffix,  will be used')
parser.add_argument('--starter', required=False, default="default", help='name of starter to be test. if none is specified, start_requests starter will be test')
parser.add_argument('--interval', type=int, help='interval time for test')
parser.add_argument('--crontab', type=str, help='crontab format string')
parser.add_argument('--loglevel', default='', type=str, help='log level', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])
parser.add_argument('--logfile', default='', type=str, help='log file')
parser.add_argument('--logrotate', action='store_true', help='log rotate interval')
parser.add_argument('--logrotatetype', default='time', type=str, help='log rotate type', choices=['time', 'size', 'time_latest'])
parser.add_argument('--logrotatetime', default='H', type=str, help='log rotate time, only action when logrotatetype set to time', choices=['S', 'M', 'H', 'D', 'midnight'])
parser.add_argument('--logrotatesize', default=3600, type=int, help='log rotate size, only action when logrotatetype set to size')
parser.add_argument('--logrotatedir', default='', type=str, help='log rotate directory')
parser.add_argument('--logrotateformat', default='', type=str, help='log rotate file format, several placeholders is supported, {year}, {month}, {day}, {hour}, {minute}, {second}, {logfile}')
parser.add_argument('spiderscript', help='a executable spider script')
parser.set_defaults(exec=araneid_test_exec)