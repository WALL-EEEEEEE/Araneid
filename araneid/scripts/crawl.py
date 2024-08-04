from araneid.runners import CrawlerRunner
import sys
from argparse import ArgumentParser
from os.path import basename, dirname, splitext
from araneid.scripts import check_if_valid_runner, check_if_valid_spider_script, start_spider, resolve_dependences


def araneid_crawl_exec(parsed_args, unused_args):
    spider_script = parsed_args.spiderscript
    crawler = parsed_args.crawler
    runner = parsed_args.runner
    if not check_if_valid_spider_script(spider_script):
        parser.error(spider_script + ' is not a valid spider script.')
    if runner:
        if not check_if_valid_runner(runner):
            parser.error(runner + ' is not a valid runner')
        else:
            runner = getattr(runners, runner)

    spider_name = splitext(basename(spider_script))[0]
    spider_path = dirname(spider_script)
    resolve_dependences(spider_path)
    script_args = unused_args
    script_args.insert(0, spider_script)
    sys.argv = script_args
    if not crawler:
        crawler = spider_name
    __default_spider_module__ = spider_name
    try:
        __spider_module__ = __import__(__default_spider_module__)
        __crawler_class__ = getattr(__spider_module__, crawler, None)
        # some crawler may be decorated by runner, then runner is be reused
        if issubclass(__crawler_class__.__class__, CrawlerRunner):
            runner = __crawler_class__ if runner is None or type(__crawler_class__) is type(runner) else runner
            __crawler_class__ = __crawler_class__.crawlers
            runner.crawlers = []
        if not __crawler_class__:
            parser.error('crawler ' + crawler + ' doesn\'t exist in spider script' + spider_script + '.')
        else:
            runner = runner() if isinstance(runner, type) else runner
            start_spider(__crawler_class__, runner=runner)

    except ImportError:
        parser.error(spider_script + ' doesn\'t exists.')


# crawl subparser
parser = ArgumentParser('crawl', description='crawl a spider', add_help=False)
parser.add_argument('--runner', required=False, help='runner to start crawler')
parser.add_argument('--spider', required=False, help='spider in spider script needed to be executed')
parser.add_argument('spiderscript', help='spider script need to be run')
parser.set_defaults(exec=araneid_crawl_exec)