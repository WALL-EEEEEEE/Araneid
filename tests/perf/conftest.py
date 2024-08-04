import logging
from itertools import count
from operator import itemgetter
import sys
import multiprocessing
import inspect
import os
import pytest
import pandas 
import psutil
import threading
import json
import time
from pathlib import Path
from random import randint
from xprocess import  ProcessStarter
from araneid.spider.spider import Spider

logger = logging.getLogger(__name__)

class Collector:
    __metrics__ = None
    __dir = os.path.join(Path(__file__).parent.parent.parent, 'docs/data/perf')

    def __init__(self) -> None:
       self.__metrics__ = {}
    
    def collect(self, metric, data):
        self.__metrics__[metric] = pandas.DataFrame.from_dict(data, orient='index')
    
    def get(self, metric):
        return self.__metrics__.get(metric, None)
    
    def average(self, metric):
        metric_data = self.get(metric)
        if metric_data is None:
           return 0
        
    def set_dir(self, directory):
        self.__dir = directory
    
    def total(self, metric):
        metric_data = self.get(metric)
        if metric_data is None:
            return 0
    
    def to_csv(self, path):
        merge_df = pandas.concat(self.__metrics__)
        path = os.path.join(self.__dir, path+'.csv')
        merge_df.to_csv(path)
    
    def to_excel(self, path):
        merge_df = pandas.concat(self.__metrics__)
        path = os.path.join(self.__dir, path+'.xlsx')
        merge_df.to_excel(path)

class PerfCollector():

    def __init__(self, time=60, times=6) -> None:
        self.__mem_metrics__ = []
        self.__cpu_metrics__ = []
        self.__sample_interval__ = time/times
        self.__time__ = time
        self.__num__ = times
        self._process = None
    
    def __collect__(self, pid=None):
        start_time = time.time()
        if not pid:
           pid = os.getpid()
        proc = psutil.Process(pid)
        while proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
            mem_stats = proc.memory_info().rss/1024/1024
            cpu_stats = proc.cpu_percent(interval=1)
            self.__mem_metrics__.append(mem_stats)
            self.__cpu_metrics__.append(cpu_stats)
            self.__mem_metrics__ = sorted(self.__mem_metrics__, reverse=True)[0:self.__num__]
            self.__cpu_metrics__ = sorted(self.__cpu_metrics__, reverse=True)[0:self.__num__]
    
    def start(self, pid=None):
        _process = threading.Thread(target=self.__collect__, args=(pid,))
        _process.start()
        self._process = _process
    
    def join(self):
        self._process.join()
    
    def memory(self):
        if isinstance(self._process, multiprocessing.Process) and self._process.is_alive():
           self._process.kill()
        self._process.join()
        return  self.__mem_metrics__
    
    def cpu(self):
        if isinstance(self._process, multiprocessing.Process) and self._process.is_alive():
           self._process.kill()
        self._process.join()
        return  self.__cpu_metrics__


 

@pytest.fixture
def websocket_server(request, xprocess):
    port = randint(5000, 9999)
    server = f'ws://127.0.0.1:{port}'
    count = request.node.funcargs.get('count')
    cost = request.node.funcargs.get('cost')
    scripts_directory = os.path.join(os.path.dirname(__file__), 'scripts')
    class Starter(ProcessStarter):
        terminate_on_interrupt = True
        pattern = '.*开启WebSocket服务.*'
        args = ("sh", "-c", f"cd {scripts_directory}; go run src/websocket_server.go --port {port} --count {count} --cost {cost}")
    try:
        xprocess.ensure("websocket_server", Starter)
        yield {'websocket_server_url':server}
    finally:
        xprocess.getinfo("websocket_server").terminate()


@pytest.fixture
def http_server(request, xprocess):
    port = randint(5000, 9999)
    server = f'http://127.0.0.1:{port}'
    scripts_directory = os.path.join(os.path.dirname(__file__), 'scripts')
    payload = {'status': 'ok', 'content': 'success'}
    class Starter(ProcessStarter):
        terminate_on_interrupt = True
        pattern = '.*开启Http服务.*'
        args = ("sh", "-c", f"cd {scripts_directory}; go run src/http_server.go --port {port} --json '{json.dumps(payload)}'")
    try:
        xprocess.ensure("http_server", Starter)
        yield {'http_server_url':server}
    finally:
        xprocess.getinfo("http_server").terminate()


@pytest.fixture
def spider_perf_starter(request, xprocess):
    port = randint(5000, 9999)
    sample_times = 6
    param = request.node.funcargs.get('param', {})
    spider_cls = request.node.funcargs.get('spider', None)
    runtime = param.get('runtime', 60)
    spider_perf_type = param.get('spider_perf_type', None)
    count = param.get('count', None)
    connections = param.get('connections', None)
    cost = runtime if not param.get('cost', None) else param.get('cost')
    interval = param.get('interval', None)
    scripts_directory = os.path.join(os.path.dirname(__file__), 'scripts')
    payload = {'status': 'ok', 'content': 'success'}
    spider_script = inspect.getfile(spider_cls)
    spider_name = spider_cls.__name__
    araneid_executor = os.path.join(sys.exec_prefix, 'bin', 'araneid')
    start_spider_cmd = [f"{araneid_executor}",'run', spider_script, '--spider', spider_name, 11, 22,33,44, 'test', '']
    if not spider_perf_type:
       pytest.skip("spider_perf_type not specified!")
    if not spider_cls:
       pytest.skip("spider not specified!")
    elif not (inspect.isclass(spider_cls) and issubclass(spider_cls, Spider)):
       pytest.skip(f"{spider_cls} is a valid spider class!")

    if spider_perf_type == 'http':
       spider_pattern = '.*启动http_spider.*'
       server_pattern = '.*开启Http服务.*'
       spider_params = json.dumps({'url': f'http://127.0.0.1:{port}', 'count': count, 'interval': interval})
       spider_starter_pattern = spider_pattern
       spider_starter_env = {'spider_params': spider_params}
       start_server_cmd = f"cd {scripts_directory}; go run src/http_server.go --port {port} --json '{json.dumps(payload)}'"
       server_starter_args = ("sh", "-c", start_server_cmd)
       server_starter_pattern = server_pattern
    elif spider_perf_type == 'websocket':
       spider_pattern = '.*启动websocket_spider.*'
       server_pattern = '.*开启WebSocket服务.*'
       start_server_cmd = f"cd {scripts_directory}; go run src/websocket_server.go --port {port} --count {count} --cost {cost}"
       spider_params = json.dumps({'url': f'ws://127.0.0.1:{port}', 'count': count*connections, 'cost': cost, 'connections': connections})
       spider_starter_pattern = spider_pattern
       spider_starter_env = {'spider_params': spider_params}
       server_starter_args  = ("sh", "-c", start_server_cmd)
       server_starter_pattern = server_pattern
    else:
       pytest.skip(f"spider_perf_type {spider_perf_type} is not supprotied!")

    class ServerStarter(ProcessStarter):
        terminate_on_interrupt = True
        pattern = server_starter_pattern
        args = server_starter_args

    class SpiderStarter(ProcessStarter):
        terminate_on_interrupt = True
        pattern = spider_starter_pattern
        env = spider_starter_env
        args = (*start_spider_cmd, )

    try:
        collector = PerfCollector(time=runtime, times=sample_times)
        server_process_pid, _= xprocess.ensure('server_starter', ServerStarter)
        spider_process_pid, _=xprocess.ensure("spider_starter", SpiderStarter)
        collector.start(pid=spider_process_pid)
        yield collector
        collector.join()
        #logger.info(log.readlines())
    finally:
        xprocess.getinfo('server_starter').terminate()
        xprocess.getinfo("spider_starter").terminate()


@pytest.fixture
def perf_load(request):
    sample_times = 6
    runtime = request.node.funcargs.get('runtime', 60)
    collector = PerfCollector(time=runtime, times=sample_times)
    collector.start(pid=os.getpid())
    yield  collector
           

@pytest.fixture
def perf_metrics_collector(request):
    collector = Collector()
    yield collector
    testcase = request.node.name
    collector.to_excel(testcase)