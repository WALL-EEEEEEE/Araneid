import os
import pytest
import sys
from os.path import join,dirname
from xprocess import  ProcessStarter

 

@pytest.fixture
def spider_starter(request, xprocess):
    spider_name = request.node.funcargs.get('spider')
    url = request.node.funcargs.get('url')
    scripts_directory = join(os.path.dirname(__file__), 'spiders')
    spider_script_path =join(scripts_directory, spider_name+'.py')
    executable_path = join(dirname(sys.executable), 'araneid')
    start_spider_cmd = f'{executable_path} run {spider_script_path} 11 22 33 44 test {url}'
    print(start_spider_cmd)
     
    class Starter(ProcessStarter):
        terminate_on_interrupt = True
        pattern = '.*'
        popen_kwargs = {
            "shell": True,
        }
        args = [start_spider_cmd]
    try:
        _, logfile = xprocess.ensure("spider_starter", Starter)
        with open(logfile, 'r+') as logreader:
             yield {'output': ''.join(logreader.readlines())}
    finally:
        xprocess.getinfo("spider_starter").terminate()