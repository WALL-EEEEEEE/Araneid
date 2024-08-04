import inspect
import os
import pytest
import pytest_asyncio
from unittest.mock import patch
from aioresponses import aioresponses
from xprocess import ProcessStarter
from araneid.core import plugin
from araneid.core.signal import SignalManager
from araneid.runner import AsyncRunner, AsyncCronRunner
from araneid.spider.spider import Spider

active_entrypoints = plugin.list_active_plugins([plugin.PluginType.DOWNLOADMIDDLEWARE, plugin.PluginType.EXTENSION, plugin.PluginType.SPIDERMIDDLEWARE])
class MockSpider(Spider):
    name = 'mock_spider'

    def start_requests(self):
        pass

    def parse(self, response ):
        pass

def process_entry_points(entry_points, mode):
    def __ensure_entry_point(group, entry):
        if inspect.isclass(entry):
           plugin_entry = plugin.PluginEntry.from_class(entry, group=group)
        elif isinstance(entry, str):
           plugin_entry = plugin.PluginEntry.from_schema(entry, group=group)
        else:
           plugin_entry = entry
        return plugin_entry
    plugin_entries = [ __ensure_entry_point(group, entry) for group, entries  in entry_points.items() for entry in entries]
    if mode == 'exclude':
       [ plugin.disable(plugin.PluginType(plugin_entry.group), plugin_entry) for plugin_entry in plugin_entries] 
    elif mode == 'include':
       [ plugin.register(plugin.PluginType(plugin_entry.group), plugin_entry) for plugin_entry in plugin_entries] 
    elif mode == 'only':
       for plugin_entry in active_entrypoints:
           if plugin_entry not in plugin_entries:
              plugin.disable(plugin.PluginType(plugin_entry.group), plugin_entry)

@pytest.fixture
def aioresponse():
    with aioresponses() as m:
        yield m

@pytest_asyncio.fixture
async def async_runner():
    runner = await AsyncRunner.create()
    yield runner

@pytest_asyncio.fixture()
async def asyncron_runner():
    runner = await AsyncCronRunner.create()
    yield runner
 

@pytest.fixture()
def spider(request):
    mark = request.node.get_closest_marker("spider")
    if not mark:
       pytest.skip(f'@pytest.mark.spider must specified!')
    params = mark.kwargs
    spider_name = params.get('name', None)
    spider_start_requests = params.get('start_requests', lambda self: None)
    spider_parse = params.get('parse', lambda self, response: None)
    if not spider_name:
       pytest.skip(f'@pytest.mark.spider must use with a name param!')
    patch.object(MockSpider, 'start_requests', new=spider_start_requests).start()
    patch.object(MockSpider, 'parse', new=spider_parse).start()
    spider = MockSpider
    spider.name = spider_name
    spider_args = spider.metas.get('args', {})
    setattr(spider_args, '__araneid_project_id', '11')
    setattr(spider_args, '__araneid_job_id', '22')
    setattr(spider_args, '__araneid_job_record_id', '33')
    setattr(spider_args, '__araneid_task_id', '44')
    setattr(spider_args, '__araneid_task_code', 'test')
    spider.metas['args'] = spider_args
    yield spider
    patch.stopall()

@pytest.fixture()
def mocker(request):
    mockers = request.node.iter_markers(name="mocker")
    for  mocker in mockers:
      params = mocker.kwargs
      object = params.get('object', None)
      attribute = params.get('attribute', None)
      new = params.get('new', None)
      if not (object and attribute and new):
         pytest.skip(f'@pytest.mark.spider must use with a (object, attribute, new) param!')
      patch.object(object, attribute, new=new).start()
    yield 
    patch.stopall()


@pytest.fixture
def script_runner(request, xprocess):
    test_dir = os.path.dirname(request.module.__file__)
    script_marker = request.node.get_closest_marker("script")
    if script_marker is None:
       pytest.skip('No scripts are set up by @pytest.mark.script !')
    script = script_marker.kwargs.get('script', None)
    script_params = script_marker.kwargs.get('params', {})
    script_pattern = script_marker.kwargs.get('partern', '.*')
    script_env = script_marker.kwargs.get('env', None)
    script_path = os.path.join(test_dir, script)
    if not script:
       pytest.skip('No scripts are set up by @pytest.mark.script !')
    
    start_script_cmd = f'cd {os.path.dirname(script_path)}; go run {script_path} {"".join([ f"{key}={value} " for key, value in  script_params.items() ])}'

    class ScriptStarter(ProcessStarter):
        terminate_on_interrupt = True
        pattern = script_pattern
        popen_kwargs = {
            "shell": True,
        }
        env = script_env
        args = (start_script_cmd, )

    try:
        server_process_pid, _= xprocess.ensure('script_starter', ScriptStarter)
        yield 
    finally:
        xprocess.getinfo("script_starter").terminate()
 
    
def pytest_runtest_setup(item):
    for mark in item.iter_markers(name="entrypoints"):
        entry_points = mark.kwargs.get('entry_points', None)
        mode = mark.kwargs.get('mode', 'only')
        if mode not in ['include', 'exclude', 'only']:
           pytest.skip('@pytest.mark.entrypoints mark mode param must in  ["include", "exclude", "only"]!')
        if not entry_points:
           continue
        if not isinstance(entry_points, dict):
           pytest.skip('@pytest.mark.entrypoints mark must accept a dict entrypoints as parameter!')
        process_entry_points(entry_points, mode)



   
