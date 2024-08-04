from setuptools import setup, find_packages

core_plugin = {
    'araneid.downloader': [
        'Http=araneid.downloader.aiohttp:Http',
        'Socket=araneid.downloader.aiosocket:Socket',
        'WebSocket=araneid.downloader.aiowebsocket:WebSocket',
    ],
    'araneid.spider.router': [
        'LocalRouter=araneid.spider.routes.LocalSpiderRouter:LocalSpiderRouter'
    ],
    'araneid.scheduler': [
        'DefaultScheduler=araneid.scheduler.default:DefaultScheduler'
    ],
    'araneid.script': [
        'start=araneid.scripts.start:parser',
        'test=araneid.scripts.test:parser',
        'parse=araneid.scripts.parse:parser',
    ],
    'araneid.setting': [
        'araneid=araneid.settings'
    ]
}
def get_install_requires():
    with open('requirements.txt') as f:
        required = f.read().splitlines()
    return required

setup(
    name='araneid',
    version='v3.0.2rc6',
    author='Wall\'e',
    author_email='',
    packages=find_packages('.'),
    entry_points = {
        "console_scripts": [
            'araneid=araneid.scripts.araneid:main'
        ],
        **core_plugin,
    },
    data_files = [('.', ['requirements.txt'])],
    url='https://github.com/WALL-EEEEEEE/araneid',
    python_requires='>=python3.7',
    license='LICENSE.txt',
    description='Aranied is designed to be a  highly intergrated, configurable, and out of box crawler framework for data',
    long_description=open('README.rst', encoding='utf-8').read(),
    install_requires=get_install_requires(),
)
