import logging
import yaml
import os
from os.path import join, exists, isabs
from logging.config import dictConfig
from araneid.setting import settings, merge

def load_logger_config():
    project_logger_config_path = join(os.getcwd(), 'logging.yaml')
    config  = settings.get('logger', {})
    if exists(project_logger_config_path):
       with open(project_logger_config_path, 'r') as stream:
            project_config = yaml.load(stream, Loader=yaml.FullLoader)
            config = merge(config, project_config)
    return config

def getLogger(name):
    return logging.getLogger(name)

metas = {}

def config_log(level=None, metavars={}):
    global metas
    metas = metavars
    log_config = load_logger_config()
    root_logger = log_config.get('root', None)
    if root_logger:
       root_logger['level'] = level
    dictConfig(log_config)
