"""
@author: Wall\'e
@mail:   
@date:   2019.11.06
"""
import crython
from crython.expression import CronExpression, field


class CrontabUnitError(Exception):
    pass


class CrontabJob:

    @property
    def cron_expression(self):
        return self.crontab_expression

    def __init__(self, name=None, **kwargs):
        kwargs = dict((k, kwargs.pop(k)) for k in list(kwargs.keys()) if k in field.NAMES)
        self.crontab_expression = CronExpression.from_kwargs(**kwargs)
        self.ctx = crython.tab.DEFAULT_EXECUTION_CONTEXT
        self.name = name

    @staticmethod
    def from_expr(expr, name=None):
        crontab_job = CrontabJob(name)
        crontab_job.crontab_expression = CronExpression.new(expr)
        return crontab_job
