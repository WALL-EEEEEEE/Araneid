import pytest
import asyncio
from datetime import datetime

def asyncron_runner_group():
    def no_concurrent():
        async def start_requests(self):
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - no_concurrent spider")

        def parse(self):
            pass
        def check():
            pass
        params = { key : value for key, value in locals().items()}
        return params

    cases = list(locals().items())
    params = {}
    for name, case in cases: 
        test_alia = name
        case_param = case()
        test_param = pytest.param(
            case_param.get('check', None),
            marks=[
                 pytest.mark.spider(
                     name= name,
                     start_requests=case_param.get('start_requests', None),
                     parse=case_param.get('parse', None)),
                 *case_param.get('mockers', lambda: [])(),
            ]) 
        params[test_alia] = test_param
    return params


async def timeout(coroutine, wait=30):
    return await asyncio.wait_for(asyncio.ensure_future(coroutine), timeout=wait)


@pytest.mark.asyncio
@pytest.mark.parametrize("check", list(asyncron_runner_group().values()), ids=list(asyncron_runner_group().keys()))
async def test_asyncron_runner(check, spider, asyncron_runner):
    await asyncron_runner.add_spider(spider, '*/1 * * * *')
    await asyncron_runner.start()
    if check:
       check()