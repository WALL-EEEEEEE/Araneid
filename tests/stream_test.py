from contextlib import suppress
import math
from multiprocessing.sharedctypes import Value
from tokenize import group
import pytest
import asyncio
from araneid.core.stream import Stream

def data_generate(count=1):
    for v in range(1, count+1):
        yield v

test_stream_read_write_group = {
    'count=1': pytest.param(1, marks=pytest.mark.dependency(name='test_stream_read_write_1')),
    'count=1000': pytest.param(1000, marks=pytest.mark.dependency(name='test_stream_read_write_1000', depends=['test_stream_read_write_1'])),
}
test_stream_ack_group = {
    'ack': pytest.param(True, marks=pytest.mark.dependency(name='test_stream_ack')),
    'no_ack': pytest.param(False, marks=pytest.mark.dependency(name='test_stream_no_ack')),
}
test_stream_action_map_group = {
    'count=1': pytest.param(1, marks=pytest.mark.dependency(name='test_stream_action_map_1')),
    'count=1000': pytest.param(1000, marks=pytest.mark.dependency(name='test_stream_action_map_1000', depends=['test_stream_action_map_1'])),
}

def stream_set_exception_group():

    async def reader():
        async def publisher():
            for v in data:
                await stream.write(v)
            await stream.set_exception(exception('value error'))

        async def consumer():
            with pytest.raises(exception):
                async with stream.read() as reader:
                    async for v in reader:
                        res.append(v)
        res = []
        count = 10
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer())
        assert list(data_generate(count)) == res

    async def multi_reader():
        async def publisher():
            for v in data:
                await stream.write(v)
            await stream.set_exception(exception('value error'))

        async def consumer1():
            with pytest.raises(exception):
                async with stream.read() as reader:
                    async for v in reader:
                        res.append(v)

        async def consumer2():
            with pytest.raises(exception):
                async with stream.read() as reader:
                    async for v in reader:
                        res.append(v)
        res = []
        count = 10
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer1(), consumer2())
        assert list(data_generate(count)) == res

    async def partial_reader():
        async def publisher():
            for v in data:
                if v > partial_stop:
                   await stream.set_exception(exception('value error'))
                await stream.write(v)

        async def consumer():
            with pytest.raises(exception):
                async with stream.read() as reader:
                    async for v in reader:
                        res.append(v)
        res = []
        count = 10
        partial_stop = math.ceil(count/2)
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer())
        assert list(data_generate(count))[:partial_stop] == res

    async def multi_partial_reader():
        async def publisher():
            for v in data:
                if v > partial_stop:
                   await stream.set_exception(exception('value error'))
                await stream.write(v)

        async def consumer1():
            with pytest.raises(exception):
                async with stream.read() as reader:
                    async for v in reader:
                        res.append(v)

        async def consumer2():
            with pytest.raises(exception):
                async with stream.read() as reader:
                    async for v in reader:
                        res.append(v)
        res = []
        count = 10
        partial_stop = math.ceil(count/2)
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer1(), consumer2())
        assert list(data_generate(count))[:partial_stop] == res



    async def get():
        async def publisher():
            for v in data:
                await stream.write(v)
            await stream.set_exception(exception('value error'))

        async def consumer():
            with pytest.raises(exception):
                while not stream.is_closed():
                   v =  await stream.get()
                   res.append(v)
        res = []
        count = 10
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer())
        assert list(data_generate(count)) == res

    async def multi_get():
        async def publisher():
            for v in data:
                await stream.write(v)
            await stream.set_exception(exception('value error'))

        async def consumer1():
            with pytest.raises(exception):
                while not stream.is_closed():
                   v =  await stream.get()
                   res.append(v)
 
        async def consumer2():
            with pytest.raises(exception):
                while not stream.is_closed():
                   v =  await stream.get()
                   res.append(v)
 
        res = []
        count = 10
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer1(), consumer2())
        assert list(data_generate(count)) == res

    async def partial_get():
        async def publisher():
            for v in data:
                if v > partial_stop:
                   await stream.set_exception(exception('value error'))
                await stream.write(v)

        async def consumer():
            with pytest.raises(exception):
                while not stream.is_closed():
                   v =  await stream.get()
                   res.append(v)
        res = []
        count = 10
        partial_stop = math.ceil(count/2)
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer())
        assert list(data_generate(count))[:partial_stop] == res

    async def multi_partial_get():
        async def publisher():
            for v in data:
                if v > partial_stop:
                   await stream.set_exception(exception('value error'))
                await stream.write(v)
            await stream.set_exception(exception('value error'))

        async def consumer1():
            with pytest.raises(exception):
                while not stream.is_closed():
                   v =  await stream.get()
                   res.append(v)
 
        async def consumer2():
            with pytest.raises(exception):
                while not stream.is_closed():
                   v =  await stream.get()
                   res.append(v)
 
        res = []
        count = 10
        partial_stop = math.ceil(count/2)
        exception = ValueError
        data = data_generate(count)
        stream = Stream()
        await asyncio.gather(publisher(), consumer1(), consumer2())
        assert list(data_generate(count))[:partial_stop] == res

 

    operators = list(locals().items())
    test_params = {}
    for name, operator in operators: 
        test_alia = name
        test_param = pytest.param(operator) 
        test_params[test_alia] = test_param
    return test_params


@pytest.mark.parametrize('count', list(test_stream_read_write_group.values()), ids=list(test_stream_read_write_group.keys()))
@pytest.mark.asyncio
async def test_stream_async_with_read_write(count):
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()

    async def consumer():
        async with stream.read() as reader:
            async for v in reader:
                res.append(v)
    res = []
    data = data_generate(count)
    stream = Stream()
    await asyncio.gather(publisher(), consumer())
    assert list(data_generate(count)) == res

@pytest.mark.parametrize('count', list(test_stream_read_write_group.values()), ids=list(test_stream_read_write_group.keys()))
@pytest.mark.asyncio
async def test_stream_read_write(count):
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()

    async def consumer():
        async for v in stream.read():
            res.append(v)
    res = []
    data = data_generate(count)
    stream = Stream()
    await asyncio.gather(publisher(), consumer())
    assert list(data_generate(count)) == res



@pytest.mark.parametrize('ack', list(test_stream_ack_group.values()), ids=list(test_stream_ack_group.keys()))
@pytest.mark.asyncio
async def test_stream_ack(ack):
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()
    async def consumer():
        async for v in stream.read():
            res.append(v)
            if ack:
               stream.ack(v)
    async def run():
        group_task = asyncio.gather(publisher(), consumer())
        if not ack:
            try:
                await asyncio.wait_for(group_task, timeout=10)
            except Exception as e:
                raise e
            finally:
                with suppress(asyncio.CancelledError):
                    await group_task
        else:
            await group_task

    res = []
    data = data_generate(1)
    stream = Stream(confirm_ack=True)
    if not ack:
       with pytest.raises(asyncio.TimeoutError):
            await run()
    else:
        await run()
        assert list(data_generate(1)) == res

@pytest.mark.parametrize('ack', list(test_stream_ack_group.values()), ids=list(test_stream_ack_group.keys()))
@pytest.mark.asyncio
async def test_stream_async_with_ack(ack):
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()
    async def consumer():
        async with stream.read() as reader:
            async for v in reader:
                res.append(v)
                if ack:
                   stream.ack(v)
    async def run():
        group_task = asyncio.gather(publisher(), consumer())
        if not ack:
            try:
                await asyncio.wait_for(group_task, timeout=10)
            except Exception as e:
                raise e
            finally:
                with suppress(asyncio.CancelledError):
                    await group_task
        else:
            await group_task

    res = []
    data = data_generate(1)
    stream = Stream(confirm_ack=True)
    if not ack:
       with pytest.raises(asyncio.TimeoutError):
            await run()
    else:
        await run()
        assert list(data_generate(1)) == res



@pytest.mark.parametrize('count', list(test_stream_action_map_group.values()), ids=list(test_stream_action_map_group.keys()))
@pytest.mark.asyncio
async def test_stream_action_map(count):
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()
    async def consumer():
        async for v in stream.read():
            res.append(v)
    res = []
    data = data_generate(count)
    stream = Stream()
    stream.map(lambda v: v*2)
    await asyncio.gather(publisher(), consumer())
    assert list(map(lambda v: v*2, data_generate(count))) == res

@pytest.mark.parametrize('count', list(test_stream_action_map_group.values()), ids=list(test_stream_action_map_group.keys()))
@pytest.mark.asyncio
async def test_stream_async_with_action_map(count):
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()
    async def consumer():
        async with stream.read() as reader:
            async for v in reader:
                res.append(v)
    res = []
    data = data_generate(count)
    stream = Stream()
    stream.map(lambda v: v*2)
    await asyncio.gather(publisher(), consumer())
    assert list(map(lambda v: v*2, data_generate(count))) == res


@pytest.mark.asyncio
async def test_stream_async_with_half_read_before_close():
    async def publisher():
        for v in data:
            await stream.write(v)
    async def consumer():
        async with stream.read() as reader:
            async for v in reader:
                res.append(v)
                break
        await stream.close()

    res = []
    data = data_generate(2)
    stream = Stream()
    await asyncio.gather(publisher(), consumer())
    assert [1] == res

@pytest.mark.asyncio
async def test_stream_half_read_before_close():
    async def publisher():
        for v in data:
            await stream.write(v)
    async def consumer():
        async for v in stream.read():
            res.append(v)
            break
        await stream.close()

    res = []
    data = data_generate(2)
    stream = Stream()
    await asyncio.gather(publisher(), consumer())
    assert [1] == res

@pytest.mark.asyncio
async def test_stream_read_after_close():
    async def consumer():
        await stream.close()
        async for v in stream.read():
            res.append(v)

    res = []
    stream = Stream()
    await stream.write(1)
    await asyncio.gather(consumer())
    assert [] == res

@pytest.mark.asyncio
async def test_stream_read_after_close():
    async def consumer():
        await stream.close()
        async for v in stream.read():
            res.append(v)
    res = []
    stream = Stream()
    await stream.write(1)
    await asyncio.gather(consumer())
    assert [] == res

@pytest.mark.asyncio
async def test_stream_write_after_close():
    stream = Stream()
    await stream.write(1)
    await stream.close()
    await stream.write(2)

@pytest.mark.asyncio
async def test_stream_multi_read():
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()
    async def consumer1():
        async for v in stream.read():
            res.append(v)
    async def consumer2():
        async for v in stream.read():
            res.append(v)
    res = []
    data = data_generate(2)
    stream = Stream()
    await asyncio.gather(publisher(), consumer1(), consumer2())
    assert [1,2] == res

@pytest.mark.asyncio
async def test_stream_async_with_multi_read():
    async def publisher():
        for v in data:
            await stream.write(v)
        await stream.join()
        await stream.close()
    async def consumer1():
        async with stream.read() as reader:
            async for v in reader:
                res.append(v)
    async def consumer2():
        async with stream.read() as reader:
            async for v in reader:
                res.append(v)
    res = []
    data = data_generate(2)
    stream = Stream()
    await asyncio.gather(publisher(), consumer1(), consumer2())
    assert [1,2] == res



@pytest.mark.parametrize('operator', list(stream_set_exception_group().values()), ids=list(stream_set_exception_group().keys()))
@pytest.mark.asyncio
async def test_stream_set_exception(operator):
    await operator()

@pytest.mark.skip()
def test_stream_route():
    pass