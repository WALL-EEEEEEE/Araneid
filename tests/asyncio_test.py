import asyncio

async def coro1():
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        print('coro1 canceled')

async def coro2():
    try:
        await asyncio.sleep(30)
        print('??????')
    except asyncio.CancelledError:
        print('coro2 canceled')

async def group():
    try:
        await asyncio.gather(asyncio.create_task(coro1()), asyncio.shield(asyncio.create_task(coro2())))
    except asyncio.CancelledError:
        print('group canceled')


async def main():
    async def cancel():
        await asyncio.sleep(3)
        group_task.cancel()

    group_task = asyncio.create_task(group())
    await asyncio.wait([group_task, cancel()], timeout=5)
    await asyncio.sleep(40)

asyncio.run(main())



