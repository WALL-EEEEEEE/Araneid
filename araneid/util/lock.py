from asyncio.locks import Lock as Lock_
from asyncio import futures
from collections import deque

class Lock(Lock_):

   def release(self):
        """Release a lock.

        When the lock is locked, reset it to unlocked, and return.
        If any other coroutines are blocked waiting for the lock to become
        unlocked, allow exactly one of them to proceed.

        When invoked on an unlocked lock, a RuntimeError is raised.

        There is no return value.
        """
        if self._locked:
            self._locked = False
            self._wake_up_last()
        else:
            raise RuntimeError('Lock is not acquired.')

   async def acquire(self):
        """Acquire a lock.

        This method blocks until the lock is unlocked, then sets it to
        locked and returns True.
        """
        if not self._locked:
            self._locked = True

        if self._waiters is None:
            self._waiters = deque()
        fut = self._loop.create_future()
        self._waiters.append(fut)

        # Finally block should be called before the CancelledError
        # handling as we don't want CancelledError to call
        # _wake_up_first() and attempt to wake up itself.
        try:
            try:
                await fut
            finally:
                self._waiters.remove(fut)
        except futures.CancelledError:
            if not self._locked:
                self._wake_up_last()
            raise
        self._locked = True
        return True

   def _wake_up_last(self):
        """Wake up the first waiter if it isn't done."""
        try:
            fut = next(reversed(self._waiters))
        except StopIteration:
            return

        # .done() necessarily means that a waiter will wake up later on and
        # either take the lock, or, if it was cancelled and lock wasn't
        # taken already, will hit this again and wake up a new waiter.
        if not fut.done():
            fut.set_result(True)

