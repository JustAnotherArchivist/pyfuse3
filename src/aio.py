'''
aio.py

asyncio/Trio compatibility layer for pyfuse3

Copyright © 2018 JustAnotherArchivist, Nikolaus Rath <Nikolaus.org>

This file is part of pyfuse3. This work may be distributed under
the terms of the GNU LGPL.
'''

import asyncio
import contextlib
import sys
try:
	import trio
except ImportError:
	trio = None


_aio = 'trio' # Possible values: 'asyncio' or 'trio'
_read_lock = None


# Get the name of the current task (launched through nursery.start_soon)
def get_task_name():
	if _aio == 'asyncio':
		if sys.version_info < (3, 7):
			return asyncio.Task.current_task().name
		else:
			return asyncio.current_task().name
	else:
		return trio.hazmat.current_task().name


# Wait for file descriptor to become readable
async def wait_fuse_readable_trio(session_fd):
	await trio.hazmat.wait_readable(session_fd)

async def wait_fuse_readable_asyncio(session_fd):
	future = asyncio.Future()
	loop = asyncio.get_event_loop()
	loop.add_reader(session_fd, future.set_result, None)
	future.add_done_callback(lambda f: loop.remove_reader(session_fd))
	await future

async def wait_fuse_readable(worker_data, session_fd):
	#name = get_task_name()
	worker_data.active_readers += 1
	#log.debug('%s: Waiting for read lock...', name)
	async with _read_lock:
		#log.debug('%s: Waiting for fuse fd to become readable...', name)
		if _aio == 'asyncio':
			await wait_fuse_readable_asyncio(session_fd)
		else:
			await wait_fuse_readable_trio(session_fd)
	worker_data.active_readers -= 1
	#log.debug('%s: fuse fd readable, unparking next task.', name)


# Nursery
class AsyncioNursery:
	async def __aenter__(self):
		self.tasks = set()
		return self

	def start_soon(self, func, *args, name = None):
		if sys.version_info < (3, 7):
			task = asyncio.ensure_future(func(*args))
		else:
			task = asyncio.create_task(func(*args))
		task.name = name
		self.tasks.add(task)

	async def __aexit__(self, exc_type, exc_value, traceback):
		# Wait for tasks to finish
		while len(self.tasks):
			done, pending = await asyncio.wait(tuple(self.tasks)) # Create a copy of the task list to ensure that it's not a problem when self.tasks is modified
			for task in done:
				self.tasks.discard(task)
			for task in pending:
				self.tasks.add(task) #TODO: Should not be necessary; verify

def open_nursery():
	if _aio == 'asyncio':
		return AsyncioNursery()
	else:
		return trio.open_nursery()


# Management
def set_aio(aio):
	if aio not in ('asyncio', 'trio'):
		raise ValueError('Invalid aio')
	if aio == 'trio' and trio is None:
		raise RuntimeError('trio unavailable')
	global _aio, _read_lock
	_aio = aio
	_read_lock = asyncio.Lock() if aio == 'asyncio' else trio.Lock()
