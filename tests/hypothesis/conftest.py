import pytest
import trio
import queue
from functools import partial
from contextlib import contextmanager
from hypothesis.stateful import run_state_machine_as_test, RuleBasedStateMachine


class ThreadToTrioCommunicator:
    def __init__(self, portal, timeout=None):
        self.timeout = timeout
        self.portal = portal
        self.queue = queue.Queue()
        self.trio_queue = trio.Queue(1)

    def send(self, msg):
        self.portal.run(self.trio_queue.put, msg)
        ret = self.queue.get(timeout=self.timeout)
        if isinstance(ret, Exception):
            raise ret

        return ret

    async def trio_recv(self):
        ret = await self.trio_queue.get()
        return ret

    async def trio_respond(self, msg):
        self.queue.put(msg)

    def close(self):
        # Avoid deadlock if somebody is waiting on the other end
        self.queue.put(RuntimeError("Communicator has closed while something was still listening"))


@contextmanager
def open_communicator(portal):
    communicator = ThreadToTrioCommunicator(portal)
    try:
        yield communicator

    except Exception as exc:
        # Pass the exception to the listening part, to have the current
        # hypothesis rule crash correctly
        communicator.queue.put(exc)
        raise

    finally:
        communicator.close()


@pytest.fixture
async def portal():
    return trio.BlockingTrioPortal()


@pytest.fixture
async def TrioDriverRuleBasedStateMachine(nursery, portal, loghandler, hypothesis_settings):
    class TrioDriverRuleBasedStateMachine(RuleBasedStateMachine):
        _portal = portal
        _nursery = nursery
        _running = trio.Lock()

        @classmethod
        async def run_test(cls):
            await trio.run_sync_in_worker_thread(
                partial(run_state_machine_as_test, cls, settings=hypothesis_settings)
            )

        async def trio_runner(self, task_status):
            raise NotImplementedError()

        @property
        def communicator(self):
            assert self._communicator
            return self._communicator

        async def _trio_runner(self, *, task_status=trio.TASK_STATUS_IGNORED):
            print("=====================================================")
            # We need to hijack `task_status.started` callback because error
            # handling of trio_runner coroutine depends of it (see below).
            task_started = False
            vanilla_task_status_started = task_status.started

            def task_status_started_hook(ret=None):
                nonlocal task_started
                task_started = True
                vanilla_task_status_started(ret)

            task_status.started = task_status_started_hook

            # Drop previous run logs, preventing flooding stdout
            loghandler.records.clear()
            try:
                # This lock is to make sure the hypothesis thread doesn't start
                # another `_trio_runner` coroutine while this one hasn't done
                # it teardown yet.
                async with self._running:
                    with trio.open_cancel_scope() as self._trio_runner_cancel_scope:
                        with open_communicator(self._portal) as self._communicator:
                            await self.trio_runner(task_status)
            except Exception as exc:
                if not task_started:
                    # If the crash occurs during the init phase, hypothesis
                    # thread is synchrone with this coroutine so raising the
                    # exception here will have the expected effect.
                    raise

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._trio_runner_crash = None
            self._portal.run(self._nursery.start, self._trio_runner)

        def teardown(self):
            self._trio_runner_cancel_scope.cancel()

    return TrioDriverRuleBasedStateMachine


@pytest.fixture
def oracle_fs_factory(tmpdir):
    from pathlib import Path

    class FSOracle:
        def __init__(self, root_path):
            self.base_path = Path(root_path)
            self.root_path = self.base_path / "root"
            self.root_path.mkdir(parents=True)
            self.base_path.chmod(0o500)  # Root oracle can no longer be removed this way

        def create_file(self, path):
            assert path.startswith("/")
            path = self.root_path / path[1:]
            try:
                path.touch(exist_ok=False)
            except OSError as exc:
                return "invalid_path"
            return "ok"

        def create_folder(self, path):
            assert path.startswith("/")
            path = self.root_path / path[1:]
            try:
                path.mkdir()
            except OSError as exc:
                return "invalid_path"
            return "ok"

        def delete(self, path):
            assert path.startswith("/")
            path = self.root_path / path[1:]
            try:
                if path.is_file():
                    path.unlink()
                else:
                    path.rmdir()
            except OSError as exc:
                return "invalid_path"
            return "ok"

        def move(self, src, dst):
            assert src.startswith("/")
            src = self.root_path / src[1:]
            assert dst.startswith("/")
            dst = self.root_path / dst[1:]
            try:
                src.rename(str(dst))
            except OSError as exc:
                return "invalid_path"
            return "ok"

    count = 0

    def _oracle_fs_factory():
        nonlocal count
        count += 1
        return FSOracle(Path(tmpdir / f"fs_oracle-{count}"))

    return _oracle_fs_factory
