import pytest
from hypothesis import strategies as st, note
from hypothesis.stateful import rule

from parsec.utils import to_jsonb64, from_jsonb64

from tests.common import (
    connect_core, core_factory, backend_factory, run_app
)
from tests.hypothesis.conftest import skip_on_broken_stream


class FileOracle:
    def __init__(self):
        self._buffer = bytearray()

    def read(self, size, offset):
        return self._buffer[offset:size + offset]

    def write(self, offset, content):
        self._buffer[offset:len(content) + offset] = content


@pytest.mark.slow
@pytest.mark.trio
async def test_online(
    TrioDriverRuleBasedStateMachine,
    mocked_local_storage_connection,
    tcp_stream_spy,
    backend_addr,
    tmpdir,
    alice
):

    class CoreOnline(TrioDriverRuleBasedStateMachine):
        count = 0

        async def trio_runner(self, task_status):
            mocked_local_storage_connection.reset()
            type(self).count += 1
            backend_config = {
                'blockstore_url': 'backend://',
            }
            core_config = {
                'base_settings_path': tmpdir.mkdir('try-%s' % self.count).strpath,
                'backend_addr': backend_addr,
            }
            self.core_cmd = self.communicator.send

            async with backend_factory(**backend_config) as backend:

                await backend.user.create(
                    author='<backend-fixture>',
                    user_id=alice.user_id,
                    broadcast_key=alice.user_pubkey.encode(),
                    devices=[(alice.device_name, alice.device_verifykey.encode())]
                )

                async with run_app(backend) as backend_connection_factory:

                    tcp_stream_spy.install_hook(backend_addr, backend_connection_factory)
                    try:
                        async with core_factory(**core_config) as core:
                            await core.login(alice)
                            async with connect_core(core) as sock:

                                await sock.send({'cmd': 'file_create', 'path': '/foo.txt'})
                                rep = await sock.recv()
                                assert rep == {'status': 'ok'}
                                self.file_oracle = FileOracle()

                                task_status.started()

                                while True:
                                    msg = await self.communicator.trio_recv()
                                    await sock.send(msg)
                                    rep = await sock.recv()
                                    await self.communicator.trio_respond(rep)

                    finally:
                        tcp_stream_spy.install_hook(backend_addr, None)

        @rule(size=st.integers(min_value=0), offset=st.integers(min_value=0))
        @skip_on_broken_stream
        def read(self, size, offset):
            rep = self.core_cmd({
                'cmd': 'file_read',
                'path': '/foo.txt',
                'offset': offset,
                'size': size,
            })
            note(rep)
            assert rep['status'] == 'ok'
            expected_content = self.file_oracle.read(size, offset)
            assert from_jsonb64(rep['content']) == expected_content

        @rule()
        @skip_on_broken_stream
        def flush(self):
            rep = self.core_cmd({'cmd': 'flush', 'path': '/foo.txt'})
            note(rep)
            assert rep['status'] == 'ok'

        @rule()
        @skip_on_broken_stream
        def sync(self):
            rep = self.core_cmd({'cmd': 'synchronize', 'path': '/foo.txt'})
            note(rep)
            assert rep['status'] == 'ok'

        @rule(offset=st.integers(min_value=0), content=st.binary())
        @skip_on_broken_stream
        def write(self, offset, content):
            b64content = to_jsonb64(content)
            rep = self.core_cmd({
                'cmd': 'file_write',
                'path': '/foo.txt',
                'offset': offset,
                'content': b64content,
            })
            note(rep)
            assert rep['status'] == 'ok'
            self.file_oracle.write(offset, content)

    await CoreOnline.run_test()