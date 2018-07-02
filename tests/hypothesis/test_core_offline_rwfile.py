import pytest
from hypothesis import strategies as st, note
from hypothesis.stateful import rule

from parsec.utils import to_jsonb64, from_jsonb64

from tests.common import bootstrap_device, connect_core, core_factory, FileOracle


BLOCK_SIZE = 16
PLAYGROUND_SIZE = BLOCK_SIZE * 10


@pytest.mark.slow
@pytest.mark.trio
async def test_core_offline_rwfile(TrioDriverRuleBasedStateMachine, backend_addr, tmpdir):
    class CoreOfflineRWFile(TrioDriverRuleBasedStateMachine):
        async def trio_runner(self, task_status):

            config = {
                "base_settings_path": tmpdir.strpath,
                "backend_addr": backend_addr,
                "block_size": BLOCK_SIZE,
            }
            device = bootstrap_device("alice", "dev1")

            async with core_factory(**config) as core:
                await core.login(device)
                async with connect_core(core) as sock:

                    await core.fs.file_create("/foo.txt")
                    self.file_oracle = FileOracle()

                    self.core_cmd = self.communicator.send
                    task_status.started()

                    while True:
                        msg = await self.communicator.trio_recv()
                        await sock.send(msg)
                        rep = await sock.recv()
                        await self.communicator.trio_respond(rep)

        @rule(
            size=st.integers(min_value=0, max_value=PLAYGROUND_SIZE),
            offset=st.integers(min_value=0, max_value=PLAYGROUND_SIZE),
        )
        def atomic_read(self, size, offset):
            rep = self.core_cmd(
                {"cmd": "file_read", "path": "/foo.txt", "offset": offset, "size": size}
            )
            note(rep)
            assert rep["status"] == "ok"
            expected_content = self.file_oracle.read(size, offset)
            assert from_jsonb64(rep["content"]) == expected_content

        @rule(
            offset=st.integers(min_value=0, max_value=PLAYGROUND_SIZE),
            content=st.binary(max_size=PLAYGROUND_SIZE),
        )
        def atomic_write(self, offset, content):
            b64content = to_jsonb64(content)
            rep = self.core_cmd(
                {"cmd": "file_write", "path": "/foo.txt", "offset": offset, "content": b64content}
            )
            note(rep)
            assert rep["status"] == "ok"
            self.file_oracle.write(offset, content)

        @rule(length=st.integers(min_value=0, max_value=PLAYGROUND_SIZE))
        def atomic_truncate(self, length):
            rep = self.core_cmd({"cmd": "file_truncate", "path": "/foo.txt", "length": length})
            note(rep)
            assert rep["status"] == "ok"
            self.file_oracle.truncate(length)

    await CoreOfflineRWFile.run_test()
