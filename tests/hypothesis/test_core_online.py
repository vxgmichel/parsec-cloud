import pytest
from hypothesis import note
from hypothesis.stateful import rule

from tests.common import connect_core, core_factory, backend_factory, run_app


@pytest.mark.slow
@pytest.mark.trio
async def test_online(TrioDriverRuleBasedStateMachine, tcp_stream_spy, backend_addr, tmpdir, alice):
    class CoreOnline(TrioDriverRuleBasedStateMachine):
        count = 0

        async def trio_runner(self, task_status):
            type(self).count += 1
            workdir = tmpdir.mkdir("try-%s" % self.count)

            backend_config = {"blockstore_postgresql": True}
            core_config = {"base_settings_path": workdir.strpath, "backend_addr": backend_addr}
            alice.local_storage_db_path = str(workdir / "alice-local_storage")
            self.core_cmd = self.communicator.send

            async with backend_factory(**backend_config) as backend:

                await backend.user.create(
                    author="<backend-fixture>",
                    user_id=alice.user_id,
                    broadcast_key=alice.user_pubkey.encode(),
                    devices=[(alice.device_name, alice.device_verifykey.encode())],
                )

                async with run_app(backend) as backend_connection_factory:

                    with tcp_stream_spy.install_hook(backend_addr, backend_connection_factory):
                        async with core_factory(**core_config) as core:
                            await core.login(alice)
                            async with connect_core(core) as sock:

                                task_status.started()

                                while True:
                                    msg = await self.communicator.trio_recv()
                                    await sock.send(msg)
                                    rep = await sock.recv()
                                    await self.communicator.trio_respond(rep)

        @rule()
        def get_core_state(self):
            rep = self.core_cmd({"cmd": "get_core_state"})
            note(rep)
            assert rep == {"status": "ok", "login": "alice@test", "backend_online": True}

    await CoreOnline.run_test()
