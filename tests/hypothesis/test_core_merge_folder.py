import pytest
import os

from hypothesis import strategies as st, note
from hypothesis.stateful import Bundle, rule

from tests.common import (
    connect_core, core_factory, backend_factory, run_app
)
from tests.hypothesis.conftest import skip_on_broken_stream


@pytest.mark.slow
@pytest.mark.trio
@pytest.mark.hypothesis
async def test_core_merge_folder(
    TrioDriverRuleBasedStateMachine,
    mocked_local_storage_connection,
    tcp_stream_spy,
    backend_addr,
    tmpdir,
    alice
):
    st_name = st.text(min_size=1).filter(lambda x: '/' not in x)

    class CoreMergeFolder(TrioDriverRuleBasedStateMachine):
        Files = Bundle('file')
        Folders = Bundle('folder')
        count = 0

        async def trio_runner(self, task_status):
            self.tree = {}

            mocked_local_storage_connection.reset()

            type(self).count += 1
            base_settings_path = tmpdir.mkdir('try-%s' % self.count).strpath
            self.core_cmd = self.communicator.send

            async with backend_factory(blockstore_url='backend://') as backend:
                await backend.user.create(
                    author='Test backend author',
                    user_id=alice.user_id,
                    broadcast_key=alice.user_pubkey.encode(),
                    devices=[
                        (alice.device_name, alice.device_verifykey.encode())
                    ]
                )

                async with run_app(backend) as backend_co:
                    tcp_stream_spy.install_hook(backend_addr, backend_co)

                    try:
                        async with core_factory(base_settings_path=base_settings_path, backend_addr=backend_addr) as core:
                            await core.login(alice)

                            async with connect_core(core) as sock:
                                task_status.started()

                                while True:
                                    msg = await self.communicator.trio_recv()
                                    await sock.send(msg)
                                    rep = await sock.recv()
                                    await self.communicator.trio_respond(rep)

                    finally:
                        tcp_stream_spy.install_hook(backend_addr, None)

        def get_entry(self, path):
            if path == '/':
                return self.tree

            parts = path.split('/')
            current = self.tree

            for part in parts:
                if part in current:
                    current = current[part]

                else:
                    return None

            return current

        def remove_entry(self, path):
            if path == '/':
                return False

            parts = path.split('/')
            parent = os.path.join(parts[:-1])
            entry = self.get_entry(parent)

            if entry is not None:
                del entry[parts[-1]]
                return True

            return False

        @rule(target=Folders)
        def init_root(self):
            return '/'

        @rule(target=Files, parent=Folders, name=st_name)
        @skip_on_broken_stream
        def create_file(self, parent, name):
            entry = self.get_entry(parent)
            path = os.path.join(parent, name)
            rep = self.core_cmd({'cmd': 'file_create', 'path': path})

            if entry is None or name in entry:
                assert rep and rep['status'] == 'invalid_path'

            else:
                entry[name] = 'file'
                assert rep and rep['status'] == 'ok'

            return path

        @rule(target=Folders, parent=Folders, name=st_name)
        @skip_on_broken_stream
        def create_folder(self, parent, name):
            entry = self.get_entry(parent)
            path = os.path.join(parent, name)
            rep = self.core_cmd({'cmd': 'folder_create', 'path': path})

            if entry is None or name in entry:
                assert rep and rep['status'] == 'invalid_path'

            else:
                entry[name] = {}
                assert rep and rep['status'] == 'ok'

            return path

        def delete_path(self, path):
            entry = self.get_entry(path)
            rep = self.core_cmd({'cmd': 'delete', 'path': path})

            if entry is None:
                assert rep and rep['status'] == 'invalid_path'

            else:
                res = self.remove_entry(path)

                if res:
                    assert rep and rep['status'] == 'ok'

                else:
                    assert rep and rep['status'] == 'invalid_path'

        @rule(path=Files)
        @skip_on_broken_stream
        def delete_file(self, path):
            self.delete_path(path)

        @rule(path=Folders)
        @skip_on_broken_stream
        def delete_file(self, path):
            self.delete_path(path)

        def move_path(self, src, dst_parent, dst_name):
            src_entry = self.get_entry(src)
            dst_entry = self.get_entry(dst_parent)

            dst = os.path.join(dst_parent, dst_name)
            rep = self.core_cmd({'cmd': 'move', 'src': src, 'dst': dst})

            if src == '/' or src_entry is None or dst_entry is None:
                assert rep and rep['status'] == 'invalid_path'

            else:
                dst_entry[dst_name] = src_entry
                self.remove_entry(src)
                assert rep and rep['status'] == 'ok'

            return dst

        @rule(target=Files, src=Files, dst_parent=Folders, dst_name=st_name)
        @skip_on_broken_stream
        def move_file(self, src, dst_parent, dst_name):
            return self.move_path(src, dst_parent, dst_name)

        @rule(target=Folders, src=Folders, dst_parent=Folders, dst_name=st_name)
        @skip_on_broken_stream
        def move_folder(self, src, dst_parent, dst_name):
            return self.move_path(src, dst_parent, dst_name)

        def sync_path(self, path):
            rep = self.core_cmd({'cmd': 'synchronize', 'path': path})
            assert rep and rep['status'] == 'ok'

        @rule(path=Files)
        @skip_on_broken_stream
        def sync_file(self, path):
            self.sync_path(path)
        
        @rule(path=Folders)
        @skip_on_broken_stream
        def sync_folder(self, path):
            self.sync_path(path)

    await CoreMergeFolder.run_test()
