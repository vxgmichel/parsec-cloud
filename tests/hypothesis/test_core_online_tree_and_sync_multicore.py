import os
import attr
import pytest
from hypothesis import strategies as st, note
from hypothesis.stateful import Bundle, rule

from tests.common import (
    connect_core, core_factory, backend_factory, run_app
)
from tests.hypothesis.conftest import skip_on_broken_stream


@attr.s
class OracleFS:
    root = attr.ib(default=attr.Factory(dict))

    def create_file(self, parent_path, name):
        parent_folder = self.get_folder(parent_path)
        if parent_folder is None or name in parent_folder:
            return 'invalid_path'
        parent_folder[name] = '<file>'
        return 'ok'

    def create_folder(self, parent_path, name):
        parent_folder = self.get_folder(parent_path)
        if parent_folder is None or name in parent_folder:
            return 'invalid_path'
        parent_folder[name] = {}
        return 'ok'

    def delete(self, path):
        parent_path, name = path.rsplit('/', 1)
        parent_dir = self.get_path(parent_path)
        if isinstance(parent_dir, dict) and name in parent_dir:
            del parent_dir[name]
            return 'ok'
        else:
            return 'invalid_path'

    def move(self, src, dst):
        parent_src, name_src = src.rsplit('/', 1)
        parent_dst, name_dst = dst.rsplit('/', 1)

        parent_dir_src = self.get_folder(parent_src)
        parent_dir_dst = self.get_folder(parent_dst)

        if parent_dir_src is None or name_src not in parent_dir_src:
            return 'invalid_path'
        if parent_dir_dst is None or name_dst in parent_dir_dst:
            return 'invalid_path'

        parent_dir_dst[name_dst] = parent_dir_src.pop(name_src)
        return 'ok'

    def get_folder(self, path):
        elem = self.get_path(path)
        return elem if elem != '<file>' else None

    def get_file(self, path):
        elem = self.get_path(path)
        return elem if elem == '<file>' else None

    def get_path(self, path):
        current_folder = self.root
        try:
            for item in path.split('/'):
                if item:
                    current_folder = current_folder[item]
        except KeyError:
            return None
        return current_folder

    def sync(self, parent_path, name):
        parent_folder = self.get_folder(parent_path)
        if parent_folder is None or name not in parent_folder:
            return 'invalid_path'
        return 'ok'


def get_tree_from_core(core):
    def get_tree_from_folder_entry(entry):
        tree = {}

        for k, v in entry._children.items():
            if isinstance(v, core.fs._file_entry_cls):
                tree[k] = v._access.dump()

            else:
                tree[k] = get_tree_from_folder_entry(v)

        return tree

    return get_tree_from_folder_entry(core.fs.root)


@pytest.mark.slow
@pytest.mark.trio
@pytest.mark.hypothesis
async def test_online_core_tree_and_sync_multicore(
    TrioDriverRuleBasedStateMachine,
    mocked_local_storage_connection,
    tcp_stream_spy,
    backend_addr,
    tmpdir,
    alice
):

    st_entry_name = st.text(min_size=1).filter(lambda x: '/' not in x)
    st_core = st.sampled_from(['core1', 'core2'])

    class MultiCoreRW(TrioDriverRuleBasedStateMachine):
        Files = Bundle('file')
        Folders = Bundle('folder')
        count = 0

        async def trio_runner(self, task_status):
            mocked_local_storage_connection.reset()
            self.oracle1 = OracleFS()
            self.oracle2 = OracleFS()

            type(self).count += 1
            backend_config = {
                'blockstore_url': 'backend://',
            }
            core_config = {
                'base_settings_path': tmpdir.mkdir('try-%s' % self.count).strpath,
                'backend_addr': backend_addr,
            }

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
                        async with core_factory(logname='parsec.core1.app', **core_config) as self.core1, \
                                   core_factory(logname='parsec.core2.app', **core_config) as self.core2:

                            await self.core1.login(alice)
                            await self.core2.login(alice)

                            async with connect_core(self.core1) as sock1, \
                                       connect_core(self.core2) as sock2:
                                task_status.started()

                                targets = {
                                    'core1': sock1,
                                    'core2': sock2
                                }

                                while True:
                                    target, msg = await self.communicator.trio_recv()
                                    sock = targets[target]
                                    await sock.send(msg)
                                    rep = await sock.recv()
                                    await self.communicator.trio_respond(rep)

                    finally:
                        tcp_stream_spy.install_hook(backend_addr, None)

        def core_cmd(self, core, msg):
            return self.communicator.send((core, msg))

        def get_oracle(self, core):
            oracles = {
                'core1': self.oracle1,
                'core2': self.oracle2
            }

            return oracles[core]

        @rule(target=Folders)
        def init_root(self):
            return '/'

        @rule(core=st_core, target=Files, parent=Folders, name=st_entry_name)
        @skip_on_broken_stream
        def create_file(self, core, parent, name):
            path = os.path.join(parent, name)
            rep = self.core_cmd(core, {'cmd': 'file_create', 'path': path})
            note(rep)
            expected_status = self.get_oracle(core).create_file(parent, name)
            assert rep['status'] == expected_status
            return path

        @rule(core=st_core, target=Folders, parent=Folders, name=st_entry_name)
        @skip_on_broken_stream
        def create_folder(self, core, parent, name):
            path = os.path.join(parent, name)
            rep = self.core_cmd(core, {'cmd': 'folder_create', 'path': path})
            note(rep)
            expected_status = self.get_oracle(core).create_folder(parent, name)
            assert rep['status'] == expected_status
            return path

        @rule(core=st_core, path=Files)
        @skip_on_broken_stream
        def delete_file(self, core, path):
            rep = self.core_cmd(core, {'cmd': 'delete', 'path': path})
            note(rep)
            expected_status = self.get_oracle(core).delete(path)
            assert rep['status'] == expected_status

        @rule(core=st_core, path=Folders)
        @skip_on_broken_stream
        def delete_folder(self, core, path):
            rep = self.core_cmd(core, {'cmd': 'delete', 'path': path})
            note(rep)
            expected_status = self.get_oracle(core).delete(path)
            assert rep['status'] == expected_status

        @rule(core=st_core, target=Files, src=Files, dst_parent=Folders, dst_name=st_entry_name)
        @skip_on_broken_stream
        def move_file(self, core, src, dst_parent, dst_name):
            dst = os.path.join(dst_parent, dst_name)
            rep = self.core_cmd(core, {'cmd': 'move', 'src': src, 'dst': dst})
            note(rep)
            expected_status = self.get_oracle(core).move(src, dst)
            assert rep['status'] == expected_status
            return dst

        @rule(core=st_core, target=Folders, src=Folders, dst_parent=Folders, dst_name=st_entry_name)
        @skip_on_broken_stream
        def move_folder(self, core, src, dst_parent, dst_name):
            dst = os.path.join(dst_parent, dst_name)
            rep = self.core_cmd(core, {'cmd': 'move', 'src': src, 'dst': dst})
            note(rep)
            expected_status = self.get_oracle(core).move(src, dst)
            assert rep['status'] == expected_status
            return dst

        @rule()
        @skip_on_broken_stream
        def sync_all_the_files(self):
            rep1 = self.core_cmd('core1', {'cmd': 'synchronize', 'path': '/'})
            rep2 = self.core_cmd('core2', {'cmd': 'synchronize', 'path': '/'})
            note((rep1, rep2))

            note(self.core1.fs.root._children)
            note(self.core2.fs.root._children)
            synced_tree1 = get_tree_from_core(self.core1)
            synced_tree2 = get_tree_from_core(self.core2)
            note((synced_tree1, synced_tree2))

            assert rep1['status'] == 'ok'
            assert rep2['status'] == 'ok'
            assert not self.core1.fs.root.need_sync
            assert not self.core2.fs.root.need_sync
            assert self.core1.fs.root.base_version == self.core2.fs.root.base_version
            assert synced_tree1 == synced_tree2

    await MultiCoreRW.run_test()
