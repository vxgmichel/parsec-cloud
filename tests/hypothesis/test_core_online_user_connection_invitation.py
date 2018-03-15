from marshmallow import ValidationError, validate
import pytest
from hypothesis import strategies as st, note
from hypothesis.stateful import Bundle, rule

from parsec.core.app import user_device_name_regexp
from parsec.utils import to_jsonb64, from_jsonb64

from tests.common import (
    connect_core, core_factory, backend_factory, run_app
)
from tests.hypothesis.conftest import skip_on_broken_stream


validator = validate.Regexp(user_device_name_regexp)


class UserOracle:
    def __init__(self):
        self.connected = 'alice'
        self.users = {}
        self.invitations = {}
        self.tokens = {}

    def login(self, user, password):
        if self.connected:
            return 'already_logged'
        elif user in self.users and password == self.users[user]:
            self.connected = user
            return 'ok'
        else:
            return 'unknown_user'

    def logout(self):
        if self.connected:
            self.connected = None
            return 'ok'
        else:
            return 'login_required'

    def invite(self, user):
        if user in self.users:
            return 'Already exist'
        elif self.connected:
            try:
                validator(user)
            except ValidationError:
                return 'bad_message'
            self.invitations[user] = 'missing_token'
            return 'ok'
        else:
            return 'login_required'

    def set_invitation_token(self, user, token):
        if self.invitations[user] == 'missing_token':
            self.invitations[user] = token
        if token in self.tokens:
            raise Exception('Token conflict')
        else:
            self.tokens[token] = user

    def claim(self, user, device, token, password):
        if self.connected:
            return 'already_logged'
        try:
            validator(user)
            validator(device)
        except ValidationError:
            return 'bad_message'
        if user + '@' + device in self.users:
            return 'device_config_saving_error'
        elif user in self.invitations and token == self.invitations[user]:
            del self.invitations[user]
            del self.tokens[token]
            self.users[user + '@' + device] = password
            return 'ok'
        elif user in self.invitations and token != self.invitations[user]:
            return 'claim_error'
        else:
            return 'already_exists_error'


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

    st_user_device_name = st.from_regex(user_device_name_regexp)

    class CoreOnline(TrioDriverRuleBasedStateMachine):
        Invitations = Bundle('invitation')
        Users = Bundle('user')

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

                                self.user_oracle = UserOracle()

                                task_status.started()

                                while True:
                                    msg = await self.communicator.trio_recv()
                                    await sock.send(msg)
                                    rep = await sock.recv()
                                    await self.communicator.trio_respond(rep)

                    finally:
                        tcp_stream_spy.install_hook(backend_addr, None)

        @rule(user=Users)
        @skip_on_broken_stream
        def login(self, user):
            if not user:
                return
            rep = self.core_cmd({
                'cmd': 'login',
                'id': user[0],
                'password': user[1],
            })
            note(rep)
            expected_result = self.user_oracle.login(user[0], user[1])
            assert rep['status'] == expected_result

        @rule()
        @skip_on_broken_stream
        def logout(self):
            rep = self.core_cmd({
                'cmd': 'logout',
            })
            note(rep)
            expected_status = self.user_oracle.logout()
            assert rep['status'] == expected_status

        @rule(target=Invitations, user_id=st_user_device_name)
        @skip_on_broken_stream
        def invite(self, user_id):
            rep = self.core_cmd({
                'cmd': 'user_invite',
                'user_id': user_id,
            })
            note(rep)
            expected_status = self.user_oracle.invite(user_id)
            assert rep['status'] == expected_status
            if rep['status'] == 'ok':
                self.user_oracle.set_invitation_token(user_id, rep['invitation_token'])
                return (user_id, rep['invitation_token'])

        @rule(target=Users, invitation=Invitations, device_id=st_user_device_name, password=st.text(min_size=1))
        @skip_on_broken_stream
        def claim(self, invitation, device_id, password):
            if not invitation:
                return
            rep = self.core_cmd({
                'cmd': 'user_claim',
                'id': invitation[0] + '@' + device_id,
                'invitation_token': invitation[1],
                'password': password
            })
            note(rep)
            expected_result = self.user_oracle.claim(invitation[0], device_id, invitation[1], password)
            assert rep['status'] == expected_result
            if rep['status'] == 'ok':
                return (invitation[0] + '@' + device_id, password)

    await CoreOnline.run_test()
