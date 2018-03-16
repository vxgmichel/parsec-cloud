from marshmallow import ValidationError, validate
import pytest
from hypothesis import strategies as st, note
from hypothesis.stateful import Bundle, rule

from parsec.core.app import user_or_device_regexp, device_id_regexp

from tests.common import (
    connect_core, core_factory, backend_factory, run_app
)
from tests.hypothesis.conftest import skip_on_broken_stream


user_or_device_validator = validate.Regexp(user_or_device_regexp)
device_id_validator = validate.Regexp(device_id_regexp)


class UserOracle:

    def __init__(self):
        self.connected = 'alice@test'
        self.users = {}
        self.devices_config = []
        self.invitations = {}
        self.declarations = {}
        self.tokens = {}

    def login(self, user, password):
        user_id = user.rsplit('@', 1)[0]
        if self.connected:
            return 'already_logged'
        elif (user in self.devices_config and
                user_id in self.users and password == self.users[user_id]):
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

    def list_available_logins(self):
        return self.devices_config

    def info(self):
        return (self.connected, self.connected is not None)

    def invite(self, user):
        if self.connected:
            if user in self.users:
                return 'already_exists'
            try:
                user_or_device_validator(user)
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
            user_or_device_validator(user)
            user_or_device_validator(device)
        except ValidationError:
            return 'bad_message'
        device_id = user + '@' + device
        if user in self.users:
            return 'already_exists_error'
        elif device_id in self.devices_config:
            return 'device_config_saving_error'
        elif user in self.invitations and token == self.invitations[user]:
            del self.invitations[user]
            del self.tokens[token]
            self.users[user] = password
            self.devices_config.append(device_id)
            return 'ok'
        elif user in self.invitations and token != self.invitations[user]:
            return 'claim_error'
        else:
            return 'not_found_error'

    def declare(self, device):
        if self.connected:
            device_id = self.connected.rsplit('@', 1)[0] + '@' + device
            if (device_id in self.devices_config or
                    device_id in self.declarations):
                return 'already_exists'
            try:
                user_or_device_validator(device)
            except ValidationError:
                return 'bad_message'
            self.declarations[device_id] = 'missing_token'
            return 'ok'
        else:
            return 'login_required'

    def set_configuration_token(self, device, token):
        if self.declarations[device] == 'missing_token':
            self.declarations[device] = token
        if token in self.tokens:
            raise Exception('Token conflict')
        else:
            self.tokens[token] = device

    def configure(self, device_id, token, password):
        if self.connected:
            return 'already_logged'
        try:
            device_id_validator(device_id)
        except ValidationError:
            return 'bad_message'
        if device_id in self.devices_config:
            return 'device_config_saving_error'
        elif device_id in self.declarations and token == self.declarations[device_id]:
            del self.declarations[device_id]
            del self.tokens[token]
            self.devices_config.append(device_id)
            return 'ok'
        elif device_id in self.declarations and token != self.declarations[device_id]:
            return 'configure_error'
        else:
            return 'not_found_error'

    def get_configurationt_try(self):
        pass

    def accept_configuration_try(self, try_id):
        pass


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

    st_user_or_device = st.from_regex(user_or_device_regexp)

    class CoreOnline(TrioDriverRuleBasedStateMachine):
        Invitations = Bundle('invitation')
        Users = Bundle('user')
        Declarations = Bundle('declaration')
        Devices = Bundle('device')

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

                                await sock.send({
                                    'cmd': 'event_subscribe',
                                    'event': 'device_try_claim_submitted'})
                                rep = await sock.recv()
                                assert rep == {'status': 'ok'}
                                self.user_oracle = UserOracle()

                                task_status.started()

                                while True:
                                    msg = await self.communicator.trio_recv()
                                    await sock.send(msg)

                                    if msg['cmd'] == 'device_configure':
                                        await sock.send({'cmd': 'event_listen', 'wait': False})
                                        # import pdb; pdb.set_trace()
                                        rep = await sock.recv()
                                        # import pdb; pdb.set_trace()
                                        device_name = msg['device_id'].rsplit('@', 1)[1]
                                        assert rep['event'] == 'device_try_claim_submitted'
                                        assert rep['device_name'] == device_name
                                        await sock.send({
                                            'cmd': 'device_accept_configuration_try',
                                            'configuration_try_id': rep['configuration_try_id']})
                                        rep = await sock.recv()
                                        assert rep == {'status': 'ok'}

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

        @rule()
        @skip_on_broken_stream
        def list_available_logins(self):
            rep = self.core_cmd({
                'cmd': 'list_available_logins',
            })
            note(rep)
            expected_devices = self.user_oracle.list_available_logins()
            assert len(rep) == 2
            assert rep['status'] == 'ok'
            assert set(rep['devices']) == set(expected_devices)

        @rule()
        @skip_on_broken_stream
        def info(self):
            rep = self.core_cmd({
                'cmd': 'info',
            })
            note(rep)
            expected_result = self.user_oracle.info()
            assert rep == {'status': 'ok', 'id': expected_result[0], 'loaded': expected_result[1]}

        @rule(target=Invitations, user_id=st_user_or_device)
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

        @rule(target=Users,
              invitation=Invitations,
              device_id=st_user_or_device,
              password=st.text(min_size=1))
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
            expected_result = self.user_oracle.claim(
                invitation[0],
                device_id,
                invitation[1],
                password
            )
            assert rep['status'] == expected_result
            if rep['status'] == 'ok':
                return (device_id, password)

        @rule(target=Declarations, device_name=st_user_or_device)
        @skip_on_broken_stream
        def declare(self, device_name):
            rep = self.core_cmd({
                'cmd': 'device_declare',
                'device_name': device_name
            })
            note(rep)
            expected_status = self.user_oracle.declare(device_name)
            assert rep['status'] == expected_status
            if self.user_oracle.connected:
                device_id = self.user_oracle.connected.rsplit('@', 1)[0] + '@' + device_name
                if rep['status'] == 'ok':
                    self.user_oracle.set_configuration_token(device_id, rep['configure_device_token'])
                    return (device_id, rep['configure_device_token'])

        # @rule(target=Devices,
        #       declaration=Declarations,
        #       password=st.text(min_size=1))
        # @skip_on_broken_stream
        # def configure(self, declaration, password):
        #     if not declaration:
        #         return
        #     rep = self.core_cmd({
        #         'cmd': 'device_configure',
        #         'device_id': declaration[0],
        #         'configure_device_token': declaration[1],
        #         'password': password
        #     })
        #     note(rep)
        #     expected_result = self.user_oracle.configure(
        #         declaration[0],
        #         declaration[1],
        #         password
        #     )
        #     assert rep['status'] == expected_result
        #     if rep['status'] == 'ok':
        #         return (declaration[0], password)

    await CoreOnline.run_test()

# socketConfigureDevice => `{"cmd": "device_configure", "device_id": "${identity}", "password": "${password}", "configure_device_token": "${token}"}`
# socketEventSubscribe => `{"cmd": "event_subscribe", "event": "device_try_claim_submitted"}`
# socketEventListen => `{"cmd": "event_listen", "wait": "False"}`
# socketAcceptDevice => `{"cmd": "device_accept_configuration_try", "configuration_try_id": "${configuration_try_id}"}`
# socketEventUnsubscribe => `{"cmd": "event_unsubscribe", "event": "device_try_claim_submitted"}`
