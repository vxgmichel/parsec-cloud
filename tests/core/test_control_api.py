import zmq
import json
from unittest.mock import Mock

from tests.common import BaseCoreTest


class TestControlAPI(BaseCoreTest):
    @classmethod
    def setup_class(cls):
        cls.get_user_mock = None
        config = {
            'GET_USER': lambda *args, **kwargs: cls.get_user_mock(*args, **kwargs)
        }
        super().setup_class(config)

    def setup_method(self):
        # Reset the mock for each test
        type(self).get_user_mock = Mock()

    def test_simple(self):
        with self.core.connected() as sock:
            sock.send({"cmd": "get_core_state"})
            rep = sock.recv()
            assert rep == {"status": "ok", "online": True, "logged": None}

    def test_good_login_and_logout(self):
        self.get_user_mock.return_value = (b"<john's public>", b"<john's secret>")
        with self.core.connected() as sock:
            sock.send({"cmd": "login", "id": "john", "password": "S3CReT."})
            rep = sock.recv()
            assert rep == {"status": "ok"}

        # Make closing socket doesn't logged us out
        with self.core.connected() as sock:
            sock.send({"cmd": "get_core_state"})
            rep = sock.recv()
            assert rep == {"status": "ok", "online": True, "logged": "john"}

        # Now time to logout
        with self.core.connected() as sock:
            sock.send({"cmd": "logout"})
            rep = sock.recv()
            assert rep == {"status": "ok"}

        # Aaaand we're no longer logged
        with self.core.connected() as sock:
            sock.send({"cmd": "get_core_state"})
            rep = sock.recv()
            assert rep == {"status": "ok", "online": True, "logged": None}

    def test_bad_login(self):
        self.get_user_mock.return_value = None
        with self.core.connected() as sock:
            sock.send({"cmd": "login", "id": "john", "password": "S3CReT."})
            rep = sock.recv()
            assert rep == {"status": "unknown_user", 'label': 'No user known with id `john`'}


class TestAlreadyLogged(BaseCoreTest):

    def test_login_already_logged(self):
        with self.core.connected("alice@test.com") as sock:
            sock.send({"cmd": "login", "id": "bob@test.com", "password": ""})
            rep = sock.recv()
            assert rep == {"status": "already_logged", 'label': 'Already logged in as `alice@test.com`'}
