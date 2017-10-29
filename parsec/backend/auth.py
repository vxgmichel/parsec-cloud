import zmq
from zmq.auth.base import Authenticator
from zmq.auth.thread import ThreadAuthenticator, AuthenticationThread

def authenticator_factory(app):
    return CustomAuthenticatorThreadAuthenticator(
        lambda *args, **kwargs: DBAuthenticator(app, *args, **kwargs), app.zmqcontext)


class CustomAuthenticatorThreadAuthenticator(ThreadAuthenticator):
    """
    Original zmq's ThreadAuthenticator doesn't allow customization of it
    AuthenticationThread instance's authenticator attribute. This class fix this.
    """
    def __init__(self, authenticator_factory, *args, **kwargs):
        object.__setattr__(self, 'authenticator_factory', authenticator_factory)
        super().__init__(*args, **kwargs)

    def start(self):
        self.pipe = self.context.socket(zmq.PAIR)
        self.pipe.linger = 1
        self.pipe.bind(self.pipe_endpoint)
        self.thread = AuthenticationThread(
            self.context,
            self.pipe_endpoint,
            encoding=self.encoding,
            log=self.log,
            authenticator=self.authenticator_factory(self.context, self.encoding, self.log)
        )
        self.thread.start()
        if not self.thread.started.wait(timeout=10):
            raise RuntimeError("Authenticator thread failed to start")


# TODO: replace this by our own ZAP implementation to make thing cleaner ?
class DBAuthenticator(Authenticator):

    def __init__(self, app, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__app = app

    def _retrieve_identity(self, domain, client_key):
        if self.__app.db.pubkey_auth(client_key):
            return True, b'OK'
        else:
            return False, b"Unknown key"

    def curve_user_id(self, client_key):
        return self.__app.db.pubkey_auth(client_key)

    def _authenticate_curve(self, domain, client_key):
        """CURVE ZAP authentication"""
        allowed = False
        reason = b""
        if self.allow_any:
            allowed = True
            reason = b"OK"
            self.log.debug("ALLOWED (CURVE allow any client)")
        else:
            # If no explicit domain is specified then use the default domain
            if not domain:
                domain = '*'
            allowed, reason = self._retrieve_identity(domain, client_key)
        return allowed, reason
