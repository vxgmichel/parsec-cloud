from marshmallow import fields

from parsec.exceptions import ParsecError
from parsec.tools import BaseCmdSchema
from parsec.core.fs import FSPipeline


class cmd_LOGIN_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    password = fields.String(missing=None)


class Control:
    def __init__(self):
        self.user_id = None
        self.user_pubkey = None
        self.user_privkey = None
        self.fs = None

    def init_app(self, app):
        self.app = app
        app.extensions['control'] = self
        app.register_cmd('register', self._cmd_REGISTER)
        app.register_cmd('login', self._cmd_LOGIN)
        app.register_cmd('get_available_logins', self._cmd_GET_AVAILABLE_LOGINS)
        app.register_cmd('get_core_state', self._cmd_GET_CORE_STATE)
        app.register_cmd('logout', self._cmd_LOGOUT)

    def _cmd_REGISTER(self, app, req):
        return {'status': 'ok'}

    def _cmd_LOGIN(self, app, req):
        msg = cmd_LOGIN_Schema().load(req.msg)
        if self.user_id:
            raise ParsecError('already_logged', 'Already logged in as `%s`' % self.user_id)
        userkeys = self.app.config['GET_USER'](msg['id'], msg['password'])
        if not userkeys:
            raise ParsecError('unknown_user', 'No user known with id `%s`' % msg['id'])
        else:
            self.user_id = msg['id']
            self.user_pubkey, self.user_privkey = userkeys
            self.fs = FSPipeline()
            self.fs.start()
        return {'status': 'ok'}


    def _cmd_GET_AVAILABLE_LOGINS(self, app, req):
        return {'status': 'ok'}


    def _cmd_GET_CORE_STATE(self, app, req):
        return {'status': 'ok', 'online': True, 'logged': self.user_id}


    def _cmd_LOGOUT(self, app, req):
        if not self.user_id:
            raise ParsecError('not_logged', 'Must be logged in to use this command')
        self.fs.stop()
        self.user_id = None
        self.user_privkey = None
        self.user_pubkey = None
        return {'status': 'ok'}


control = Control()
init_control_api = control.init_app
