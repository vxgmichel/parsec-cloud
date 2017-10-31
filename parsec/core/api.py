

def register(app, req):
    return {'status': 'ok'}


def login(app, req):
    return {'status': 'ok'}


def get_available_logins(app, req):
    return {'status': 'ok'}


def get_status(app, req):
    return {'status': 'ok'}


def disconnect(app, req):
    return {'status': 'ok'}


def init_control_api(app):
    app.register_cmd('register', register)
    app.register_cmd('login', login)
    app.register_cmd('get_available_logins', get_available_logins)
    app.register_cmd('status', get_status)
    app.register_cmd('disconnect', disconnect)
