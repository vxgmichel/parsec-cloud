from parsec import tools


class ParsecError(Exception):
    status = 'error'
    label = ''

    def __init__(self, *args):
        args_count = len(args)
        if args_count == 1:
            self.label = args[0]
        elif args_count == 2:
            self.status, self.label = args

    def to_dict(self):
        return {'status': self.status, 'label': self.label}

    def to_raw(self):
        return tools.ejson_dumps(self.to_dict())


class BadMessageError(ParsecError):
    status = 'bad_msg'
