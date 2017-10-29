from parsec.exceptions import ParsecError


class ServiceNotReadyError(ParsecError):
    status = 'service_not_ready'


class BadMessageError(ParsecError):
    status = 'bad_msg'


class HandshakeError(ParsecError):
    status = 'bad_handshake'
    label = 'Session handshake failed.'


# Backend errors

class PrivKeyError(ParsecError):
    status = 'privkey_error'


class PrivKeyHashCollision(PrivKeyError):
    status = 'privkey_hash_collision'


class PrivKeyNotFound(PrivKeyError):
    status = 'privkey_not_found'


class PubKeyError(ParsecError):
    status = 'pubkey_error'


class PubKeyNotFound(PubKeyError):
    status = 'pubkey_not_found'


class VlobError(ParsecError):
    status = 'vlob_error'


class VlobNotFound(VlobError):
    status = 'vlob_not_found'


class TrustSeedError(ParsecError):
    status = 'trust_seed_error'


class UserVlobError(ParsecError):
    status = 'user_vlob_error'


class UserVlobNotFound(ParsecError):
    status = 'user_vlob_not_found'


class GroupError(ParsecError):
    status = 'group_error'


class GroupAlreadyExist(GroupError):
    status = 'group_already_exists'


class GroupNotFound(GroupError):
    status = 'group_not_found'


class BlockError(ParsecError):
    status = 'block_error'


class BlockConnectionError(ParsecError):
    status = 'block_connection_error'


class BlockNotFound(BlockError):
    status = 'block_not_found'
