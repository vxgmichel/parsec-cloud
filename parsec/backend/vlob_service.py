import random
import string
from uuid import uuid4

# TODO openssl or default backend?
from cryptography.hazmat.backends.openssl import backend as openssl
from cryptography.hazmat.primitives import hashes

from parsec.service import BaseService, cmd, event, service
from parsec.exceptions import ParsecError


TRUST_SEED_LENGTH = 12


def generate_trust_seed():
    # Use SystemRandom to get cryptographically secure seeds
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                   for _ in range(TRUST_SEED_LENGTH))


class VlobError(ParsecError):
    pass


class VlobAuthError(VlobError):
    status = 'auth_error'


class VlobNotFound(VlobError):
    status = 'not_found'


class BaseVlobService(BaseService):

    crypto_service = service('CryptoService')
    pub_keys_service = service('PubKeysService')
    on_vlob_updated = event('updated')

    @cmd('get_sign_challenge')
    async def _cmd_GET_SIGN_CHAlLENGE(self, msg):
        challenge = await self.get_sign_challenge(msg['id'])
        return {'status': 'ok', 'challenge': challenge.decode()}

    @cmd('get_seed_challenge')
    async def _cmd_GET_SEED_CHALLENGE(self, msg):
        challenge = await self.get_seed_challenge(msg['id'])
        return {'status': 'ok', 'challenge': challenge}

    @cmd('create')
    async def _cmd_CREATE(self, msg):
        vlob = await self.create(self._get_field(msg, 'id', default=None),
                                 self._get_field(msg, 'blob', default=None),
                                 self._get_field(msg, 'read_trust_seed', default=None),
                                 self._get_field(msg, 'write_trust_seed', default=None))
        return {
            'status': 'ok',
            'id': vlob.id,
            'read_trust_seed': vlob.read_trust_seed,
            'write_trust_seed': vlob.write_trust_seed
        }

    @cmd('read')
    async def _cmd_READ(self, msg):
        hash = self._get_field(msg, 'hash', type_=bytes, default=None)
        vlob = await self.read(self._get_field(msg, 'id'), self._get_field(msg, 'challenge'), hash)
        blob = None
        version = self._get_field(msg, 'version', default=len(vlob.blob_versions))
        if version >= 1:
            blob = vlob.blob_versions[version - 1]
        return {'status': 'ok', 'blob': blob, 'version': version}

    @cmd('update')
    async def _cmd_UPDATE(self, msg):
        hash = self._get_field(msg, 'hash', type_=bytes, default=None)
        await self.update(msg['id'],
                          int(msg['version']),
                          msg['blob'],
                          self._get_field(msg, 'challenge'),
                          hash)
        return {'status': 'ok'}


class Vlob:
    def __init__(self, id=None, blob=None, read_trust_seed=None, write_trust_seed=None):
        # Generate opaque id if not provided
        self.id = id or uuid4().hex  # TODO uuid4 or trust seed?
        self.read_trust_seed = read_trust_seed or generate_trust_seed()
        self.write_trust_seed = write_trust_seed or generate_trust_seed()
        self.blob_versions = [blob] if blob else []


class VlobService(BaseVlobService):
    def __init__(self):
        super().__init__()
        self._challenges = {}  # TODO clean old challenges
        self._vlobs = {}

    async def get_seed_challenge(self, id):
        try:
            vlob = self._vlobs[id]
        except KeyError:
            raise VlobNotFound('Cannot find vlob.')
        challenge = generate_trust_seed()
        self._challenges[challenge] = vlob
        return challenge

    async def get_sign_challenge(self, id):
        challenge = generate_trust_seed()
        # TODO id = hash or identity?
        encrypted_challenge = await self.crypto_service.asym_encrypt(challenge, id)
        self._challenges[challenge] = id
        return encrypted_challenge

    async def validate_seed_challenge(self, operation, challenge, hash):
        digest = hashes.Hash(hashes.SHA512(), backend=openssl)
        try:
            vlob = self._challenges[challenge]
            del self._challenges[challenge]
        except KeyError:
            raise VlobAuthError('Authentication failure.')
        if operation == 'READ':
            trust_seed = vlob.read_trust_seed
        elif operation == 'WRITE':
            trust_seed = vlob.write_trust_seed
        digest.update(challenge.encode() + trust_seed.encode())
        if digest.finalize() == hash:
            return True
        else:
            raise VlobAuthError('Authentication failure.')

    async def validate_sign_challenge(self, id, challenge):
        try:
            target_id = self._challenges[challenge]
            del self._challenges[challenge]
        except KeyError:
            raise VlobAuthError('Authentication failure.')  # TODO exception messages???
        if target_id == id:
            return True
        else:
            raise VlobAuthError('Authentication failure.')

    async def create(self, id=None, blob=None, read_trust_seed=None, write_trust_seed=None):
        vlob = Vlob(id=id,
                    blob=blob,
                    read_trust_seed=read_trust_seed,
                    write_trust_seed=write_trust_seed)
        # TODO: who cares about hash collision ?
        self._vlobs[vlob.id] = vlob
        return vlob

    async def read(self, id, challenge, hash=None):
        if ((hash and await self.validate_seed_challenge('READ', challenge, hash)) or
                (not hash and await self.validate_sign_challenge(id, challenge))):
            try:
                vlob = self._vlobs[id]
            except KeyError:
                raise VlobNotFound('Cannot find vlob.')
            return vlob

    async def update(self, id, next_version, blob, challenge, hash=None):
        if ((hash and await self.validate_seed_challenge('WRITE', challenge, hash)) or
                (not hash and await self.validate_sign_challenge(id, challenge))):
            try:
                vlob = self._vlobs[id]
            except KeyError:
                raise VlobNotFound('Cannot find vlob.')
            if next_version == len(vlob.blob_versions) + 1:
                vlob.blob_versions.append(blob)
                self._vlobs[id] = vlob
            else:
                raise VlobError('Wrong blob version.')
            self.on_vlob_updated.send(id)
