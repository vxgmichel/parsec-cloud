from collections import defaultdict
import queue

from parsec.backend.db.base import BaseDB
from parsec.backend.exceptions import (
    PubKeyNotFound, VlobNotFound, UserVlobError, TrustSeedError)


class InMemoryDB(BaseDB):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._messages = defaultdict(list)
        self._vlobs = defaultdict(list)
        self._user_vlobs = defaultdict(list)
        self._notification_registers = []
        self._notifications = queue.Queue()
        self._pubkeys = {}

    def pubkey_get(self, userid):
        try:
            return self._pubkeys[userid]
        except KeyError:
            raise PubKeyNotFound('Unknown user')

    def pubkey_auth(self, pubkey):
        for userid, userkey in self._pubkeys.items():
            if userkey == pubkey:
                return userid

    def notification_register(self, topic, sender=None):
        if not sender:
            self._notification_registers.append(topic)
        self._notification_registers.append((topic, sender))

    def wait_notification(self):
        self._notifications.get()

    def _notify(self, topic, sender):
        if (topic in self._notification_registers or
                (topic, sender) in self._notification_registers):
            self._notifications.put((topic, sender))

    def message_send(self, recipientid, bodyframe):
        self._messages[recipientid].append(bodyframe.bytes)
        self._notify('message_arrived', recipientid)

    def message_get(self, userid, offset=0, limit=None):
        msgs = self._messages[userid][offset:]
        if limit:
            return msgs[:limit]
        else:
            return msgs

    def vlob_create(self, id, rts, wts, blobframe):
        self._vlobs[id] = (rts, wts, [blobframe.bytes])

    def vlob_read(self, id, trust_seed, version):
        if id not in self._vlobs:
            raise VlobNotFound('Vlob not found.')
        rts, _, blobs = self._vlobs[id]
        if rts != trust_seed:
            raise TrustSeedError('Invalid read trust seed.')
        version = version or len(blobs)
        try:
            return blobs[version - 1], version
        except IndexError:
            raise VlobNotFound('Wrong blob version.')

    def vlob_update(self, id, trust_seed, version, blobframe):
        if id not in self._vlobs:
            raise VlobNotFound('Vlob not found.')
        _, wts, blobs = self._vlobs[id]
        if wts != trust_seed:
            raise TrustSeedError('Invalid write trust seed.')
        if version - 1 == len(blobs):
            blobs.append(blobframe.bytes)
        else:
            raise VlobNotFound('Wrong blob version.')
        self._notify('vlob_updated', id)

    def user_vlob_read(self, id, version):
        vlobs = self._user_vlobs[id]
        if version == 0 or (version is None and not vlobs):
            return b'', 0
        try:
            if version is None:
                return vlobs[-1], len(vlobs)
            else:
                return vlobs[version - 1], version
        except IndexError:
            raise UserVlobError('Wrong blob version.')

    def user_vlob_update(self, id, version, blobframe):
        vlobs = self._user_vlobs[id]
        if len(vlobs) != version - 1:
            raise UserVlobError('Wrong blob version.')
        vlobs.append(blobframe.bytes)
        self._notify('user_vlob_updated', id)
