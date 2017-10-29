class BaseDB:
    def __init__(self, db_url):
        self.db_url = db_url

    def pubkeys_get(self, userid):
        raise NotImplementedError()

    def notification_register(self, topic, sender=None):
        raise NotImplementedError()

    def wait_notification(self):
        raise NotImplementedError()

    def message_send(self, recipientid, body):
        raise NotImplementedError()

    def message_get(self, userid, offset=0, limit=None):
        raise NotImplementedError()

    def vlob_create(self, id, rts, wts, blob):
        raise NotImplementedError()

    def vlob_read(self, id, trust_seed, version):
        raise NotImplementedError()

    def vlob_update(self, id, trust_seed, version, blob):
        raise NotImplementedError()

    def user_vlob_read(self, id, version):
        raise NotImplementedError()

    def user_vlob_update(self, id, version, body):
        raise NotImplementedError()
