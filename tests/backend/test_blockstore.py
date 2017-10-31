import pytest


class TestBlockStore:

    def test_get_url(self, alicesock):
        alicesock.send({'cmd': 'blockstore_get_url'})
        rep = alicesock.recv()
        assert rep == {'status': 'ok', 'url': '<inbackend>'}

    def test_post_and_get(self, alicesock):
        alicesock.send({'cmd': 'blockstore_post', 'id': '123'}, b'<block content>')
        rep = alicesock.recv()
        assert rep == {'status': 'ok'}
        alicesock.send({'cmd': 'blockstore_get', 'id': '123'})
        rep, *contentframes = alicesock.recv(exframes=True)
        assert rep == {'status': 'ok'}
        assert contentframes == [b'<block content>']

    def test_post_already_exists(self, alicesock):
        alicesock.send({'cmd': 'blockstore_post', 'id': '123'}, b'<block content>')
        rep = alicesock.recv()
        assert rep == {'status': 'ok'}
        # Repost with same id
        alicesock.send({'cmd': 'blockstore_post', 'id': '123'}, b'<block content>')
        rep = alicesock.recv()
        assert rep == {'status': 'block_id_already_exists'}
