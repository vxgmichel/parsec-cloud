from base64 import b32decode, b32encode
from nacl.public import PrivateKey as _PrivateKey, PublicKey as _PublicKey
from nacl.signing import SigningKey as _SigningKey, VerifyKey as _VerifyKey
from nacl.exceptions import CryptoError


__all__ = (
    "PrivateKey",
    "PublicKey",
    "SigningKey",
    "VerifyKey",
    "export_root_verify_key",
    "import_root_verify_key",
)


# Basically just add comparison support to nacl keys


class SigningKey(_SigningKey):
    __slots__ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.verify_key.__class__ = VerifyKey

    @classmethod
    def generate(cls, *args, **kwargs) -> "SigningKey":
        obj = super().generate(*args, **kwargs)
        obj.__class__ = SigningKey
        return obj

    def __eq__(self, other):
        return isinstance(other, _SigningKey) and self._signing_key == other._signing_key


class VerifyKey(_VerifyKey):
    __slots__ = ()

    def __eq__(self, other):
        return isinstance(other, _VerifyKey) and self._key == other._key


class PrivateKey(_PrivateKey):
    __slots__ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.public_key.__class__ = PublicKey

    @classmethod
    def generate(cls, *args, **kwargs) -> "PrivateKey":
        obj = super().generate(*args, **kwargs)
        obj.__class__ = PrivateKey
        return obj

    def __eq__(self, other):
        return isinstance(other, _PrivateKey) and self._private_key == other._private_key


class PublicKey(_PublicKey):
    __slots__ = ()

    def __eq__(self, other):
        return isinstance(other, _PublicKey) and self._public_key == other._public_key


def export_root_verify_key(key: VerifyKey) -> str:
    """
    Raises:
        ValueError
    """
    # Note we replace padding char `=` by a simple `s` (which is not part of
    # the base32 table so no risk of collision) to avoid copy/paste errors
    # and silly escaping issues when carrying the key around.
    return b32encode(key.encode()).decode("utf8").replace("=", "s")


def import_root_verify_key(raw: str) -> VerifyKey:
    """
    Raises:
        ValueError
    """
    if isinstance(raw, VerifyKey):
        # Useful during tests
        return raw
    try:
        return VerifyKey(b32decode(raw.replace("s", "=").encode("utf8")))
    except CryptoError as exc:
        raise ValueError("Invalid verify key") from exc
