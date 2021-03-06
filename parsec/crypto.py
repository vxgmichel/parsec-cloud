from typing import Tuple, NewType, Optional
import pendulum
from secrets import token_hex
from nacl.public import SealedBox
from nacl.bindings import crypto_sign_BYTES
from nacl.secret import SecretBox
from nacl.utils import random
from nacl.pwhash import argon2i
from nacl.exceptions import CryptoError, BadSignatureError

from parsec.types import DeviceID
from parsec.crypto_types import (
    PrivateKey,
    PublicKey,
    SigningKey,
    VerifyKey,
    export_root_verify_key,
    import_root_verify_key,
)
from parsec.schema import UnknownCheckedSchema, fields, ValidationError

# TODO: should isolate generic serialization stuff from api
from parsec.api.protocole import ProtocoleError
from parsec.api.protocole.base import Serializer


__all__ = (
    "CryptoError",
    "BadSignatureError",
    "PrivateKey",
    "PublicKey",
    "SigningKey",
    "VerifyKey",
    "SymetricKey",
    "HashDigest",
    "generate_token",
    "export_root_verify_key",
    "import_root_verify_key",
)


SymetricKey = NewType("SymetricKey", bytes)
HashDigest = NewType("HashDigest", bytes)


# TODO: SENSITIVE is really slow which is not good for unittests...
# CRYPTO_OPSLIMIT = argon2i.OPSLIMIT_SENSITIVE
# CRYPTO_MEMLIMIT = argon2i.MEMLIMIT_SENSITIVE
CRYPTO_OPSLIMIT = argon2i.OPSLIMIT_INTERACTIVE
CRYPTO_MEMLIMIT = argon2i.MEMLIMIT_INTERACTIVE


class SignedMetadataSchema(UnknownCheckedSchema):
    # No device_id means it has been signed by the root key
    device_id = fields.DeviceID(missing=None)
    timestamp = fields.DateTime(required=True)
    content = fields.Bytes(required=True)


signed_metadata_serializer = Serializer(SignedMetadataSchema)


# Note to simplify things, we adopt CryptoError as our root error type


class CryptoMetadataError(CryptoError):
    pass


def generate_token(length: int):
    return token_hex(length)


def generate_secret_key():
    return random(SecretBox.KEY_SIZE)


def derivate_secret_key_from_password(password: str, salt: bytes = None) -> Tuple[bytes, bytes]:
    salt = salt or random(argon2i.SALTBYTES)
    key = argon2i.kdf(
        SecretBox.KEY_SIZE,
        password.encode("utf8"),
        salt,
        opslimit=CRYPTO_OPSLIMIT,
        memlimit=CRYPTO_MEMLIMIT,
    )
    return key, salt


def encrypt_raw_with_secret_key(key: bytes, data: bytes) -> bytes:
    """
    Raises:
        CryptoError: if key is invalid.
    """
    box = SecretBox(key)
    return box.encrypt(data)


def decrypt_raw_with_secret_key(key: bytes, ciphered: bytes) -> bytes:
    """
    Raises:
        CryptoError: if key is invalid.
    """
    box = SecretBox(key)
    return box.decrypt(ciphered)


def sign_and_add_meta(
    device_id: Optional[DeviceID], device_signkey: SigningKey, signedmeta: bytes
) -> bytes:
    """
    Raises:
        CryptoError: if the signature operation fails.
    """
    return signed_metadata_serializer.dumps(
        {
            "device_id": device_id,
            "timestamp": pendulum.now(),
            "content": device_signkey.sign(signedmeta),
        }
    )


def decode_signedmeta(signedmeta: bytes) -> Tuple[Optional[DeviceID], bytes]:
    """
    Raises:
        CryptoMetadataError: if the metadata cannot be extracted
    """
    try:
        meta = signed_metadata_serializer.loads(signedmeta)
        if meta["device_id"]:
            device_id = DeviceID(meta["device_id"])
        else:
            device_id = None
        return device_id, meta["content"]

    except (ValidationError, UnicodeDecodeError, ProtocoleError) as exc:
        raise CryptoMetadataError(
            "Message doesn't contain author metadata along with signed message"
        ) from exc


def unsecure_extract_msg_from_signed(signed: bytes) -> bytes:
    return signed[crypto_sign_BYTES:]


def encrypt_for_self(
    device_id: DeviceID, device_signkey: SigningKey, device_pubkey: PublicKey, data: bytes
) -> bytes:
    return encrypt_for(device_id, device_signkey, device_pubkey, data)


def encrypt_raw_for(recipient_pubkey: PublicKey, data: bytes) -> bytes:
    return SealedBox(recipient_pubkey).encrypt(data)


def decrypt_raw_for(recipient_privkey: PrivateKey, ciphered: bytes):
    return SealedBox(recipient_privkey).decrypt(ciphered)


def encrypt_for(
    author_id: DeviceID, author_signkey: SigningKey, recipient_pubkey: PublicKey, data: bytes
) -> bytes:
    """
    Sign and encrypt a message.

    Raises:
        CryptoError: if encryption or signature fails.
    """
    signedmeta = sign_and_add_meta(author_id, author_signkey, data)

    box = SealedBox(recipient_pubkey)
    return box.encrypt(signedmeta)


def decrypt_for(recipient_privkey: PrivateKey, ciphered: bytes) -> Tuple[DeviceID, bytes]:
    """
    Decrypt a message and return it signed data and author metadata.

    Raises:
        CryptoMetadataError: if the author metadata cannot be extracted.
        CryptoError: if decryption or signature verifying fails.

    Returns: a tuple of (<user_id>, <device_name>, <signed_message>)

    Note: Once decrypted, the message should be passed to
    :func:`verify_signature_from` to be finally converted to plain text.
    """
    box = SealedBox(recipient_privkey)
    signedmeta = box.decrypt(ciphered)
    return decode_signedmeta(signedmeta)


def verify_signature_from(author_verifykey: VerifyKey, signed_text: bytes) -> bytes:
    """
    Verify signature and decode message.

    Returns: The plain text message.

    Raises:
         CryptoError: if signature was forged or otherwise corrupt.
    """
    return author_verifykey.verify(signed_text)


def encrypt_with_secret_key(
    author_id: DeviceID, author_signkey: SigningKey, key: bytes, data: bytes
) -> bytes:
    """
    Sign and encrypt a message with a symetric key.

    Raises:
        CryptoError: if the encryption or signature operation fails.
    """
    signedmeta = sign_and_add_meta(author_id, author_signkey, data)
    box = SecretBox(key)
    return box.encrypt(signedmeta)


def decrypt_with_secret_key(key: bytes, ciphered: bytes) -> Tuple[DeviceID, bytes]:
    """
    Decrypt a signed message with a symetric key.

    Raises:
        CryptoMetadataError: if the author metadata cannot be extracted.
        CryptoError: if decryption or signature verifying fails.

    Returns: a tuple of (<user_id>, <device_name>, <signed_message>)

    Note: Once decrypted, the message should be passed to
    :func:`verify_signature_from` to be finally converted to plain text.
    """
    box = SecretBox(key)
    signedmeta = box.decrypt(ciphered)
    return decode_signedmeta(signedmeta)
