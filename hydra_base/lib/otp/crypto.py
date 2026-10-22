import base64
import hashlib

from typing import Tuple

from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import (
    Cipher,
    algorithms,
    modes
)

from hydra_base import config

buf_sz = 512
chr_per_line = 64
aes_block_sz = 16

def gen_iv_key(iv_seed: bytes, key_seed: bytes) -> Tuple[bytes, bytes]:
    """
        Generate an initialisation vector and key for symmetric encryption.
        Note that the 16 byte iv is also used as the salt for Scrypt key
        derivation. The (n,r,p) args to Scrypt represent a reasonable
        compromise between security and key-generation time.

        Note that identical (n,r,p) args are required for repeatable
        key generation.
    """
    md5 = hashlib.md5()
    md5.update(iv_seed)
    iv = md5.digest()

    kdf = Scrypt(salt=iv, length=32, n=2**19, r=8, p=1)
    key = kdf.derive(key_seed)

    return iv, key


def aes256_enc_buf(iv: bytes, key: bytes, buf: bytes) -> bytes:
    """
        Encode the <buf> argument with AES256 using the specified
        <iv> and <key> args. Return B64 encoded bytes
    """
    aes = Cipher(algorithms.AES256(key), modes.CBC(iv))
    aes_enc = aes.encryptor()
    padder = padding.PKCS7(128).padder()

    pd = padder.update(buf) + padder.finalize()
    ct = aes_enc.update(pd) + aes_enc.finalize()
    return base64.b64encode(ct)


def aes256_dec_buf(iv: bytes, key: bytes, buf: str) -> bytes:
    """
        Decode the B64-encoded AES256 ciphertext <buf> argument
        using the specified <iv> and <key>
    """
    aes = Cipher(algorithms.AES256(key), modes.CBC(iv))
    aes_dec = aes.decryptor()
    unpadder = padding.PKCS7(128).unpadder()

    pad_b64 = len(buf) % 4

    ct = base64.b64decode(buf + pad_b64*b'=')
    ppt = aes_dec.update(ct) + aes_dec.finalize()
    pt = unpadder.update(ppt) + unpadder.finalize()

    return pt


def get_iv_key() -> Tuple[bytes, bytes]:
    """
        Retrieve the config [otp] section's iv_seed and
        key_seed values and use these to generate iv and key
    """
    iv_seed = config.CONFIG.get("otp", "iv_seed")
    key_seed = config.CONFIG.get("otp", "key_seed")
    return gen_iv_key(iv_seed.encode(), key_seed.encode())
