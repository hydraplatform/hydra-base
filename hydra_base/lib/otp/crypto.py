import base64
import codecs
import getpass
import hashlib
import io
import json
import os
import sys

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
    md5 = hashlib.md5()
    md5.update(iv_seed)
    shi0 = md5.digest()

    kdf = Scrypt(salt=shi0, length=32, n=2**12, r=8, p=1)
    key = kdf.derive(key_seed)

    return shi0, key


def aes256_enc_buf(iv: bytes, key: bytes, buf: bytes) -> str:
    aes = Cipher(algorithms.AES256(key), modes.CBC(iv))
    aes_enc = aes.encryptor()
    padder = padding.PKCS7(128).padder()

    pd = padder.update(buf) + padder.finalize()
    ct = aes_enc.update(pd) + aes_enc.finalize()

    return base64.b64encode(ct)


def aes256_dec_buf(iv: bytes, key: bytes, buf: str) -> bytes:
    aes = Cipher(algorithms.AES256(key), modes.CBC(iv))
    aes_dec = aes.decryptor()
    unpadder = padding.PKCS7(128).unpadder()

    pad_b64 = len(buf) % 4

    ct = base64.b64decode(buf + pad_b64*b'=')
    ppt = aes_dec.update(ct) + aes_dec.finalize()
    pt = unpadder.update(ppt) + unpadder.finalize()

    return pt


def get_iv_key() -> Tuple[bytes, bytes]:
    iv_seed = config.CONFIG.get("otp", "iv_seed")
    key_seed = config.CONFIG.get("otp", "key_seed")
    return gen_iv_key(iv_seed.encode(), key_seed.encode())


if __name__ == "__main__":
    iv, key = get_iv_key()
    breakpoint()
