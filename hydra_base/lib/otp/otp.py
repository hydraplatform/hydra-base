import base64
import hmac
import io
import os
import struct
import time
import urllib.parse

import qrcode

from typing import Dict

SECRET_BYTE_LENGTH = 20

def hotp(secret: str, counter: int, digits: int=6, hash: str="sha1") -> str:
    """
      RFC4226 HOTP : rfc requires digits>=6 and keylen>=128b
    """
    # base64 module requires uppercase and mod 8 padding
    key = base64.b32decode(secret.upper() + '='*((8-len(secret))%8))
    msg = struct.pack('>Q', counter)
    mac = hmac.digest(key, msg, hash)
    # Dynamic Truncation (rfc s5.3) uses lower order 4b of last digest byte...
    offset = mac[-1] & 0x0f
    # ...and last 31b of resulting offset range...
    code = int.from_bytes(mac[offset:offset+4], 'big') & 0x7fffffff
    # ...result is then mod 10**digits
    return str(code)[-digits:].zfill(digits)


def totp(secret: str, window: int=30, digits: int=6, hash: str="sha1") -> str:
    """
      RFC6238 TOTP
    """
    return hotp(secret, int(time.time()/window), digits, hash)


def make_uri(otp_type: str, label: str, secret: str, parameters: Dict[str, str]) -> str:
    params = {"secret": secret, **parameters}
    qs = urllib.parse.urlencode(params)
    return urllib.parse.urlunparse(("otpauth", otp_type, label, None, qs, None))


def make_data_image_url(uri: str) -> str:
    """
      RFC 2397 data url for a base64-encoded qr code
      representing a otpauth:// uri.
    """
    qr = qrcode.QRCode(
        box_size=5,
        border=4
    )
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image()
    buf = io.BytesIO()
    img.save(buf)
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read())
    return f"data:image/png;Base64,{img_b64.decode()}"


def gen_secret(sec_len: int=SECRET_BYTE_LENGTH) -> str:
    sec = os.urandom(sec_len)
    return base64.b32encode(sec).decode()


def get_user_secret(username: str) -> str:
    pass


def make_user_secret_bundle(username: str) -> Dict[str, str]:
    secret = gen_secret()
    uri = make_uri("totp", f"waterstrategy.org:{username}", secret, {"issuer": "hydra.org"})
    img_url = make_data_image_url(uri)

    return {
        "secret": secret,
        "uri": uri,
        "img": img_url
    }


if __name__ == "__main__":
    breakpoint()
