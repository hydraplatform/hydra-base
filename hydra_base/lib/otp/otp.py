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
      RFC6238 TOTP, passing the current <window>-size period to hotp() as the
      counter.
    """
    return hotp(secret, int(time.time()/window), digits, hash)


def make_uri(otp_type: str, label: str, secret: str, parameters: Dict[str, str]) -> str:
    """
      Returns a uri in "otpauth://" format commonly used by authenticators, e.g.
      "otpauth://totp/waterstrategy.org:otp_user?secret=<b32_secret>&issuer=hydra.org"

      This typically includes the <otp_type> (here 'totp') a <label> for client-side
      display including the user to whom the secret was issued and the Base32-encoded
      <secret>.
    """
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
    """
      Returns a Base32-encoded string which represents a TOTP secret.

      This is derived from <sec_len> pseudo-random bytes ultimately retrieved
      from /dev/urandom.
    """
    sec = os.urandom(sec_len)
    return base64.b32encode(sec).decode()


def make_user_secret_bundle(username: str) -> Dict[str, str]:
    """
      Returns a convenient dict containing all of the information required
      for a user to configure their Authenticator of choice to generate codes.
    """
    secret = gen_secret()
    uri = make_uri("totp", f"waterstrategy.org:{username}", secret, {"issuer": "hydra.org"})
    img_url = make_data_image_url(uri)

    return {
        "secret": secret,  # Base32-encoded TOTP secret
        "uri": uri,        # otpauth:// string
        "img": img_url     # QR code in data:image/png form
    }
