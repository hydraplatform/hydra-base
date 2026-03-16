import base64
import io
import os

import pyotp
import qrcode

from typing import Dict

from hydra_base import config

SECRET_BYTE_LENGTH = 20


def totp(secret: str) -> str:
    """
      RFC6238 TOTP via pyotp.
    """
    return pyotp.TOTP(secret).now()


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
    issuer = config.get("otp", "issuer", "hydra.org")
    uri = pyotp.TOTP(secret).provisioning_uri(username, issuer_name=issuer)
    img_url = make_data_image_url(uri)

    return {
        "secret": secret,  # Base32-encoded TOTP secret
        "uri": uri,        # otpauth:// string
        "img": img_url     # QR code in data:image/png form
    }
