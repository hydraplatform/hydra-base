import base64
import datetime
import os
import pytest

from packaging import version

import hydra_base
from hydra_base.lib.objects import JSONObject
from hydra_base.lib.users import (
    activate_user_otp,
    deactivate_user_otp,
    user_has_otp
)
from hydra_base.util.hdb import verify_otp
from hydra_base.lib.otp.otp import (
    make_user_secret_bundle,
    totp,
    SECRET_BYTE_LENGTH
)

min_lib_versions = {
#    "qrcode": version.parse("7.4.2")  # Current qrcode does not define a __version__
    "cryptography": version.parse("40.0.0")
}

otp_user_id = None

@pytest.fixture
def user_json_object():
    user = JSONObject(dict(
        username=f"otp_user",
        password="otp_user_password",
        display_name="OTP User Display"
    ))
    return user

@pytest.fixture
def temp_user_json_object():
    user = JSONObject(dict(
        username=f"temp_otp_user",
        password="temp_otp_user_password",
        display_name="Temp OTP User Display"
    ))
    return user

@pytest.fixture
def secret_b32():
    sec = os.urandom(SECRET_BYTE_LENGTH)
    sec_b32 = base64.b32encode(sec)
    return sec_b32.decode()

SECRET_B32_LENGTH = SECRET_BYTE_LENGTH*8/5

class TestTOP():

    def test_lib_versions(self):
        """
          Are the required libraries present and adequate versions?
        """
        import importlib
        for libname, semver in min_lib_versions.items():
            lib = importlib.import_module(libname)
            assert version.parse(lib.__version__) >= semver

    def test_add_user(self, client, user_json_object):
        """
            Adds a user and activates OTP for use in other tests
        """
        user = client.add_user(user_json_object)
        otp = activate_user_otp(user.id)
        otp_user_id = user.id

    def test_db_secret(self, client, user_json_object):
        """
            Is the secret retrieved from the DB plaintext with the correct format?
        """
        client.logout()
        client.login(user_json_object["username"], user_json_object["password"])
        otp = client.get_user_otp()
        assert hasattr(otp, "secret")  # Secret must be present
        assert otp.secret is not None  # Secret must not be null
        assert len(otp.secret) == SECRET_B32_LENGTH  # Secret must be 20bytes*8bits/5bits long
        assert isinstance(otp.secret, str)  # Secret must be str
        sec_bytes = base64.b32decode(otp.secret)  # Secret must be valid B32
        assert isinstance(sec_bytes, bytes)

    def test_generate_otp_bundle(self, user_json_object):
        """
            Does the bundle of OTP info for a user contain the required values:
                - Base32 encoded secret?
                - "otpauth://" string?
                - QR code as data:image?
        """
        otp = make_user_secret_bundle(user_json_object["username"])
        keys = ("secret", "uri", "img")
        for key in keys:
            assert key in otp

        assert len(otp["secret"]) == SECRET_B32_LENGTH
        assert otp["uri"].startswith("otpauth://")
        assert otp["img"].startswith("data:image/png;Base64")

    def test_verify_otp_code(self, client, user_json_object):
        """
            Does the hb.verify_otp routine agree with a manually-generated code?
        """
        client.logout()
        user_id, _ = client.login(user_json_object["username"], user_json_object["password"])
        otp = client.get_user_otp()
        local_code = totp(otp.secret)
        verify_otp(user_id, local_code)

    def test_default_otp_not_activated(self, client, temp_user_json_object):
        """
            Verify user accounts do not have OTP active by default
        """
        user = client.add_user(temp_user_json_object)
        assert user_has_otp(user.id) == False
        client.delete_user(user.id)

    def test_activate_user_otp(self, client, temp_user_json_object):
        """
            Can OTP be activated for a user?
        """
        user = client.add_user(temp_user_json_object)
        assert user_has_otp(user.id) == False
        otp = activate_user_otp(user.id)
        assert user_has_otp(user.id) == True
        client.delete_user(user.id)

    def test_deactivate_user_otp(self, client, temp_user_json_object):
        """
            Can OTP be deactivated for a user?
        """
        user = client.add_user(temp_user_json_object)
        assert user_has_otp(user.id) == False
        otp = activate_user_otp(user.id)
        assert user_has_otp(user.id) == True
        deactivate_user_otp(user.id)
        assert user_has_otp(user.id) == False
        client.delete_user(user.id)
