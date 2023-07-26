import datetime
import pytest

from packaging import version

import hydra_base
from hydra_base.lib.objects import JSONObject
from hydra_base.lib.otp.otp import (
    make_user_secret_bundle
)

min_lib_versions = {
#    "qrcode": version.parse("7.4.2")
    "cryptography": version.parse("40.0.0")
}

opt_user_id = None

@pytest.fixture
def user_json_object():
    user = JSONObject(dict(
        username=f"otp_user",
        password="otp_user_password",
        display_name="OTP User Display"
    ))
    return user


def test_gen_secret():
    pass


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
        user = client.add_user(user_json_object)
        otp_user_id = user.id

    def test_db_secret(self, client, user_json_object):
        client.logout()
        client.login(user_json_object["username"], user_json_object["password"])
        otp = client.get_user_otp()
        assert hasattr(otp, "secret")
        assert otp.secret is not None
        assert len(otp.secret) == 32

    def test_generate_otp_bundle(self, user_json_object):
        otp = make_user_secret_bundle(user_json_object["username"])
        keys = ("secret", "uri", "img")
        for key in keys:
            assert key in otp

        assert len(otp["secret"]) == 32
        assert otp["uri"].startswith("otpauth://")
        assert otp["img"].startswith("data:image/png;Base64")
