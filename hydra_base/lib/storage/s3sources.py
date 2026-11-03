"""
The S3 sources of a deployment: which endpoint, region and credentials to use for each bucket.

A deployment can read buckets on different S3 services (AWS, OVH, a local server...) with
different credentials. Two JSON files describe them. They are plain files: write them by hand,
from your own configuration tool, or with hwi-env's hcli (which builds them from nx-vault).
hydra-base itself reads only the files and never depends on any of those tools.

  s3-buckets.json      {"deployment": "...",
                        "buckets": {"<name as records carry it>": {
                            "bucket": "<real bucket name>" (optional, default: the same name),
                            "role": "data|static|results|datasets|...", "access": "rw|ro",
                            "region": "...", "endpoint": "https://..." or null (AWS),
                            "addressing_style": "auto|virtual|path",
                            <credentials, one of:>
                            "credentials_id": "c1"         a key from s3-credentials.json
                            "auth": "default"              the standard AWS chain (environment,
                                                           ~/.aws, instance or pod role)
                            "auth": "profile", "profile": "<name>"   a named profile in ~/.aws
                            "auth": "anonymous"            a public bucket, no credentials
                        }}}
  s3-credentials.json  {"c1": {"aws_access_key_id": "...", "aws_secret_access_key": "..."}}
                       (only needed by entries with credentials_id)

Where they are, first found:
  - hydra.ini [storage_s3] buckets_file / credentials_file;
  - the environment: HYDRA_S3_BUCKETS_FILE / HYDRA_S3_CREDENTIALS_FILE;
  - $HOME/.hydra/ or $HOME/.hydra/config/ (as the services mount their config).

Without a buckets file, there is no map: callers keep their previous behaviour (the default AWS
identity). With one, a bucket in it is read with its own endpoint and credentials; other buckets
keep the previous behaviour, except where the caller requires the map (the datasets store: its
pointers are only read through a listed bucket).

Strict mode (hydra.ini [storage_s3] strict = true, or HYDRA_S3_STRICT=true): every s3:// url must
name a bucket of the map, for every caller; anything else is an error that names the bucket,
never a silent fall back to whatever AWS identity the process happens to have. Use it once the
map lists every bucket the deployment's records point at.
"""
import json
import logging
import os
import threading

from urllib.parse import urlparse

from hydra_base import config

log = logging.getLogger(__name__)


_TRUE = ("1", "true", "yes", "on")
_AUTH_MODES = ("default", "profile", "anonymous")


def _truthy(value):
    return str(value).strip().lower() in _TRUE if value is not None else False


class S3SourceError(Exception):
    """ A bucket that can't be reached through the map (unknown, no key, bad file). """


def _first_existing(paths):
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return None


class S3SourceMap():
    """
      The buckets map and keys of this deployment, loaded once per process.
      Keys never leave this object except as s3fs options.
    """
    _instance = None
    _lock = threading.Lock()

    def __init__(self, buckets_file=None, credentials_file=None, strict=None):
        home = os.getenv("HOME", "/root")
        if strict is None:
            strict = _truthy(config.get("storage_s3", "strict")) or _truthy(os.getenv("HYDRA_S3_STRICT"))
        self.strict = bool(strict)
        self.buckets_file = buckets_file or _first_existing([
            config.get("storage_s3", "buckets_file"),
            os.getenv("HYDRA_S3_BUCKETS_FILE"),
            os.path.join(home, ".hydra", "s3-buckets.json"),
            os.path.join(home, ".hydra", "config", "s3-buckets.json"),
        ])
        self.credentials_file = credentials_file or _first_existing([
            config.get("storage_s3", "credentials_file"),
            os.getenv("HYDRA_S3_CREDENTIALS_FILE"),
            os.path.join(home, ".hydra", "s3-credentials.json"),
            os.path.join(home, ".hydra", "config", "s3-credentials.json"),
        ])
        self.buckets = {}
        self._keys = {}
        if self.buckets_file:
            self._load()

    @classmethod
    def get(cls):
        """ The process-wide map (loaded on first use). """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset(cls):
        """ Forget the loaded map (tests, or after the files changed). """
        with cls._lock:
            cls._instance = None

    def _load(self):
        try:
            with open(self.buckets_file) as fp:
                doc = json.load(fp)
            self.buckets = doc.get("buckets", {}) if isinstance(doc, dict) else {}
        except (OSError, ValueError) as err:
            raise S3SourceError(f"Unable to read the S3 buckets map {self.buckets_file}: {err}") from err
        if self.credentials_file:
            try:
                with open(self.credentials_file) as fp:
                    self._keys = json.load(fp)
            except (OSError, ValueError) as err:
                raise S3SourceError(f"Unable to read the S3 credentials file {self.credentials_file}") from err
        log.info("S3 sources: %s bucket(s) from %s%s", len(self.buckets), self.buckets_file,
                 " (strict)" if self.strict else "")

    @property
    def active(self):
        """ True when this deployment has a buckets map. """
        return bool(self.buckets_file)

    def entry(self, bucket):
        """ The map entry of the bucket name as records carry it. """
        try:
            return self.buckets[bucket]
        except KeyError:
            raise S3SourceError(f"Bucket '{bucket}' is not in this deployment's S3 map ({self.buckets_file})") from None

    def _key(self, bucket):
        cid = self.entry(bucket).get("credentials_id")
        key = self._keys.get(cid) if cid else None
        if not key or not key.get("aws_access_key_id") or not key.get("aws_secret_access_key"):
            raise S3SourceError(f"No key for bucket '{bucket}' (credentials_id {cid!r}) in {self.credentials_file}")
        return key

    def _credentials(self, bucket):
        """ The s3fs credential options of the bucket's entry (never logged). """
        e = self.entry(bucket)
        if e.get("credentials_id"):
            k = self._key(bucket)
            return {"anon": False, "key": k["aws_access_key_id"], "secret": k["aws_secret_access_key"]}
        auth = e.get("auth")
        if auth == "anonymous":
            return {"anon": True}
        if auth == "default":
            return {"anon": False}
        if auth == "profile":
            if not e.get("profile"):
                raise S3SourceError(f"Bucket '{bucket}': auth 'profile' needs a 'profile' name")
            return {"anon": False, "profile": e["profile"]}
        if auth is None:
            raise S3SourceError(f"Bucket '{bucket}' has no credentials_id or auth in {self.buckets_file}")
        raise S3SourceError(f"Bucket '{bucket}': unknown auth {auth!r} (expected one of {', '.join(_AUTH_MODES)})")

    def real_bucket(self, bucket):
        """ The bucket's real name (it may differ from the name records carry). """
        return self.entry(bucket).get("bucket") or bucket

    def s3fs_options(self, bucket):
        """ fsspec / s3fs storage options for the bucket (credentials, endpoint, region, style). """
        e = self.entry(bucket)
        client_kwargs = {}
        if e.get("endpoint"):
            client_kwargs["endpoint_url"] = e["endpoint"]
        if e.get("region"):
            client_kwargs["region_name"] = e["region"]
        return {
            **self._credentials(bucket),
            "client_kwargs": client_kwargs,
            "config_kwargs": {"s3": {"addressing_style": e.get("addressing_style") or "auto"}},
        }

    def storage_options(self, url, required=False):
        """
          For an s3:// url whose bucket is in the map: (the url with the bucket's real name, its
          s3fs options). Other urls, buckets not in the map, or no map: (url, None), so the caller
          keeps its previous behaviour (the default AWS identity: public data, pywr inputs).
          required=True, or strict mode: a bucket missing from the map (or no map) raises
          S3SourceError.
        """
        u = urlparse(url)
        if u.scheme != "s3":
            return url, None
        required = required or self.strict
        if not self.active or u.netloc not in self.buckets:
            if not required:
                return url, None
            mode = " (strict mode)" if self.strict else ""
            if self.active:
                raise S3SourceError(f"Bucket '{u.netloc}' is not in this deployment's S3 map{mode} ({self.buckets_file})")
            raise S3SourceError(f"Bucket '{u.netloc}': this deployment has no S3 map{mode}")
        real = self.real_bucket(u.netloc)
        return f"s3://{real}{u.path}", self.s3fs_options(u.netloc)
