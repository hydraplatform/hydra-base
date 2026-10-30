"""
The S3 sources of a deployment: which endpoint, region and key to use for each bucket.

A deployment can read buckets on different S3 services (AWS, OVH, a local server...) with
different keys. Two JSON files, written by the deployment tooling (hcli in hwi-env), describe them:

  s3-buckets.json      {"deployment": "...",
                        "buckets": {"<name as records carry it>": {
                            "bucket": "<real bucket name>", "role": "data|static|results|datasets|...",
                            "access": "rw|ro", "region": "...", "endpoint": "https://..." or null (AWS),
                            "addressing_style": "auto|virtual|path", "credentials_id": "c1"}}}
  s3-credentials.json  {"c1": {"aws_access_key_id": "...", "aws_secret_access_key": "..."}}

Where they are, first found:
  - hydra.ini [storage_s3] buckets_file / credentials_file;
  - the environment: HYDRA_S3_BUCKETS_FILE / HYDRA_S3_CREDENTIALS_FILE;
  - $HOME/.hydra/ or $HOME/.hydra/config/ (as the services mount their config).

Without a buckets file, there is no map: callers keep their previous behaviour (the default AWS
identity). With one, a bucket in it is read with its own endpoint and key; other buckets keep the
previous behaviour, except where the caller requires the map (the datasets store: its pointers
are only read through a listed bucket).
"""
import json
import logging
import os
import threading

from urllib.parse import urlparse

from hydra_base import config

log = logging.getLogger(__name__)


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

    def __init__(self, buckets_file=None, credentials_file=None):
        home = os.getenv("HOME", "/root")
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
        log.info("S3 sources: %s bucket(s) from %s", len(self.buckets), self.buckets_file)

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

    def real_bucket(self, bucket):
        """ The bucket's real name (it may differ from the name records carry). """
        return self.entry(bucket).get("bucket") or bucket

    def s3fs_options(self, bucket):
        """ fsspec / s3fs storage options for the bucket (key, secret, endpoint, region, style). """
        e = self.entry(bucket)
        k = self._key(bucket)
        client_kwargs = {}
        if e.get("endpoint"):
            client_kwargs["endpoint_url"] = e["endpoint"]
        if e.get("region"):
            client_kwargs["region_name"] = e["region"]
        return {
            "anon": False,
            "key": k["aws_access_key_id"],
            "secret": k["aws_secret_access_key"],
            "client_kwargs": client_kwargs,
            "config_kwargs": {"s3": {"addressing_style": e.get("addressing_style") or "auto"}},
        }

    def storage_options(self, url, required=False):
        """
          For an s3:// url whose bucket is in the map: (the url with the bucket's real name, its
          s3fs options). Other urls, buckets not in the map, or no map: (url, None), so the caller
          keeps its previous behaviour (the default AWS identity: public data, pywr inputs).
          required=True: a bucket missing from the map (or no map) raises S3SourceError.
        """
        u = urlparse(url)
        if u.scheme != "s3":
            return url, None
        if not self.active or u.netloc not in self.buckets:
            if required and self.active:
                raise S3SourceError(f"Bucket '{u.netloc}' is not in this deployment's S3 map ({self.buckets_file})")
            if required:
                raise S3SourceError(f"Bucket '{u.netloc}': this deployment has no S3 map")
            return url, None
        real = self.real_bucket(u.netloc)
        return f"s3://{real}{u.path}", self.s3fs_options(u.netloc)
