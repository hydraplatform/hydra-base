"""
Dataset values kept in a datasets store: an S3 bucket of HDF5 files, one per dataset.

A value moved to the store is replaced, in tDataset.value, by a pointer:

  {"data": {"url": "s3://<bucket>/datasets-v2/<id>.h5", "group": "dataset_<id>", "sha256": "<hex>",
            "format": "json-text+frame/1"}}

sha256 is the hash of the whole file. What the value is depends on "format":

  json-text+frame/1  the file holds the row's original text, raw utf-8 bytes, in the dataset
                     "value_json" (file attribute nx_format = the same name), plus the parsed
                     frame in <group> for readers that want a frame. The value is value_json,
                     returned as it is: the frame can't reproduce the text (keys, the spelling
                     of numbers, {"type": "dataframe", ...} wrappers with their other fields).
  (no format)        the first migration's objects: a pandas HDFStore frame in <group>, read back
                     as df.to_json(). Kept for those pointers only; most of them don't give back
                     the original text.

An unknown format is an error, never a guess.

Only that shape is resolved (sha256 is what tells it apart from model results, which also carry
{"data": {"url", "group"}} but are read through HdfStorageAdapter). The bucket's endpoint and key
come from the deployment's S3 map (s3sources.S3SourceMap); without a map, pointers are returned
as they are (previous behaviour).

Files are cached on local disk by sha256 (content never changes for a given hash), in
hydra.ini [storage_s3] cache_dir (default: <tmp>/hydra-datasets-cache), at most cache_max_mb
(default 2048; least recently used first out). Decoded texts are also kept in memory
(memory_cache_items, default 128).
"""
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
from collections import OrderedDict

from hydra_base import config
from hydra_base.exceptions import HydraError
from hydra_base.lib.storage.s3sources import S3SourceMap, S3SourceError

log = logging.getLogger(__name__)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
TEXT_FORMAT = "json-text+frame/1"
_FORMATS = (None, TEXT_FORMAT)
# A pointer is short; a real value that merely mentions these words is not parsed.
_MAX_POINTER_LENGTH = 2048


class DatasetStoreError(HydraError):
    """ A store pointer that can't be read (unknown bucket, network, missing object, bad hash). """


def _pointer(value):
    """
      The (url, group, sha256, format) of a store pointer (format None when absent), or None for
      any other value. An unknown format still makes a pointer: resolving it is then an error.
      Cheap tests first: most values are numbers, arrays or dataframes.
    """
    if not isinstance(value, str) or len(value) > _MAX_POINTER_LENGTH:
        return None
    text = value.strip()
    if not text.startswith("{") or '"sha256"' not in text or "s3:" not in text:
        return None
    try:
        doc = json.loads(text)
    except ValueError:
        return None
    if not isinstance(doc, dict) or list(doc) != ["data"] or not isinstance(doc["data"], dict):
        return None
    data = doc["data"]
    url, group, sha256, fmt = data.get("url"), data.get("group"), data.get("sha256"), data.get("format")
    if not (isinstance(url, str) and url.startswith("s3://") and isinstance(group, str) and group
            and isinstance(sha256, str) and _SHA256.match(sha256)):
        return None
    return url, group, sha256, fmt


def is_store_pointer(value):
    """ True when the value is a datasets store pointer. """
    return _pointer(value) is not None


class DatasetStore():
    """ Reads store pointers back into values; one per process (get()). """
    _instance = None
    _lock = threading.Lock()

    def __init__(self, cache_dir=None, cache_max_mb=None, memory_items=None, sources=None):
        self.cache_dir = cache_dir or config.get("storage_s3", "cache_dir") \
            or os.path.join(tempfile.gettempdir(), "hydra-datasets-cache")
        self.cache_max_bytes = int(cache_max_mb or config.get("storage_s3", "cache_max_mb") or 2048) * 1024 * 1024
        self.memory_items = int(memory_items or config.get("storage_s3", "memory_cache_items") or 128)
        self._sources = sources
        self._texts = OrderedDict()
        self._texts_lock = threading.Lock()
        self._warned_no_map = False

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset(cls):
        with cls._lock:
            cls._instance = None

    @property
    def sources(self):
        return self._sources or S3SourceMap.get()

    def resolve(self, value, dataset_id=None):
        """ The value itself, unless it is a store pointer: then the stored value's text. """
        ptr = _pointer(value)
        if ptr is None:
            return value
        url, group, sha256, fmt = ptr
        who = f"Dataset {dataset_id} ({url})" if dataset_id is not None else url
        if fmt not in _FORMATS:
            raise DatasetStoreError(f"{who}: unknown datasets store format {fmt!r}")
        if not self.sources.active:
            if not self._warned_no_map:
                log.warning("Dataset values in a datasets store (%s) can't be read: this deployment has no S3 map.", url)
                self._warned_no_map = True
            return value

        mkey = (sha256, group, fmt)
        with self._texts_lock:
            if mkey in self._texts:
                self._texts.move_to_end(mkey)
                return self._texts[mkey]

        bucket = url[len("s3://"):].split("/", 1)[0]
        try:
            path = self._local_file(url, sha256)
            text = self._read(path, group, fmt)
        except DatasetStoreError as err:
            raise DatasetStoreError(f"{who}: {err}") from err
        except (S3SourceError, OSError, KeyError, ValueError) as err:
            # Never the key: S3SourceError and s3fs errors name the bucket and object only.
            raise DatasetStoreError(f"{who}: unable to read the value from bucket "
                                    f"'{bucket}' ({type(err).__name__}: {err})") from err

        with self._texts_lock:
            self._texts[mkey] = text
            while len(self._texts) > self.memory_items:
                self._texts.popitem(last=False)
        return text

    def _local_file(self, url, sha256):
        """ The cached file for sha256, fetched and checked when it isn't there yet. """
        path = os.path.join(self.cache_dir, f"{sha256}.h5")
        if os.path.exists(path):
            os.utime(path)  # most recently used
            return path

        import fsspec
        real_url, options = self.sources.storage_options(url, required=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.cache_dir, suffix=".part")
        try:
            digest = hashlib.sha256()
            with os.fdopen(fd, "wb") as out, fsspec.open(real_url, mode="rb", **options) as src:
                while chunk := src.read(1024 * 1024):
                    digest.update(chunk)
                    out.write(chunk)
            if digest.hexdigest() != sha256:
                raise DatasetStoreError(f"content hash {digest.hexdigest()} doesn't match the record's {sha256}")
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        log.info("Datasets store: fetched %s (%s bytes)", url, os.path.getsize(path))
        self._trim(keep=path)
        return path

    @staticmethod
    def _read(path, group, fmt=None):
        if fmt == TEXT_FORMAT:
            import h5py
            with h5py.File(path, "r") as f:
                stored = f.attrs.get("nx_format")
                if isinstance(stored, bytes):
                    stored = stored.decode("utf-8")
                if stored != fmt:
                    raise DatasetStoreError(f"the object's nx_format {stored!r} doesn't match the record's {fmt!r}")
                if "value_json" not in f:
                    raise DatasetStoreError("the object has no value_json")
                return bytes(f["value_json"][()]).decode("utf-8")
        import pandas as pd
        return pd.read_hdf(path, key=group).to_json()

    def _trim(self, keep=None):
        """ Removes the least recently used files while the cache is over its size (never <keep>, in use). """
        try:
            files = [os.path.join(self.cache_dir, f) for f in os.listdir(self.cache_dir) if f.endswith(".h5")]
            stats = sorted(((os.stat(f), f) for f in files), key=lambda s: s[0].st_mtime)
            total = sum(st.st_size for st, _ in stats)
            for st, f in stats:
                if total <= self.cache_max_bytes:
                    break
                if f == keep:
                    continue
                os.remove(f)
                total -= st.st_size
        except OSError as err:
            log.warning("Datasets store: unable to trim the cache in %s: %s", self.cache_dir, err)


def resolve_store_value(value, dataset_id=None):
    """ The value, or the stored value when it is a datasets store pointer. """
    if not isinstance(value, str):
        return value
    return DatasetStore.get().resolve(value, dataset_id=dataset_id)
