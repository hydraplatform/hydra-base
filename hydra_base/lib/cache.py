"""
    A simple cache system for storing such things as project hierarchies and templates.
    By default uses diskcache for simpler setup and backward compatibility
    unless 'memcached' is set in the 'cache' section of the
    config, in which case use that.
"""
import logging
import datetime
from hydra_base import config as hydraconfig
import tempfile

log = logging.getLogger(__name__)
global cache

def _init_diskcache():
    log.info("Using diskcache for caching.")
    global cache
    import diskcache as dc
    cache = dc.Cache(tempfile.gettempdir())

class _MemcachedWithFallback:
    """Wraps a pylibmc client and falls back to diskcache on connection errors."""

    def __init__(self, pylibmc_cache, fallback):
        self._mc = pylibmc_cache
        self._fb = fallback

    def set(self, key, value, *args, **kwargs):
        try:
            return self._mc.set(key, value, *args, **kwargs)
        except Exception as e:
            log.warning("Memcached set failed (%s), falling back to diskcache.", e)
            return self._fb.set(key, value)

    def get(self, key, *args, **kwargs):
        try:
            return self._mc.get(key, *args, **kwargs)
        except Exception as e:
            log.warning("Memcached get failed (%s), falling back to diskcache.", e)
            return self._fb.get(key)

    def delete(self, key, *args, **kwargs):
        try:
            return self._mc.delete(key, *args, **kwargs)
        except Exception as e:
            log.warning("Memcached delete failed (%s), falling back to diskcache.", e)
            return self._fb.delete(key, retry=False)

    def flush_all(self):
        try:
            self._mc.flush_all()
        except Exception as e:
            log.warning("Memcached flush_all failed (%s), falling back to diskcache.", e)
            self._fb.clear()


if hydraconfig.get('cache', 'type') != "memcached":
    _init_diskcache()

elif hydraconfig.get('cache', 'type') == 'memcached':
    try:
        import pylibmc
        import diskcache as dc

        host = hydraconfig.get('cache', 'host', '127.0.0.1')
        port = hydraconfig.get('cache', 'port', 11211)
        _mc = pylibmc.Client([f"{host}:{port}"], binary=True)

        # Check if Memcached server is reachable by setting a test key
        test_key = "__connection_test__"
        test_value = datetime.datetime.toordinal(datetime.datetime.now())
        try:
            _mc.set(test_key, test_value, 1)
            _mc.get(test_key)
            log.info("Connected to memcached server.")
        except Exception:
            raise ConnectionError("Memcached server not responding.")

        _fallback = dc.Cache(tempfile.gettempdir())
        cache = _MemcachedWithFallback(_mc, _fallback)

    except (ModuleNotFoundError, ConnectionError) as e:
        if isinstance(e, ModuleNotFoundError):
            log.warning("Unable to find pylibmc. Defaulting to diskcache.")
        else:
            log.warning("Memcached server not reachable. Defaulting to diskcache.")

        _init_diskcache()

def clear_cache():
    if hasattr(cache, 'flush_all'):
        cache.flush_all() # memcache / wrapped memcache
    else:
        cache.clear() # diskcache
