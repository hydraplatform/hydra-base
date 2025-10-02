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


if hydraconfig.get('cache', 'type') != "memcached":
    _init_diskcache()
else:
    try:
        from pymemcache.client.base import Client as MemcacheClient
        host = hydraconfig.get('cache', 'host', '127.0.0.1')
        port = hydraconfig.get('cache', 'port', 11211)
        cache = MemcacheClient((host, int(port)))
        # Check if Memcached server is reachable by setting a test key
        test_key = b"__connection_test__"
        test_value = str(datetime.datetime.toordinal(datetime.datetime.now())).encode()
        try:
            cache.set(test_key, test_value, expire=1)
            cache.get(test_key)
            log.info("Connected to memcached server.")
        except Exception:
            raise ConnectionError("Memcached server not responding.")
    except (ModuleNotFoundError, ConnectionError) as e:
        log.warning("Memcached server not reachable or pymemcache not installed. Defaulting to diskcache.")
        _init_diskcache()

def clear_cache():
    if hasattr(cache, 'flush_all'):
        cache.flush_all() # diskcache
    elif hasattr(cache, 'flush_all'): # legacy memcache
        cache.flush_all()
    elif hasattr(cache, 'flush_all_keys'):
        cache.flush_all_keys() # pymemcache
    elif hasattr(cache, 'clear'):
        cache.clear() # diskcache
