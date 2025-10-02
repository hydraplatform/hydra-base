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

startup_config = hydraconfig.get_startup_config()
cache_type = startup_config["hydra_cachetype"]
cache_host = startup_config["hydra_cachehost"]

def _init_diskcache():
    log.info("Using diskcache for caching.")
    global cache
    import diskcache as dc
    cache = dc.Cache(tempfile.gettempdir())

if cache_type != "memcached":
    _init_diskcache()
elif cache_type == 'memcached':
    try:
        import pylibmc
        cache = pylibmc.Client([f"{cache_host}:31211"], binary=True)

        # Check if Memcached server is reachable by setting a test key
        test_key = "__connection_test__"
        #pick a unique key based on the time
        test_value = datetime.datetime.toordinal(datetime.datetime.now())
        try:
            cache.set(test_key, test_value, 1)
            cache.get(test_key)
            log.info("Connected to memcached server.")
        except Exception:
            raise ConnectionError("Memcached server not responding.")

    except (ModuleNotFoundError, ConnectionError) as e:
        if isinstance(e, ModuleNotFoundError):
            log.warning("Unable to find pylibmc. Defaulting to diskcache.")
        else:
            log.warning("Memcached server not reachable. Defaulting to diskcache.")

        _init_diskcache()

def clear_cache():
    if hasattr(cache, 'flush_all'):
        cache.flush_all() # memcache
    else:
        cache.clear() # diskcache
