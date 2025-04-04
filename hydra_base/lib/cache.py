"""
    A simple cache system for storing such things as project hierarchies and templates.
    By default uses diskcache for simpler setup and backward compatibility
    unless 'memcached' is set in the 'cache' section of the
    config, in which case use that.
"""
import logging

from hydra_base import config as hydraconfig
import tempfile

log = logging.getLogger(__name__)
global cache

startup_config = hydraconfig.get_startup_config()
cache_type = startup_config["hydra_cachetype"]
cache_host = startup_config["hydra_cachehost"]

if cache_type != "memcached":
    import diskcache as dc
    cache = dc.Cache(tempfile.gettempdir())
elif cache_type == 'memcached':
    try:
        import pylibmc
        cache = pylibmc.Client([cache_host], binary=True)
    except ModuleNotFoundError:
        log.warning("Unable to find pylibmc. Defaulting to diskcache.")
        import diskcache as dc
        cache = dc.Cache(tempfile.gettempdir())


def clear_cache():
    if hasattr(cache, 'flush_all'):
        cache.flush_all() # memcache
    else:
        cache.clear() # diskcache
