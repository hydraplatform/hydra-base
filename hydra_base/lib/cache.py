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

if hydraconfig.get('cache', 'type') != "memcached":
    log.info("Using diskcache for caching.")
    import diskcache as dc
    cache = dc.Cache(tempfile.gettempdir())
elif hydraconfig.get('cache', 'type') == 'memcached':
    log.info("Using memcached for caching.")
    try:
        import pylibmc
        host = hydraconfig.get('cache', 'host', '127.0.0.1')
        port = hydraconfig.get('cache', 'port', 31211)
        cache = pylibmc.Client([f"{host}:{port}"], binary=True)
        log.info(f"Memcached client initialized with host: {host}:{port}")
    except ModuleNotFoundError:
        log.warning("Unable to find pylibmc. Defaulting to diskcache.")
        import diskcache as dc
        cache = dc.Cache(tempfile.gettempdir())
    except Exception as e:
        log.error(f"Error initializing memcached: {e}.  Defaulting to diskcache.")
        import diskcache as dc
        cache = dc.Cache(tempfile.gettempdir())

def clear_cache():
    if hasattr(cache, 'flush_all'):
        cache.flush_all() # memcache
    else:
        cache.clear() # diskcache
