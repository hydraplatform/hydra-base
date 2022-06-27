"""
    A simple cache system for storing such things as project hierarchies and templates.
    By default uses diskcache for simpler setup and backward compatibility
    unless 'memcached' is set in the 'cache' section of the
    config, in which case use that.
"""
from hydra_base import config as hydraconfig
import logging
log = logging.getLogger(__name__)
global cache

if hydraconfig.get('cache', 'type') is None:
    import diskcache as dc
    cache = dc.Cache('tmp')
elif hydraconfig.get('cache', 'type') == 'memcached':
    try:
        import pylibmc
        cache = pylibmc.Client([hydraconfig.get('cache', 'host', '127.0.0.1')], binary=True)
    except ModuleNotFoundError:
        log.warning("Unable to find pylibmc. Defaulting to diskcache.")
        import diskcache as dc
        cache = dc.Cache('tmp')

def clear_cache():
    if hasattr(cache, 'flush_all'):
        cache.flush_all() # memcache
    else:
        cache.clear() # diskcache
