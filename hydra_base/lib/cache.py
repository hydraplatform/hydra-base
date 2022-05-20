"""
    A simple cache system for storing such things as project hierarchies and templates.
    By default uses diskcache for simpler setup and backward compatibility
    unless 'memcached' is set in the 'cache' section of the
    config, in which case use that.
"""
from hydra_base import config as hydraconfig

global cache

if hydraconfig.get('cache', 'type') is None:
    import diskcache as dc
    cache = dc.Cache('tmp')
elif hydraconfig.get('cache', 'type') == 'memcached':
    import pylibmc
    cache = pylibmc.Client([hydraconfig.get('cache', 'host', '127.0.0.1')], binary=True)

def clear_cache():
    cache.clear()
