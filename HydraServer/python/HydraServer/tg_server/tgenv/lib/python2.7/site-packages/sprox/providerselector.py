"""
Provider Locator Module

a module to help dbsprockets automatically find providers

Copyright (c) 2008 Christopher Perkins
Original Version by Christopher Perkins 2007
Released under MIT license.
"""
import inspect

try:
    from sqlalchemy import MetaData
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import _mapper_registry, class_mapper
    from sqlalchemy.orm.session import Session
    from sqlalchemy.orm.scoping import ScopedSession
except ImportError: # pragma: no cover
    pass

try: #pragma:no cover
    from sqlalchemy.orm.instrumentation import ClassManager
except ImportError: #pragma:no cover
    try: # pragma: no cover
        #sa 0.6- support
        from sqlalchemy.orm.attributes import ClassManager
    except ImportError:
        pass

SAORMProvider = None
try:
    from sprox.sa.provider import SAORMProvider
except ImportError: # pragma: no cover
    pass
#MongoKitProvider = None
#try:
#    from sprox.mk.provider import MongoKitProvider
#except ImportError: # pragma: no cover
#    pass
MingProvider = None
MappedClass = None
try:
    from sprox.mg.provider import MingProvider
    try:
        from ming.odm.declarative import MappedClass
    except ImportError: #pragma: no cover
        from ming.orm.declarative import MappedClass

except ImportError:   # pragma: no cover
    pass

from sprox.dummyentity import DummyEntity

class ProviderSelector:
    def __init__(self):
        self._identifiers = {}
        self._entities = {}

    def get_entity(self, name, **hints):
        raise NotImplementedError

    def get_identifier(self, entity, **hints):
        raise NotImplementedError

    def get_provider(self, entity, **hints):
        raise NotImplementedError

#class _MongoKitSelector(ProviderSelector):
#    def get_identifier(self, entity, **hints):
#        return entity.__name__

#    def get_provider(self, entity=None, hint=None, **hints):
#        #TODO cache
#        return MongoKitProvider(None)

class _MingSelector(ProviderSelector):
    #def get_identifier(self, entity, **hints):
    #    return entity.__name__

    def get_provider(self, entity=None, hint=None, **hints):
        #TODO cache
        return MingProvider(entity.__mongometa__.session)


class _SAORMSelector(ProviderSelector):

    def __init__(self):
        self._providers = {}

    def _get_engine(self, hint, hints):
        metadata = hints.get('metadata', None)
        engine   = hints.get('engine', None)
        session  = hints.get('session', None)

        if isinstance(hint, Engine):
            engine=hint

        if isinstance(hint, MetaData):
            metadata=hint

        if isinstance(hint, (Session, ScopedSession)):
            session = hint

        if session is not None and engine is None:
            engine = session.bind

        if metadata is not None and engine is None:
            engine = metadata.bind

        return engine

    def get_entity(self, identifier, hint=None, **hints):
        engine = self._get_engine(hint, hints)
        for mapper in _mapper_registry:
            if mapper.class_.__name__ == identifier:
                if engine is None:
                    return mapper.class_
                if engine is not None and mapper.tables[0].bind == engine:
                    return mapper.class_

        raise KeyError('could not find model by the name %s in %s'%(model_name, metadata))

    def get_identifier(self, entity, **hints):
        return entity.__name__

    def get_provider(self, entity=None, hint=None, **hints):
        """
        :Arguments:

        Entity
          Mapped class to find a provider for

        hint/hints
          variables sent in to the provider to give more information about
          how the provider connects to the database.

        Get a provider related to the entity.  (They should share the same engine)
        The provider's are cached as not to waste computation/memory.

        :Usage:

        >>> from sprox.providerselector import SAORMSelector
        >>> provider = SAORMSelector.get_provider(User, session=session)
        >>> str(provider.engine.url.drivername)
        'sqlite'
        """

        if entity is None and isinstance(hint, Engine):
            engine = hint
            if engine not in self._providers:
                self._providers[engine] = SAORMProvider(hint, **hints)
            return self._providers[engine]

        if hint is None and entity is not None:
            mapper = class_mapper(entity)
            hint = mapper.tables[0].bind
        engine = self._get_engine(hint, hints)
        if engine not in self._providers:
            if hint is None and len(hints) == 0:
                hint = engine
            self._providers[engine] = SAORMProvider(hint, **hints)
        return self._providers[engine]

SAORMSelector = _SAORMSelector()
#MongoKitSelector = _MongoKitSelector()
MingSelector = _MingSelector()

#XXX:
#StormSelector = _StormSelector()
#SOSelector    = _SOSelector()

class ProviderTypeSelectorError(Exception):pass

class ProviderTypeSelector(object):

    def get_selector(self, entity=None, **hints):
        #use a SA Helper
        if hasattr(entity, '_sa_class_manager') and isinstance(entity._sa_class_manager, ClassManager):
            return SAORMSelector
        elif inspect.isclass(entity) and issubclass(entity, DummyEntity):
            return SAORMSelector
        #elif hasattr(entity, '_use_pylons') or hasattr(entity,'_enable_autoref'):
            #xxx: find a better marker
        #    return MongoKitSelector
        elif inspect.isclass(entity) and MappedClass is not None and issubclass(entity, MappedClass):
            return MingSelector
        #other helper definitions are going in here
        else:
            raise ProviderTypeSelectorError('Entity %s has no known provider mapping.'%entity)

