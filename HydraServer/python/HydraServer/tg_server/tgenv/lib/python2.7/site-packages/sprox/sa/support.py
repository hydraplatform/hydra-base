import inspect

try:
    from sqlalchemy.orm import PropertyLoader
except ImportError:
    # Compatibility with SQLA0.9
    from sqlalchemy.orm import RelationshipProperty as PropertyLoader

try:
    from sqlalchemy.ext.declarative.clsregistry import _class_resolver
except ImportError as e:  # pragma: no cover
    # Compatibility with SQLA < 0.9
    _class_resolver = None

try:
    # Not available in some SQLA versions
    from sqlalchemy.types import LargeBinary
except ImportError:  # pragma: no cover
    class LargeBinary:
        pass

try:
    # Future proof as it will probably be removed as deprecated
    from sqlalchemy.types import Binary
except ImportError:  # pragma: no cover
    class Binary(LargeBinary):
        pass


def resolve_entity(entity):
    if inspect.isfunction(entity):
        entity = entity()
    if _class_resolver is not None and isinstance(entity, _class_resolver):
        entity = entity()
    return entity

