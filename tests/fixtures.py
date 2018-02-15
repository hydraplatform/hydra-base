from hydra_base.db import DeclarativeBase as _db
from hydra_base.util.hdb import create_default_users_and_perms, make_root_user
import hydra_base
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture()
def testdb_uri(tmpdir):
    return 'sqlite:///{}/testdb.sqlite'.format(tmpdir)


@pytest.fixture(scope='function')
def engine(testdb_uri):
    engine = create_engine(testdb_uri)
    return engine


@pytest.fixture(scope='function')
def db(engine, request):
    """ Test database """
    _db.metadata.create_all(engine)
    return _db


@pytest.fixture(scope='function')
def session(db, engine, request):
    """Creates a new database session for a test."""
    db.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    # A DBSession() instance establishes all conversations with the database
    # and represents a "staging zone" for all the objects loaded into the
    # database session object. Any change made against the objects in the
    # session won't be persisted into the database until you call
    # session.commit(). If you're not happy about the changes, you can
    # revert all of them back to the last commit by calling
    # session.rollback()
    session = DBSession()

    # Patch the global session in hydra_base
    hydra_base.db.DBSession = session

    # Now apply the default users and roles
    create_default_users_and_perms()
    make_root_user()
    return session