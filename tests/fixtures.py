from hydra_base.db import DeclarativeBase as _db
from hydra_base.util.hdb import create_default_users_and_perms, make_root_user
import hydra_base
import util
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture()
def testdb_uri(db_backend):
    if db_backend == 'sqlite':
        # Use a :memory: database for the tests.
        return 'sqlite://'
    elif db_backend == 'postgres':
        # This is designed to work on Travis CI
        return 'postgresql://postgres@localhost:5432/hydra_base_test'
    elif db_backend == 'mysql':
        return 'mysql+mysqlconnector://root@localhost/hydra_base_test'
    else:
        raise ValueError('Database backend "{}" not supported when running the tests.'.format(db_backend))


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

    DBSession = sessionmaker(bind=engine, autoflush=False, autocommit=False)
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

    # Add some users
    util.create_user("UserA")
    util.create_user("UserB")
    util.create_user("UserC")

    yield session

    # Tear down the session

    # First make sure everything can be and is committed.
    session.commit()
    # Finally drop all the tables.
    hydra_base.db.DeclarativeBase.metadata.drop_all()


@pytest.fixture()
def network(project_id=None, num_nodes=10, new_proj=True, map_projection='EPSG:4326'):
    return util.build_network(project_id, num_nodes, new_proj, map_projection)


@pytest.fixture()
def network_with_data(project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326',
                      use_existing_template=True):
    return util.create_network_with_data(project_id, num_nodes, ret_full_net, new_proj, map_projection,
                                         use_existing_template=use_existing_template)


@pytest.fixture()
def network_with_data_new_template(project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326',
                      use_existing_template=False):
    return util.create_network_with_data(project_id, num_nodes, ret_full_net, new_proj, map_projection,
                                         use_existing_template=use_existing_template)
