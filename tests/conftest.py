import pytest

import tempfile
import datetime
import sqlite3
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import hydra_base
from hydra_base.db import DeclarativeBase as _db
from hydra_base.util.hdb import create_default_users_and_perms, make_root_user,\
                                create_default_units_and_dimensions
from hydra_base.util import testing
from hydra_client.connection import JSONConnection, RemoteJSONConnection
from hydra_base.lib.cache import clear_cache


no_externaldb_opt = "--no-externaldb"
externaldb_mark = "externaldb"
requires_hdf_mark = "requires_hdf"

def pytest_addoption(parser):
    parser.addoption("--db-backend", action="store", default="sqlite",
                     help="Database backend to use when running the tests.")
    parser.addoption("--connection-type", action="store", default="local",
                     help="Remote or Local Connection")
    parser.addoption(no_externaldb_opt, action="store_true", default=False,
                     help="Do not run tests which require an external storage database")

def pytest_configure(config):
    config.addinivalue_line("markers", f"{externaldb_mark}: Tests external storage")
    config.addinivalue_line("markers", f"{requires_hdf_mark}: Indicates that decorated test requires HDF support")

def pytest_collection_modifyitems(config, items):
    """
    When the `no_externaldb_opt` is present, add a skip mark to every
    test marked with `externaldb_mark`

    When config has disabled HDF support, add a skip mark to every test
    marked with `requires_hdf_mark`
    """
    if config.getoption(no_externaldb_opt):
        externaldb_skip = pytest.mark.skip(reason=f"{no_externaldb_opt} selected")
        for item in items:
            if externaldb_mark in item.keywords:
                item.add_marker(externaldb_skip)

    conf_disabled = hydra_base.config.CONFIG.get("storage_hdf", "disable_hdf").lower()
    hdf_disabled = True if conf_disabled in ("true", "yes") else False
    hdf_skip = pytest.mark.skip(reason="Test not applicable when HDF support disabled")
    if hdf_disabled:
        for item in items:
            if requires_hdf_mark in item.keywords:
                item.add_marker(hdf_skip)


@pytest.fixture(scope="session")
def db_backend(request):
    return request.config.getoption("--db-backend")

@pytest.fixture(scope="session")
def connection_type(request):
    return request.config.getoption("--connection-type")

def pytest_report_header(config):
    headers = []
    db_string = config.getoption("--db-backend")
    headers.append('db-backend: {}'.format(db_string))
    connection_type = config.getoption("--connection-type")
    headers.append('connection-type: {}'.format(connection_type))
    return '\n'.join(headers)


"""
    Test Fixtures
"""
#def pytest_namespace():
#    return {'root_user_id': 1}

@pytest.fixture()
def dateformat():
    return hydra_base.config.get('DEFAULT', 'datetime_format', "%Y-%m-%dT%H:%M:%S.%f000Z")

@pytest.fixture(scope='module')
def testdb_uri(db_backend):
    if db_backend == 'sqlite':
        tmp = tempfile.gettempdir()
        # Use a :memory: database for the tests.
        millis = int(round(time.time() * 1000))
        return f'sqlite:///{tmp}/test_db_{millis}.db'
    elif db_backend == 'postgres':
        # This is designed to work on Travis CI
        return 'postgresql://postgres@localhost:5432/hydra_base_test'
    elif db_backend == 'mysql':
        return 'mysql+mysqldb://root:root@localhost/hydra_base_test'
    else:
        raise ValueError('Database backend "{}" not supported when running the tests.'.format(db_backend))

@pytest.fixture(scope='module')
def client(connection_type, testdb_uri):
    print("GETTING CLIENT")
    if connection_type == 'local':
        client = JSONConnection(app_name='Hydra Local Test Suite', db_url=testdb_uri)
        #fake a login using the test's session
        client.user_id = 1
        client.connect()
    else:

        from spyne.server.null import NullServer
        from hydra_server import initialize_api_server

        hydra_server = initialize_api_server(testdb_uri, test=True)

        null_server = NullServer(hydra_server.json_application, ostr=True)

        #The url argument here is to avoid the connection complaining.
        #It's not actually used, as we're using a null (testing) server
        client = RemoteJSONConnection(url='localhost:8080/json',
                                      app_name='Hydra Remote Test Suite',
                                      test_server=null_server)
        client.login('root', '')

    client.testutils = testing.TestUtil(client)
    pytest.root_user_id = 1
    pytest.user_a = client.testutils.create_user("UserA")
    pytest.user_b = client.testutils.create_user("UserB")
    pytest.user_c = client.testutils.create_user("UserC", role='developer')
    pytest.user_d = client.testutils.create_user("UserD", role='developer')
    yield client
    #???
    hydra_base.lib.template.clear_cache()
    hydra_base.db.close_session()

    clear_cache() # clear the user project cache
    try:
        drop_tables(testdb_uri)
    except Exception as err:
        print("Error dropping DB")

def drop_tables(db_url):
    """
        Drop all the tables in the specified DB
    """
    from hydra_base.db import DeclarativeBase, engine
    DeclarativeBase.metadata.drop_all(engine)

@pytest.fixture()
def network(client, project_id=None, num_nodes=10, new_proj=True, map_projection='EPSG:4326'):
    return client.testutils.build_network(project_id, num_nodes, new_proj, map_projection)

@pytest.fixture()
def network_with_data(client, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
    return client.testutils.create_network_with_data(project_id, num_nodes, ret_full_net, new_proj, map_projection)

@pytest.fixture()
def network_with_child_scenario(client, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
    return client.testutils.create_network_with_child_scenario(project_id, num_nodes, ret_full_net, new_proj, map_projection, levels=2)

@pytest.fixture()
def network_with_grandchild_scenario(client, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
    """
    Creates a network with 3 scenarios -- a baseline, a child, and a child of the child.
    """
    return client.testutils.create_network_with_child_scenario(project_id, num_nodes, ret_full_net, new_proj, map_projection, levels=3)

@pytest.fixture()
def second_network_with_data(client, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
    """
        Creates a second project with a new network and returns the network created
    """
    return client.testutils.create_network_with_data(project_id, num_nodes, ret_full_net, new_proj, map_projection)

@pytest.fixture()
def network_with_extra_group(client, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
    return client.testutils.create_network_with_extra_group(project_id, num_nodes, ret_full_net, new_proj, map_projection)


@pytest.fixture()
def networkmaker(client):
    class NetworkMaker:
        def create(self, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
            return client.testutils.create_network_with_data(project_id, num_nodes, ret_full_net, new_proj, map_projection)
    return NetworkMaker()

@pytest.fixture()
def template(client):
    return client.testutils.create_template()

@pytest.fixture()
def attributes(client):
    return client.testutils.create_attributes()

@pytest.fixture()
def attribute(client):
    return client.testutils.create_attribute()


@pytest.fixture()
def projectmaker(client):
    class ProjectMaker:
        def create(self, name=None, share=True, parent_id=None):
            if name is None:
                name = 'Project %s' % (datetime.datetime.now())
            return client.testutils.create_project(name=name, share=share, parent_id=parent_id)

    return ProjectMaker()


@pytest.fixture()
def attributegroup(client):
    project = client.testutils.create_project('Project %s' % (datetime.datetime.now()))
    newgroup = client.testutils.create_attributegroup(project.id)
    return newgroup

@pytest.fixture()
def attributegroupmaker(client):
    class AttributeGroupMaker:
        def create(self, project_id, name=None, exclusive='N'):

            if name is None:
                name = 'Attribute Group %s' % (datetime.datetime.now())

            newgroup = client.testutils.create_attributegroup(project_id,
                                                  name=name,
                                                  exclusive=exclusive)

            return newgroup

    return AttributeGroupMaker()

@pytest.fixture()
def new_dataset(client):
    return client.testutils.create_dataset()
