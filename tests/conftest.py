
import pytest

def pytest_addoption(parser):
    parser.addoption("--db-backend", action="store", default="sqlite",
        help="Database backend to use when running the tests.")

@pytest.fixture
def db_backend(request):
    return request.config.getoption("--db-backend")

def pytest_report_header(config):
    headers = []
    solver_name = config.getoption("--db-backend")
    headers.append('db-backend: {}'.format(solver_name))
    return '\n'.join(headers)
