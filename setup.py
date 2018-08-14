#This is just a work-around for a Python2.7 issue causing
#interpreter crash at exit when trying to log an info message.
try:
    import logging
    import multiprocessing
except:
    pass

import platform
import sys
import os

py_version = sys.version_info[:2]

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

testpkgs=[
               'pytest',
               'coverage',
               ]

install_requires=[
    "sqlalchemy",
    "psycopg2",
    "zope.sqlalchemy >= 0.4",
    "pandas",
    "numpy",
    "bcrypt",
    "lxml",
    "mysqlclient",
    "python-dateutil",
    "cheroot",
    "beaker",
    "packaging",
    ]

# get version string from __init__.py
with open(os.path.join(os.path.dirname(__file__), "hydra_base", "__init__.py")) as f:
    for line in f:
        if line.startswith("__version__"):
            version = line.split("=")[1].strip().strip("\"'")

if platform.system() == "Windows":  # only add winpaths when platform is Windows so that setup.py is universal
    install_requires.append("winpaths")

setup(
    name='hydra-base',
    version=version,
    description='A data manager for networks',
    author='Stephen Knox',
    author_email='stephen.knox@manchester.ac.uk',
    url='https://github.com/hydraplatform/hydra-base',
    packages=find_packages(exclude=['ez_setup']),
    install_requires=install_requires,
    include_package_data=True,
    tests_require=testpkgs,
    package_data={'hydra-base': []},
    message_extractors={'hydra-base': [
            ('**.py', 'python', None),
            ('templates/**.html', 'genshi', None),
            ('public/**', 'ignore', None)]},

    entry_points = {
                'setuptools.installation': [
                    'eggsecutable = server:run',
                ]
    },
    zip_safe=False,
)
