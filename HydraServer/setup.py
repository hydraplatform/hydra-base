#This is just a work-around for a Python2.7 issue causing
#interpreter crash at exit when trying to log an info message.
try:
    import logging
    import multiprocessing
except:
    pass

import sys
py_version = sys.version_info[:2]

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

testpkgs=[
               'nose',
               'coverage',
               ]

install_requires=[
    "zope.sqlalchemy >= 0.4",
    "sqlalchemy",
    "numpy",
    "pandas",
    "bcrypt",
    "lxml",
    "mysql-connector-python",
    "suds",
    "spyne >= 2.12",
    "winpaths",
    "cherrypy",
    "python-dateutil",
    ]

setup(
    name='HydraPlatform',
    version='0.1',
    description='A data manager for networks',
    author='Stephen Knox',
    author_email='stephen.knox@manchester.ac.uk',
    url='http://umwrg.github.io/HydraPlatform',
    packages=find_packages(exclude=['ez_setup']),
    install_requires=install_requires,
    include_package_data=True,
    test_suite='nose.collector',
    tests_require=testpkgs,
    package_data={'HydraPlatform': []},
    message_extractors={'HydraPlatform': [
            ('**.py', 'python', None),
            ('templates/**.html', 'genshi', None),
            ('public/**', 'ignore', None)]},

    entry_points = {
                'setuptools.installation': [
                    'eggsecutable = server:run',
                ]
    },
    zip_safe=False
)
