HydraPlatform
=============

![Build Status](../../workflows/CI/badge.svg)


A library for managing networks. Full documentation can be found [here](http://umwrg.github.io/HydraPlatform/).


Installation
------------

pip install hydra-base

Initialisation
--------------
The first thing you ened to do when working with Hydra, if you are using a local database is to add some default data to the database.
These include:
1. The admin user
2. Default roles and permissions
3. Default units and dimensions
4. A default network and scenario (required to perform some admin tasks)

You can do this using the [hydra client](https://github.com/hydraplatform/hydra-client-python).

```
> pip install hydra-client-python
> hydra-cli intitalise-db
```

Usage
-----

Hydra base can be used in conjunction with a [hydra server](https://github.com/hydraplatform/hydra-server).

Hydra server relies on web clients sending it requests. Usefully, there is a hydra client
libary [here](https://github.com/hydraplatform/hydra-client-python) with some example on how to use the system.

Performing a DB Upgrade
-----------------------

As Hydra is developed, its database structure is sometimes altered typically to add or modify database columns.
If you need to update your DB to the latest version, these steps descibe how to do this.

1. Install alembic: `$ pip install alembic`
2. Go to the alembic folder `$ cd /path/to/hydra-base/hydra_base/db/`
3. Modify the alembic.ini file: Change this line to point to your database 
```
sqlalchemy.url = mysql+mysqldb://root:root@localhost/hydradb
```
4. Run the upgrade: `$ alembic upgrade head`
5. This should result in an output like:
```
37569 2022-08-08 14:19:58,201 - INFO - Registering data type "SCALAR".
37569 2022-08-08 14:19:58,201 - INFO - Registering data type "ARRAY".
37569 2022-08-08 14:19:58,202 - INFO - Registering data type "DESCRIPTOR".
37569 2022-08-08 14:19:58,202 - INFO - Registering data type "DATAFRAME".
37569 2022-08-08 14:19:58,202 - INFO - Registering data type "TIMESERIES".
37569 2022-08-08 14:19:58,206 - WARNING - Unable to find pylibmc. Defaulting to diskcache.
INFO  [alembic.runtime.migration] Context impl MySQLImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.runtime.migration] Applying 04e4ae80b7b9_project_inheritance.py
```
6. If you see an error relating to there being 'multiple heads', then run `alembic upgrade heads` (notice the 'heads' instead if 'head')
7. Your DB upgrade is done. If you experience an issue, make an issue on this repository and it will be resolved
