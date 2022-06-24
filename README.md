HydraPlatform
=============

![Build Status](https://github.com/pmslavin/hydra-base/workflows/CI/badge.svg)


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

