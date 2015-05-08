Connecting to Hydra Platform
============================
As the app connects to a remote server, a login is required so that data is protected.
For local use with one user, this can simply be read from a config file.
Once login is performed, the ``session_id`` must be stored and added to the request
header for all subsequent requests

.. code-block:: python
    
    login_response = cli.service.login('myuser', 'Pa55w0rD')
    token = cli.factory.create('RequestHeader')
    token.session_id = login_response.session_id

    #Now set the request header using cli.set_options:
    cli.set_options(soapheaders=token)

    #Finally, for easier usage, make a sensible namespace:
    cli.add_prefix('hyd', 'soap_server.hydra_complexmodels')


