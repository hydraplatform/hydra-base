App development
===============

General concepts
----------------

The client - server based architecture of Hydra allows very flexible plug-in
development. A plug-in is a stand-alone executable which connects to
the server. The connection to the server is established using either SOAP or JSON.
This implies that a plug-in can be written in any programming language that is
capable of implementing a client to a SOAP service. 

.. toctree::
   :maxdepth: 2

   tutorial_json
   tutorial_soap
   tutorial_xml


.. _plugin-step-by-step:

Developing an app in Python
---------------------------
 
.. sidebar:: Concepts introduced in this tutorial are:

    .. toctree::
       :maxdepth: 2
       :numbered:
       
       setup
       imports
       connecting
       export
       parsing_arguments
       run
       documentation
       templates
       integration
   
This tutorial shows how a simple Hydra app is developed in Python. It takes you
step-by-step through a plugin which retrieves a network from Hydra and saves it
to a file.  This is a simple example plugin, but it is 'real' and performs a
very useful task. It allows network data to be easily retrieved, edited and
shared in a common format.

Hydra apps connect to Hydra Platform through a HTTP interface, using `JSON
<http://www.json.org/>`_ to encode the data.  Developing a app in any other
programming language that provides HTTP libraries should be similar to the code
presented here.

It is assumed that the reader is familiar with Python or any other programming
language.
