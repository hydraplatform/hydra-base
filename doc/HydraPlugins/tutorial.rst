How to develop a Hydra App
==========================

A quick introduction to the SOAP interface provided by the Hydra server.

General concepts
----------------

The client - server based architecture of Hydra allows very flexible plug-in
development. A plug-in is a stand-alone executable which connects to
the server. The connection to the server is established using either SOAP or JSON.
This implies that a plug-in can be written in any programming language that is
capable of implementing a client to a SOAP service. 

.. toctree::
   tutorial_json
   tutorial_soap
   tutorial_xml

For a start-to-finish walkthrough of a simple App, see :ref:`plugin-step-by-step`
