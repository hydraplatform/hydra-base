Hydra Apps 
==========

Current Apps
------------

.. toctree::
   :maxdepth: 2

   importcsv
   exportcsv
   importwml



App development
---------------

General concepts
^^^^^^^^^^^^^^^^

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

Common Tasks
^^^^^^^^^^^^

.. toctree::
   :maxdepth: 2

   projects
   networks
   attributes
   datasets
   scenarios
