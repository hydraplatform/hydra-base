.. HYDRA documentation master file, created by
   sphinx-quickstart on Tue Jul 23 15:31:40 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. |br| raw:: html

   <br />


Welcome to HydraPlatform
========================

HydraPlatform is an open-source model platform for network based data
management. It facilitates the development of complex resource network models by
providing a consistent storage facility for network topology and associated
datasets. HydraPlatform is build around a server that exposes all functionality
as a web service to which Apps can connect to interact with network data.

`HydraModeller <http://hydramodeller.com>`_ is user interface tailored to support the full functionality of HydraPlatform.

.. admonition:: Learn more
    :class: note

    |br|

    :doc:`design/index`
        HydraPlatform is acting as a server providing a web service to which
        Apps connect.

    :doc:`tutorials/index` 
        This chapter should get you up and running with HydraPlatform. It
        explains the installation process, basic usage of the web service and
        how to build your own App and template.
    
    :doc:`implementation/index`
        Details about the technical implementation are described in this
        chapter.

    :doc:`webapi/index`
        Full description of the web service API.

    :doc:`devdocs/index`
        Documentation for HydraPlatform developers.



Development team
----------------

Stephen Knox
    some description.

Philipp Meier
    some description.

References
----------

Stephen Knox, Philipp Meier, and Julien J. Harou: Web service and plug-in
architecture for flexibility and openness of environmental data sharing
platforms. 7th Intl. Congress on Env. Modelling and Software, San Diego,
California, USA; 06/2014 `[PDF] <http://www.iemss.org/sites/iemss2014/papers/iemss2014_submission_211.pdf>`_

Philipp Meier, Stephen Knox, and Julien J. Harou: Linking water resource network
models to an open data management platform. 7th Intl. Congress on Env. Modelling
and Software, San Diego, California, USA; 06/2014 `[PDF] <http://www.iemss.org/sites/iemss2014/papers/iemss2014_submission_276.pdf>`_

Main packages
-------------

.. toctree::
   :maxdepth: 3

   HydraDB/index
   HydraUI/index
   HydraServer/index
   HydraLib/index

Hydra plug-ins
--------------

.. toctree::
   :maxdepth: 2

   HydraPlugins/index

Tutorials
---------

.. toctree::
   :maxdepth: 1

   tutorials/plug-in/simple_plugin_in_python

Technical documents
-------------------

.. toctree::
   :maxdepth: 1

   techdocs/units_and_dimensions
   techdocs/timeseries
   techdocs/libraries_and_templates
   techdocs/EBSD_constraints

Tests
-----

.. toctree::
   :maxdepth: 1

   HydraServer/unittests


Miscellaneous
-------------

Server maintenance
~~~~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   devdocs/server

______________________________________________________________________________________

**Indices and tables**

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

