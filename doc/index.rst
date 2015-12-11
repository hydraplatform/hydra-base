.. HYDRA documentation master file, created by
   sphinx-quickstart on Tue Jul 23 15:31:40 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. |br| raw:: html

   <br />


Welcome to HydraPlatform
========================

.. toctree::
   :hidden:

   HydraPlugins/index
   Design <design/index>
   tutorials/index
   Implementation <implementation/index>
   Web API <webapi/index>
   devdocs/index

HydraPlatform is an open-source model platform for network based data
management. It facilitates the development of complex resource network models by
providing a consistent storage facility for network topology and associated
datasets. HydraPlatform is build around a server that exposes all functionality
as a web service to which Apps can connect to interact with network data.

`HydraModeller <http://hydramodeller.com>`_ provides a user interface tailored
to support the full functionality of HydraPlatform.

.. sidebar:: Learn more

    :doc:`design/index`
        HydraPlatform is acting as a server providing a web service to which
        Apps connect.

    :doc:`tutorials/index` 
        This chapter will get you up and running with HydraPlatform. It
        explains the installation process, basic usage of the web service and
        how to build your own App and template.

    :doc:`implementation/index`
        Details about the technical implementation are described in this
        chapter.

    :doc:`webapi/index`
        Full description of the web service API.

    :doc:`devdocs/index`
        Documentation for HydraPlatform developers.


Apps
----

HydraPlatform provides a set of selected Apps for importing and exporting
datasets. Currently network topology and data can be exported to and imported
from CSV files (:ref:`exportcsv` and :ref:`importcsv`). We also provide an App
to :doc:`HydraPlugins/importwml`.

Development team
----------------

Stephen Knox is a computer scientist and currently the main developer of
HydraPlatform at the University of Manchester. 

`Philipp Meier
<http://www.eawag.ch/de/ueberuns/portraet/organisation/mitarbeitende/profile/philipp-meier/show/>`_
currently works as a PostDoc at the Department of Surface Waters Research and
Management at `Eawag <http://www.eawag.ch>`_. He is involved in developing
HydraPlatform from the very beginning in early 2013.


References
----------

Stephen Knox, Philipp Meier, and Julien J. Harou: Web service and plug-in
architecture for flexibility and openness of environmental data sharing
platforms. 7th Intl. Congress on Env. Modelling and Software, San Diego,
California, USA; 06/2014 `[PDF] <http://www.iemss.org/sites/iemss2014/papers/iemss2014_submission_211.pdf>`_

Philipp Meier, Stephen Knox, and Julien J. Harou: Linking water resource network
models to an open data management platform. 7th Intl. Congress on Env. Modelling
and Software, San Diego, California, USA; 06/2014 `[PDF] <http://www.iemss.org/sites/iemss2014/papers/iemss2014_submission_276.pdf>`_
