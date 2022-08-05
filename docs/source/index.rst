.. HYDRA documentation master file, created by
   sphinx-quickstart on Tue Jul 23 15:31:40 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. |br| raw:: html

   <br />


Welcome to Hydra Platform
========================

.. toctree::
   :hidden:

   tutorials/index
   plugins/index
   Design <design/index>
   Implementation <implementation/index>
   Web API <webapi/index>
   devdocs/index

Hydra Platform is an open-source model platform for network based data
management. It facilitates the development of complex resource network models by
providing a consistent storage facility for network topology and associated
datasets. Hydra Platform is built around a server that exposes all functionality
as a web service to which Apps can connect to access data.

.. sidebar:: Learn more

    :doc:`design/index`
        Hydra Platform is acting as a server providing a web service to which
        Apps connect.

    :doc:`tutorials/index` 
        This chapter will get you up and running with Hydra Platform. It
        explains the installation process, basic usage of the web service and
        how to build your own App and template.

    :doc:`implementation/index`
        Details about the technical implementation are described in this
        chapter.

    :doc:`webapi/index`
        Full description of the web service API.

    :doc:`devdocs/index`
        Documentation for Hydra Platform developers.


Apps
----

Hydra Platform provides a set of selected Apps for importing and exporting
datasets. Currently network topology and data can be exported to and imported
from CSV files (:ref:`exportcsv` and :ref:`importcsv`). We also provide an App
to :ref:`importwml`. To get started building an app, see our tutorial on building 
a simple app (:doc:`here <tutorials/plug-in/index>`).

User Interfaces
---------------

`Hydra Modeller <http://hydramodeller.com>`_, built by ch2m, provides a desktop user interface tailored to support the full functionality of Hydra Platform. In future other web-based or desktop user-interfaces may be built.

Development team
----------------

Stephen Knox is a computer scientist and currently the main developer of
Hydra Platform at the University of Manchester. 

`Philipp Meier
<http://www.eawag.ch/de/ueberuns/portraet/organisation/mitarbeitende/profile/philipp-meier/show/>`_
currently works as a postdoc at the Department of Surface Waters Research and
Management at `Eawag <http://www.eawag.ch>`_.

Khaled Mohamed has working on Hydra Platform for 18 months, primarily building Apps and related models.

References
----------

Stephen Knox, Philipp Meier, Khaled Mohammed, Brett Korteling, Evgenii Matrosov,
Anthony Hurford, Ivana Huskova, Julien Harou, David Rosenberg, Amaury Thilmant, Josue Medellin-Azuara, Jon Wicks:
An open-source software platform for data management, visualisation, model building and model sharing in water, energy and other resource modelling domains. American Geophysical Union (AGU) 2015, San Francisco, USA; 12/2015 `[GO] <https://agu.confex.com/agu/fm15/meetingapp.cgi/Paper/78165>`_

Stephen Knox, Philipp Meier, and Julien J. Harou: Web service and plug-in
architecture for flexibility and openness of environmental data sharing
platforms. 7th Intl. Congress on Env. Modelling and Software, San Diego,
California, USA; 06/2014 `[PDF] <http://www.iemss.org/sites/iemss2014/papers/iemss2014_submission_211.pdf>`_

Philipp Meier, Stephen Knox, and Julien J. Harou: Linking water resource network
models to an open data management platform. 7th Intl. Congress on Env. Modelling
and Software, San Diego, California, USA; 06/2014 `[PDF] <http://www.iemss.org/sites/iemss2014/papers/iemss2014_submission_276.pdf>`_
