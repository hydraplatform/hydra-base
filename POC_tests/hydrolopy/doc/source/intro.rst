.. _introduction:

************
Introduction
************

This collection of scripts provides basic functionality for the handling of
hydrology or other time series data. It also features a class which provides a
consistent handling of reservoirs, including hydrological, hydraulic and
economical properties.

This package still under heavy development. Many submodules are far from being
complete. However the structure as it is allows to flexibly expand the package
functionality.


Structure
=========

This package is split up into the following submodules:

========================= =====================================================
Module                      
========================= =====================================================
:py:mod:`hydrolopy.TS`    Basic functionality for time series
:py:mod:`hydrolopy.data`  Data import and export facilities
:py:mod:`hydrolopy.evap`  Evaporation functions based on **evaplib**
                          developed by Maarten J. Waterloo  
                          <maarten.waterloo@falw.vu.nl>
:py:mod:`hydrolopy.util`  Utility functions, which can help dealing with
                          different data types or with data interpolation.
:py:mod:`hydrolopy.stat`  Statistical functions
:py:mod:`hydrolopy.optim` Optimization facilities providing functions for
                          Linear Programming (LP), Non-Linear Programming 
                          (NLP) and Stochastic Dynamic Programming (SDP).
:py:mod:`hydrolopy.model` Model interfaces and a collection of simple
                          conceptual models.
:py:mod:`hydrolopy.assim` Tools for data assimilation.
========================= =====================================================

.. _download-and-install:

Download and install
====================

In short
--------

The hydrolopy package can be downloaded using **subversion**::

   mkdir hydrolopy
   svn co http://ec2-23-20-90-63.compute-1.amazonaws.com/svn-http/hydrolopy hydrolopy

To keep your copy of hydrolopy up-to-date type ``svn update`` once in a while.

More in detail
--------------

#. In order to make hydrolopy available for a specific python version (for the
   moment version ``2.7`` is recommended), create the ``hydrolopy`` folder in your
   personal *site-package* folder::

      cd ~
      mkdir -p .local/lib/python2.7/site-packages/hydrolopy

   Then of course you have to checkout the newest version into this directory::

      svn co http://ec2-23-20-90-63.compute-1.amazonaws.com/svn-http/hydrolopy .local/lib/python2.7/site-packages/hydrolopy

   Make sure that your local site-packages folder is found by python. If you
   cannot access hydrology package from python add the ``site-packages`` folder
   to the ``PYTHONPATH`` variable (see below).

#. If you wish to make hydrolopy available for all python versions running on
   your computer I recommend you to create a folder for python packages in your
   ``home`` directory::

      mkdir  ~/python-packages

   Copy ``hydrolopy`` to this directory::
      
      cp -R hydrolopy ~/python-packages

   To tell python to look for packages in this directory, you have to specify
   the ``PYTHONPATH`` variable. It can be permanetly definde by adding it to
   your ``.bashrc`` file::

      echo "export PYTHONPATH=~/python-packages" >> ~/.bashrc

   Hydrolopy or parts of it can now be made available within python scripts
   using ``import``. 
   
   **Example**::

      import hydrolopy.TS as TS

      a_date = TS.TSdate((2013, 1, 10))


Development
===========

This package is currently developed and maintained by Philipp Meier
<philipp@diemeiers.ch>. If you wish to contribute to this project, drop me a
line.
