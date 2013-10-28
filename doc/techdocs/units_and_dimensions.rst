Handling of units and dimensions
================================

The Hydra server implements checks that make sure that the units of a dataset
assigned to an attribute are consistent with the physical dimension asked for by
the attribute. This requires some conventions about how physical dimensions are
denoted in the respective fields of the database. Also, a standard way of
describing physical units needs to be defined. This document describes these
conventions and provides a controlled vocabulary for both, dimensions and units.

.. contents:: Table of Contents
   :local:

Definitions
-----------

**Dimension**
    In this document a dimension is the physical dimension of a physical
    quantity. A dimension is independent of the units used to describe a
    physical quantity.

**Unit**
    A unit defines the magnitude of a physical quantity. A unit is defined by
    convention and refers to a system of measurement, such as `SI
    <http://en.wikipedia.org/wiki/International_System_of_Units>`_.

Dimensions
----------

Basic concepts
~~~~~~~~~~~~~~

There are two basic ways of defining physical dimensions. 

#. Define a dimension as mathematical expression based on the seven fundamental
   quantities:

   ======================= ===============
   Base quantity           Symbol
   ----------------------- ---------------
   *Length*                :math:`L`
   *Mass*                  :math:`M`
   *Time*                  :math:`T`
   *Electric current*      :math:`I`
   *Temperature*           :math:`\Theta`
   *Amount of substance*   :math:`N`
   *Luminous intensity*    :math:`J`
   ======================= ===============

   All derived quantities can be expressed based on these fundamental
   quantities. For example *Energy* would be written as :math:`M L^{2} T^{-2}`.

#. Define a dimension using a keyword. This will allow to set fundamental and
   derived quantities using a name defined by a controlled vocabulary. 

In Hydra the second definition is implemented since expressing all the derived
quantities based on the fundamental ones is rather complicated, even for
quantities that are used frequently (such as energy, power, etc.).

List of dimension keywords
~~~~~~~~~~~~~~~~~~~~~~~~~~

======================== ======================================================
Keyword                  Description
------------------------ ------------------------------------------------------
``Length``
``Mass``
``Time``
``Temperature``
------------------------ ------------------------------------------------------
``Area``
``Volume``
``Angle``
``Speed``
``Energy``
``Force``
``Power``
``Pressure``
``Volumetric flow rate``
======================== ======================================================


Units
-----

Basic concepts
~~~~~~~~~~~~~~

List of unit keywords
~~~~~~~~~~~~~~~~~~~~~
