EBSD constraints
================

This document describes how constraints of an EBSD model can be generated from a
Hydra dataset using groups. All code examples show how the different constraints
are implemented in the EBSD GAMS code. The concepts shown here will be used for
the development of a Hydra-EBSD plug-in. 

General remarks
---------------

It is assumed here that EBSD will have a template that defines one or multiple
groups that are used in each constraint type.

.. _environmental-demand:

Environmental demand
--------------------


Implementation in GAMS:
    Environmental demands are demands on a node or on a link. They are
    implemented as a normal group (named ``envDEM``) summarising all nodes or
    links where environmental demands are necessary. Currently they are
    proportional to total supply from a source to a user. The values are chosen
    on a 'scenario' basis (Note: a scenario in EBSD is not the same as a
    scenario in Hydra, in EBSD a scenario refers to dry, wet and normal years). 

Implementation in Hydra:
    Nodes or links where an environmental demand is active, need to be
    summarised in one group. The group needs one attribute (the fraction of
    supply which is re-routed to the environment).


.. _water-treatment-works-constraints:

Water treatment works constraints
---------------------------------

.. _negative-d0:

Negative DO
-----------

Implementation in GAMS:
    At the moment there is one equation for each node. Equations need to be
    hard-coded in the GAMS code.

.. _capacity-constraints:

Capacity constraints
--------------------

Implementation in GAMS:
    The set of existing capacity constraints is defined using the ``capSET``
    variable::
        
        capSET /
        CAP14001
        CAP14002
        /

    There are two groups for each resource type (node and link). A resource can
    either belong to a maximum capacity constraint or a minimum capacity
    constraint. Since equation of this constraint is of the form::

        sum(Supply[max_constr_nodes]) + sum(Flow[max_constr_links]) 
        <= 
        sum(Supply[min_constr_nodes]) + sum(Flow[min_constr_links])
        + constr_flow[flow_scenario],

    the groups are named after their position in the equation (left and right)::

        capSleft(capSET,i) /
        CAP14001 . Node1
        CAP14001 . Node2
        /

        capLleft(capSET,i,j) /
        CAP14002 . Node1 . Node2
        /

        capSright(capSET,i) /
        CAP14002 . Node1
        /

        capLright(capSET,i,j) /
        cap14001 . Node2 . Node3
        /

    Data for flow constraints is stored as follows::

        flow_scenario /
        FlowDYAA
        FlowDYCP
        FlowMDO
        FlowNYAA
        /

        Table constr_flow(capSET,flow_scenario) /
                  FlowDYAA FlowDYCP FlowMDO FlowNYAA
        CAP14001  50       60       60      15
        CAP14002  30       30       15      30



.. note::
    From the EBSD code at hand, the implementation of these constraints is not
    entirely clear. There is a constraint group (``CAP14010``) that translates
    to the following equation::

        0 <= sum(Supply[CAP14010]) + sum(Flow[CAP14010]) + 110

    This is equivalent to a negative minimum supply, unless we have a negative
    flow in some links belonging to the group CAP14010 (that doesn't seem right).


Implementation in Hydra:
    Implementing these constraints in Hydra might be a bit tricky. 

    The general form of a capacity constraint is::

           "Nodes subject to maximum constraint" 
        <= "Nodes subject to minimum constraint" 
         + "Constant value"

    Basically each resource has to be grouped by constraint it belongs to and by
    position in the equation. There two possible positions for each resource
    type, on the left hand side of the equation and on the right hand side.
    Since Hydra knows the type of each resource, nodes and links can be mixed in
    one group. Also one node or link can be part of multiple constraints.

    This means we need to group the resources subject to a capacity constraint
    in a group that determines the constraint and a group that determines the
    position. This can only be achieved with hierarchical groups:

        +------------------------------------------------+
        | **Group:** ``Example capacity constraint``     |
        +------------------------------------------------+
        | **Attributes:**                                |
        +----------------------+-------------------------+
        | ``Constant value``:  | Number                  |
        +----------------------+-------------------------+
        | **Members:**                                   |
        +------------------------------------------------+
        | ``left`` *(group)*                             |
        +------------------------------------------------+
        | ``right`` *(group)*                            |
        +------------------------------------------------+


        +--------------------------------+
        | **Group:** ``left``            |
        +--------------------------------+
        | **Members:**                   |
        +--------------------------------+
        | *Nodes on the left hand side*  |
        +--------------------------------+
        | *Links on the left hand side*  |
        +--------------------------------+

        +--------------------------------+
        | **Group:** ``right``           |
        +--------------------------------+
        | **Members:**                   |
        +--------------------------------+
        | *Nodes on the right hand side* |
        +--------------------------------+
        | *Links on the right hand side* |
        +--------------------------------+

    This means that there will be three groups for every capacity constraint.

.. _ratchet-constraints:

Ratchet constraints
-------------------

Implementation in GAMS:
    Ratchet constraints act on one single node or link only.  Unfortunately
    there are no ratchet constraints in the EBSD code which was available at the
    time of writing of this document. But it can be inferred from the code that
    a flag determines, whether a ratchet constraint is active on a node or not.
    The flag is called ``SO_Flg_RC`` for options and ``SE_Flg_RC`` for existing
    nodes. 

Implementation in Hydra:
    Since a ratchet constraint only acts on one single node, a simple attribute
    to a node or a link is sufficient. Attribute names are ``SO_Flg_RC`` and
    ``SO_Flg_RC``, respectively.


.. _start-date-constraints:

Start date constraints
----------------------

I have found no trace of such a constraint in the EBSD code. It will certainly
act on one node or link only. It should therefore be possible to assign a value
to an attribute of the node or link subject to this constraint.

.. _continuity-constraints:

Continuity constraints
----------------------

This is a constraint hard-coded in the EBSD code.

.. _mutually-exclusive:

Mutually exclusive constraints
------------------------------

Implementation in GAMS: 
    In the GAMS code three sets are needed. One defines the groups within which
    nodes or links are mutually exclusive::

        mutset  / 
           MUT0001
           MUT0002
        /

    Two sets define which nodes and links belong to which group::

        NmutexclSet(mutset,i) "Assign nodes to group" /
            MUT00001 . Node1
            MUT00001 . Node2
        /

        LmutexclSet(mutset,i,j) "Assign links to group" /
            MUT0001 . Node1 . Node 2
        /

Implementation in Hydra:
    Nodes and links need to be assigned to a group of the type ``Mutually
    exclusive constraint``. This group does not need any attributes.

.. _prerequisite-constraints:

Prerequisite constraints (AND / OR / Lag time)
----------------------------------------------

Implementation in GAMS:
    In GAMS, prerequisite constraints depend on the definition of five different
    sets. The implementation of AND and OR prerequisite constraints are
    equivalent.
   
    Definition of all constraints::

        prersetAND /
        prer0001
        prer0002
        /

    Nodes on the left and the right hand side of the equation::

        NprerSETleftAND (prersetAND,i) /
        prer0001.Node1
        prer0002.Node2
        /

        NprerSETrightAND (prersetAND,i) /
        prer0001.Node3
        prer0001.Node4
        /


    Links on the left and the right hand side of the equation::

        LprerSETleftAND (prersetAND,i,j) /
        prer001.Node1.Node3
        /

        LprerSETrightAND (prersetAND,i,j) /
        prer001.Node3.Node4
        /

    This grouping is equivalent to the grouping for capacity constraints.

Implementation in Hydra:
    The structure of groups needed for prerequisite constraints is equivalent
    to the structure needed for :ref:`capacity-constraints`. Each constraint uses a
    constraint group and two sub-groups. The constraint group has an attribute
    ``lagyear`` and. The two sub-groups determine whether a resource (node or
    link) is on the left or the right hand side of the equation.
    
.. _mutually-dependent-constraints:

Mutually dependent constraints
------------------------------

See :ref:`mutually-exclusive`.

.. _flow-constraints:

Flow constraints
----------------

See :ref:`capacity-constraints`.

.. _demand-management-constraints:

Demand management constraints
-----------------------------

Implemenation in GAMS:
    There is one group (``DMLR``) that defines which nodes are subject to demand
    management constraints. There are time series associated with each single
    node.

Implementation in Hydra:
    This constraint needs one single group without attributes. The timeseries
    attribute needed are attached to each node.
