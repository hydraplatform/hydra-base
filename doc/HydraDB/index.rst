Hydra database schema
=====================

The database schema can be found :download:`here <Hydra_DB_ERD.png>`.

Hydra data
----------

Network definition
******************
These tables are referred to in the schema as 'resources'. A resource
is something to which data can be assigned, through attributes.

tProject
^^^^^^^^

A project is a high level container for networks. A project can contain
multiple networks.

 * project_id: Unique identifier for the project
 * project_name: The name of the project
 * project_description: A non-mandatory description
 * status: A character, which can be A (active) or X (deleted)
 * cr_date: Creation date
 * created_by: The user_id of the user who created the project 

tNetwork
^^^^^^^^

A network contains links and scenarios, not no nodes directly. The topology
of a network is defined through its links -- a link connects two nodes.
A network also contains scenarios, which are containers for network data.

 * project_id: THe project in which this network resides.
 * network_id: Unique identifier
 * network_name: The name of the network. This is UNIQUE within a project.
 * network_description: A non-mandatory description
 * network_layout: Layout parameters for the network.
 * status: A character, which can be A (active) or X (deleted)
 * projection: A string describing the map projection of the coordinates in the
   network.
 * cr_date: Creation date

tNode
^^^^^

Along with the standard id, name, description
and status, a Node has an X, Y coordinate.


 * node_id: Unique identifier
 * network_id: The network in which this link resides.
 * node_name: Node name. This is UNIQUE within a network.
 * node_description: non-mandatory description
 * status: A character, which can be A (active) or X (deleted)
 * node_x: The node's X-coordinate on a standard plane
 * node_y: The node's Y-coordinate on a standard plane
 * node_layout: A string describing layout parameters.
 * node_type: The name of the template to which this node belongs. For example, the 'reservoir' node_type refers to the 'reservoir' templates which may reside in multiple groups.
 * cr_date: Creation date

tLink
^^^^^

Links belong inside a network and link two nodes. Links define the topology of the network. Along with the standard id, name and status, a link has two node ids
and a network_id.

 * link_id: Unique identifier
 * network_id: The network in which this link resides.
 * status: A character, which can be A (active) or X (deleted)
 * node_1_id: Link from node 1...
 * node_2_id: ...to node 2
 * link_name: Name of the link. This is UNIQUE for links between the same 2 nodes.
 * link_layout: A string describing layout parameters. The layout includes
   intermediate points.
 * cr_date: Creation date

Attributes
**********

tAttr
^^^^^

A basic attribute definition, with just a name and dimension.
For example: A reservoir might have this attribute: Name: 'Capacity' Dimension 'Volume'

 * attr_id: Unique identifier
 * attr_name: Name (Capacity, Flow)
 * attr_dimen: Dimension of the value that will be stored against this attribute. 


tTemplate
^^^^^^^^^^^^^^^^^

A resource template defines a grouping for attributes. This allows a 'type' of
resource to be defined. For example, a simple reservoir template would
contain two attributes: Flow and Capacity (each of which were defined in tattr)

 * template_id: Unique identifier
 * template_name: Template Name ('Reservoir' for example)
 * alias: This is a non-functional string used for display purposes.
 * layout: Default display parameters for a tempalte -- colour, shape etc.
 * group_id: The group to which this template belongs (For example: "EBSD Nodes")

tTemplateGroup
^^^^^^^^^^^^^^^^^^^^^^

A grouping for resource templates. Used to categorise resource types into a single
group -- for example, the 'GAMS Nodes' Group might contain two resource templates:
'Reservoir' and 'Refinery'. This grouping should define what is required by
a GAMS plugin.

 * group_id: Unique Identifier
 * group_name: Name

tTemplateItem
^^^^^^^^^^^^^^^^^^^^^

This links attributes to their template. An attribute can be in several templates.
Both attr_id and template_id make up the PK.

 * attr_id: The attribute
 * template_id: The template that this attribute is in.

tResourceAttr
^^^^^^^^^^^^^

A 'resource' can be a Project, Network, Node, Link or Scenario.
A resource attribute is an attribute associated with a specific resource.
For example, given an attribute (attr_id = 1) a node (node_id = 100), the
resource attribute states that node 100 has attribute 1. It is through this
table that data can be associated with a resource.

 * resource_attr_id: Unique identifier
 * attr_id: The attribute being assigned to this resource
 * ref_key: The type of resource. Can be one of: ('NODE', 'LINK', 'NETWORK', 'PROJECT', 'SCENARIO')
 * ref_id: The identifer for the resource.
 * attr_is_var: Either 'Y' or 'N' -- This flag indicates whether data should be assigned to the resource attribute. If not, it is assumed this will be done by an app.

tAttrMap
^^^^^^^^

This maps two attributes, meaning they are equivalent. For example, 'Capacity' in one app might be the same as and 'Size' in another.

 * attr_id_a: Attribute a is the same as ...
 * attr_id_b: ... attribute b.

Scenarios
*********
 
tScenario
^^^^^^^^^

A scenario is a set of data associated with a network. Let's say there is a
network with some node and links, all of which have been assigned some resource attributes. A scenario is what contains the data for those resource attributes. Several scenarios
can be created per network, meaning multiple different datasets can be used on the 
same network.

 * scenario_id: Unique identifier
 * network_id: The network to which this scenario applies
 * scenario_name: The name of this scenario
 * scenario_description: Non-mandatory description
 * status: A character, which can be A (active) or X (deleted)
 * cr_date: Creation date

tResourceScenario
^^^^^^^^^^^^^^^^^

This connects a piece of data, a scenario and a resource attribute.
The data itself is not accessed directly from this table, but through 
tDataset, which stores what type the data its, its units and other information.

 * dataset_id: A reference to the scenario data table.
 * scenario_id: A reference to the scenario
 * resource_attr_id: A reference to the resource attribute.


Datasets
********

tDataset
^^^^^^^^^^^^^

Links a scenario to a single piece of data. This table references the data
in the appropriate data table using data_id. It knows which table to access
using the data_type column. Ex: data_id = 1 and data_type = 'descriptor' means
look in tDescriptor for data_id 1.

 * dataset_id: Unique identifier
 * data_id: Reference to a row in one of the data tables.
 * data_type: Defines which data table to look in. Must be one of: ('descriptor', 'timeseries', 'eqtimeseries', 'scalar', 'array')
 * data_units: What is this data type measured in?
 * data_name: A name for this data
 * data_dimen: Dimension -- for comparison with dimension in tAttr.
 * data_hash: The hash of the datum. This hash is generated using python's hash() function, as used in hash tables. Allows for easy comparison of data.
 * cr_date: Creation date

tDatasetGroup
^^^^^^^^^^^^^

Groups datasets into named sets for easy & convenient categorisation.

 * group_name: The human-readable name of the group or category
 * group_id  : Unique identifier for the group. PK.

tDatasetGroupItem
^^^^^^^^^^^^^^^^^

Keeps track of which piece of data is in which group.

 * dataset_id : refers to the piece of data in tDataset that is in the group
 * group_id   : refers to the group_id in tDatasetGroup.

tDescriptor
^^^^^^^^^^^

A string: ``"I am a value"``

 * data_id: unique identifier
 * desc_val: Value (string)

tScalar
^^^^^^^

A single numeric value: 10.5

 * data_id: Unique identifier
 * param_value: value (double)

tArray
^^^^^^

A multi-dimensional array, stored as a BLOB.

 * data_id: Unique identifier
 * arr_data: Value (BLOB)

tTimeSeries
^^^^^^^^^^^

A container for time series data.

 * data_id: Unique identifier

tTimeSeriesData
^^^^^^^^^^^^^^^

Time series data, stored as multiple time - value pairs, all associated with
a single data_id, which is contained in tTimeSeries.

 * data_id: Reference to data_id in tTimeSeries
 * ts_time: Timestamp
 * ts_value: a multi-dimensional array, stored as a blob. Can also just be a single value.

tEqTimeSeries
^^^^^^^^^^^^^

Equally spaced time series. Starting at a specified time, and with a given
frequency (measured in seconds), there is a multi-dimensional array, stored as a BLOB.

 * data_id: Unique identifier
 * start_time: timestamp
 * frequency: measured in seconds
 * arr_data: multi-dimensional array, stored as a BLOB.

tDataAttr
^^^^^^^^^

Auxiliary information about the data, in name / value pairs.

 * d_attr_id: Unique Identifier
 * dataset_id: Reference to the data about which this info is stored.
 * d_attr_name: Name of the auxiliary piece of data
 * d_attr_val: Value

Rules and constraints
*********************
Using these three tables, it is possible to generate a complex mathematical
equation. Think of the constraint as the container, the groups as parenteses (), and
items as the values within the parentheses. Constraints are logically contained within
scenarios.

tConstraint
^^^^^^^^^^^

A constraint exists within a scenario and essentially looks like this:
(some equation) = Value. There's a left-hand-side, which contains the actual
contraint information, and operation, and a value to which the left-hand-side must
be related.

    constraint_id: Unique identifer
    scenario_id: Reference to scenario
    group_id: Reference to first group (or outermost brackets) on the left hand side of the equation.
    constant: The value to which the left hand side is campared.
    op: The operation used to compare the left and right hand side.

tConstraintGroup
^^^^^^^^^^^^^^^^

A constraint group can be thought of as the inside of a pair of parentheses in
a mathematical equation. For example, in the condition (A + B) = 1, the group
is A + B, where A and B are Items, on either side of an operation.
In a more complex example: ((A + B) - C) = 1, (A + B) - C is one group containing the group (A + B) and the item C.

    group_id: Unique identifier
    constraint_id: reference to the constraint
    ref_key_1: Type of entity contained on the left hand side of this group (can be
    another group (GROUP) or an item (ITEM).
    ref_id_1: Id of either group or item (as indicated by ref_key_1)
    ref_key_2: Type of entity on right hand side of this group (can be GROUP, ITEM).
    This value can be null if there is nothing on the right hand side of the operation.
    ref_id_2:  Id of either group or item (as indicated by ref_key_2)
    op: Operation put between the two entites in this group. Can be null if the group
    contains only a single entity.


tConstraintItem
^^^^^^^^^^^^^^^

The atomic part of a constraint. This must link to a resource attribute -- and 
therefore to a piece of data. 

 * item_id: Unique identifier
 * constraint_id: Reference to constraint
 * resource_attr_id: Reference to the resource attribute, through which we can access the data used in the constraint equation.

User and permission management
******************************

These tables are not connected to the ones containing network information.

tUser
^^^^^

Save access credentials for each user

 * user_id: unique identifier
 * username: Username
 * password: Password
 * cr_date: Creation date

tRole
^^^^^
  
Define roles
  
 * role_id: Unique identifier 
 * role_name: Role name
 * role_code: Role code. Unique. Used for easier identification
 * cr_date: Creation date

tPerm
^^^^^
  
Define particular permissions

 * perm_id: Unique identifier
 * perm_name: Permission Name
 * perm_code: Permission code. Unique. Used for easier identification
 * cr_date: Creation date

tRoleUser
^^^^^^^^^
  
Assign each user to specific roles
 
 * user_id: Reference to user
 * role_id: Reference to role

tRolePerm
^^^^^^^^^
  
Assign particular permissions to a role
  
 * perm_id: Reference to permission
 * role_id: Reference to role

Ownership
******************************

These tables define what belongs to which users.

tOwner
^^^^^^^^^^^^^

Ownership of a resource

 * user_id   : User's ID
 * ref_key   : resource type identifier (currently PROJECT & NETWORK)
 * ref_id    : resource instance identifier
 * view      : Flag to indicate read permissions (read is a reserved word, hence 'view).
 * edit      : Flag to indicate write permissions (write is a reserved word, hence 'edit').
 * share     : Flag to indicate share permissions
 * cr_date   : creation date
