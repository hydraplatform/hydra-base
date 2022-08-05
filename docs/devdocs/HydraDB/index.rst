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

 * project_id: The project in which this network resides.
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

 * id: Unique identifier
 * network_id: The network in which this link resides.
 * name: Node name. This is UNIQUE within a network.
 * description: non-mandatory description
 * status: A character, which can be A (active) or X (deleted)
 * x: The node's X-coordinate on a standard plane
 * y: The node's Y-coordinate on a standard plane
 * layout: A string describing layout parameters.
 * type: The name of the template to which this node belongs. For example, the 'reservoir' type refers to the 'reservoir' templates which may reside in multiple groups.
 * cr_date: Creation date

tLink
^^^^^

Links belong inside a network and link two nodes. Links define the topology of the network. Along with the standard id, name and status, a link has two node ids
and a network_id.

 * id: Unique identifier
 * network_id: The network in which this link resides.
 * status: A character, which can be A (active) or X (deleted)
 * node_1_id: Link from node 1...
 * node_2_id: ...to node 2
 * name: Name of the link. This is UNIQUE for links between the same 2 nodes.
 * description: Description of the link..
 * layout: A string describing layout parameters. The layout includes
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
 * cr_date: Creation date


tType
^^^^^^^^^^^^^^^^^

A resource type defines a grouping for attributes. This allows a 'type' of
resource to be defined. For example, a simple reservoir template would
contain two attributes: Flow and Capacity (each of which were defined in tattr)

 * type_id: Unique identifier
 * type_name: Template Name ('Reservoir' for example)
 * alias: This is a non-functional string used for display purposes.
 * layout: Default display parameters for a type -- colour, shape etc.
 * template_id: The template to which this type belongs (For example: "EBSD Nodes")

tTemplate
^^^^^^^^^^^^^^^^^^^^^^

A grouping for resource types. Used to categorise resource types into a single
group -- for example, the 'GAMS Nodes' Group might contain two resource templates:
'Reservoir' and 'Refinery'. This grouping should define what is required by
a GAMS plugin.

 * template_id: Unique Identifier
 * template_name: Name
 * layout: Default display parameters for a template -- colour, shape etc.

tTypeattr
^^^^^^^^^^^^^^^^^^^^^

This links attributes to their template. An attribute can be in several templates.
Both attr_id and template_id make up the PK.

 * attr_id: The attribute
 * type_id: The type that this attribute is in.
 * default_dataset_id: Id of a dataset which can be used as a default.
 * attr_is_var: Flag to indicate whether, in this type, the attribute is a variable
 * data_type:   The expected data type for the attribute in this type
 * data_dimension: The expeted dimension of the data
 * data_restriction: A python dictionary, which looks something like:{'NUMPLACES': '1', 'LESSTHAN': '10'}

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
 * network_id: The identifer for the network (can only be not-null if ref_key is 'NETWORK').
 * project_id: The identifer for the project (can only be not-null if ref_key is 'PROJECT').
 * node_id: The identifer for the node.      (can only be not-null if ref_key is 'NODE')
 * link_id: The identifer for the link.      (can only be not-null if ref_key is 'LINK')
 * group_id: The identifer for the resource group. (can only be not-null if ref_key is 'GROUP')
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
 * scenario_layout: Used to store layout information for the UI
 * start_time: Scenario start time (required for some models)
 * end_time: Scenario end time
 * time_step: Scenario time step
 * locked: Flag to indicate whether the scenario is editable
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
 * source: An varchar describing which app this dataset came from. 


Datasets
********

tDataset
^^^^^^^^^^^^^

Links a scenario to a single piece of data. This table references the data
in the appropriate data table using data_id. It knows which table to access
using the data_type column. Ex: data_id = 1 and data_type = 'descriptor' means
look in tDescriptor for data_id 1.

 * id: Unique identifier
 * type: Defines which data table to look in. Must be one of: ('descriptor', 'timeseries', 'eqtimeseries', 'scalar', 'array')
 * unit: What is this data type measured in?
 * name: A name for this data
 * hash: The hash of the datum. This hash is generated using python's hash() function, as used in hash tables. Allows for easy comparison of data.
 * hidden: Flag to indicate whether this dataset has been hidden by its owner.
 * value: Contains the actual value. This will usually be a single value or a JSON string.
 * cr_date: Creation date

tDatasetCollection
^^^^^^^^^^^^^^^^^^

Collections datasets into named sets for easy & convenient categorisation.

 * name: The human-readable name of the collection or category
 * id  : Unique identifier for the collection. PK.

tDatasetCollectionItem
^^^^^^^^^^^^^^^^^^^^^^

Keeps track of which piece of data is in which collection.

 * dataset_id : refers to the piece of data in tDataset that is in the collection
 * collection_id   : refers to the collection_id in tDatasetCollection.

tTimeSeriesData
^^^^^^^^^^^^^^^

Time series data, stored as multiple time - value pairs, all associated with
a single data_id, which is contained in tTimeSeries.

 * dataset_id: Reference to data_id in tTimeSeries
 * ts_time: Timestamp
 * ts_value: a multi-dimensional array, stored as a blob. Can also just be a single value.

tMetaData
^^^^^^^^^

Auxiliary information about the data, in name / value pairs.

 * dataset_id: Reference to the data about which this info is stored.
 * metadata_name: Name of the auxiliary piece of data
 * metadata_val: Value

User and permission management
******************************

These tables are not connected to the ones containing network information.

tUser
^^^^^

Save access credentials for each user

 * id: unique identifier
 * username: Username
 * password: Password
 * cr_date: Creation date

tRole
^^^^^
  
Define roles
  
 * id: Unique identifier 
 * name: Role name
 * code: Role code. Unique. Used for easier identification
 * cr_date: Creation date

tPerm
^^^^^
  
Define particular permissions

 * id: Unique identifier
 * name: Permission Name
 * code: Permission code. Unique. Used for easier identification
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

tProjectOwner
^^^^^^^^^^^^^

Ownership of a resource

 * user_id   : User's ID
 * project_id   : Reference to the project
 * view      : Flag to indicate read permissions (read is a reserved word, hence 'view).
 * edit      : Flag to indicate write permissions (write is a reserved word, hence 'edit').
 * share     : Flag to indicate share permissions
 * cr_date   : creation date

tNetworkOwner
^^^^^^^^^^^^^

Ownership of a resource

 * user_id   : User's ID
 * network_id    : Reference to the Network.
 * view      : Flag to indicate read permissions (read is a reserved word, hence 'view).
 * edit      : Flag to indicate write permissions (write is a reserved word, hence 'edit').
 * share     : Flag to indicate share permissions
 * cr_date   : creation date

tDatasetOwner
^^^^^^^^^^^^^

Ownership of a resource

 * user_id   : User's ID
 * dataset_id    : Reference to the dataset
 * view      : Flag to indicate read permissions (read is a reserved word, hence 'view).
 * edit      : Flag to indicate write permissions (write is a reserved word, hence 'edit').
 * share     : Flag to indicate share permissions
 * cr_date   : creation date
