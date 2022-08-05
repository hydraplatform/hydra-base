.. _json_app_example:

Developing a app using JSON
---------------------------

Developing an app involves connecting to Hydra through its SOAP or JSON API.
Here we provide an example of simple app, which retrieves a network.
This example uses the requests python library to connect to JSON api.

Creating a client
*****************

First, the WSDL must be identified and connected to the library.
By default, the hydra server is hosted on port 8080: ``http://localhost:8080/json``

.. code-block:: python

    from HydraLib.PluginLib import JsonConnection
    url = "http://localhost:8080/json?wsdl"
    conn = JsonConnection(url)


Connect to the server
*********************
As the app connects to a remote server, a login is required so that data is protected.
For local use with one user, this can simply be read from a config file.
Once login is performed, the ``session_id`` must be stored and added to the request
header for all subsequent requests. Hydra provides a library which handles this through
the JsonConnection object. To log in, just call ``login`` like so:

.. code-block:: python
    
    login_response = conn.login('myuser', 'Pa55w0rD')

Creating a network instance
***************************
Having successfully logged in, networks can be added, accessed and manipulated.

Hydra employs following structure:
::

 Project -> 
    Networks ->
        Nodes ->
            Node Attributes
        Links ->
            Link Attributes
        Scenarios ->
            Data

This way, a project can contain multiple networks, which can in turn contain
multiple scenarios. A scenario represents one 'state' of the network.

Before a network can be created, we must first create a project
 
.. code-block:: python

    proj = dict(name = 'JSON test %s'%(datetime.datetime.now()))
    project   = conn.call('add_project', {'project':proj})

A network can now be created
.. code-block:: python

    net  = dict(
        name        = args.network_name 
        description = "A network created by the example plugin"
        project_id  = project.id #Note that the project now has an ID, after adding it
       )

Nodes can now be added to the project

.. code-block:: python

    node1 = dict(
        id = -1
        name = "Node 1",
        description = "A node representing a water resource",
        x = 10,
        y = 10,

    node2 = dict(
        node1.id = -2,
        node2.name = "Node 2",
        node2.description = "A node representing another water resource",
        node2.x = 20,
        node2.y = 20,
    )
    
    nodes = [node1, node2]

...and now we link the nodes

.. code-block:: python

    link = dict(
        name = "Link 1",
        description = "A link between two water resources",
        node_1_id = -1
        node_2_id = -2)
    links = [link]

    network['nodes'] = nodes
    network['links'] = linkd

One slight complication with linking nodes is that the
nodes do not yet have IDS. So how do the links what they are connecting? For this,
**temporary negative IDS** are used. Notice on the nodes above, they have been assigned negative IDS. These will be replaced by permenant, positive IDS once the data is inserted into hydra. *Negative IDs are only necessary if the object needs to be referred to and the referrer is not a direct descendant of the referee.*

Now the network can be created

.. code-block:: python

    network = conn.call('add_network', {'net':network})


Attributes
**********
Hydra provides the feature to assign attributes to nodes and links.
For example, data associated with a node representing a water treatment plant
might be 'capacity', 'annual energy cost' or 'daily throughput'.

To achieve this, first the attributes themselves must be defined. Once an attribute
is defined, it does not need to be defined again. It can be used throughout Hydra.
A Name and Dimension uniquely define an attribute

.. code-block:: python

    #Define the attribute details
    name      = "Capacity"
    dimension = Volume

    #Check the attribute does not already exist.
    attr = conn.call('get_attribute', ({'name':name, 'dimension':dimension})
    if attr is None:
        attr = dict(
            name  = name,
            dimen = dimension)
        attr = conn.call('get_attribute', ({'attr':attr})

Once the attribute has been defined, it can be assigned to the node.
Going back to the network creation example, a node is defined as follows

.. code-block:: python

    node2 = dict(
        id = -2,
        name = "Node 2",
        description = "A node representing another water resource",
        x = 20,
        y = 20,
    )
    
An attribute is added to this node using a ``ResourceAttr`` object.
A ``ResourceAttr`` links a resource (a network, node or link) to a network. Each has
its own id and ref_key, which indicates whether it refers to a node, link or network.
In this example, the node ``Node 2`` is being given attribute ``Capacity``

.. code-block:: python

    res_attr = dict(
        ref_key = 'NODE'
        attr_id = attr.id
        id      = -1
    )
    node.attributes. = [res_attr]

Note that a temporary negative ID is once again given to the ResourceAttr. This bears no
relation to the negative ID on the node. It will be used later to associate data
with this attribute. When the network is saved, this ID will be replaced by a permenant,
positive, ID.

Scenarios and Data
******************
Node and link attributes are not particularly useful by themselves without them
having a value. Using scenarios, attributes can have multiple values for different
purposes. For example, a network represenging a river network might have two
scenarios: ``Dry Year`` and ``Wet Year``. While the topology of the network will
not change, the attributes of many of the nodes might change. ``Daily Throughput`` of
our water treatment work will be less in a dry year compared to a wet year, for example.

In order to assign data to specific attributes, a scenario is used

.. code-block:: python

    scenario = dict(
        name        = 'Dry Year',
        description = 'Projected scenario of network in a dry year.',
        resourcescenarios = []
    )


Now data can be added

.. code-block:: python
    
    rs = dict(
        resource_attr_id = -1 #This refers to the ID given to the resource attr earlier.
    )

    dataset = dict(
        type = 'descriptor',
        name = 'Volume of water in a reservoir during a dry year',
        unit = 'ml',
        dimension = 'Volume', # THis must match the dimension of the attribute.
        hidden = 'N',
        value = {'desc_val':100000},
    )
    rs['value'] = dataset

    scenario['resourcescenarios'] = [rs]

    net['scenarios'] = [scenario]
    #add the network...
