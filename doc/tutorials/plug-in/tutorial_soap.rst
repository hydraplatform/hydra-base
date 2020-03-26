.. _soap_app_example:

Developing a app using SOAP
---------------------------

Developing an app involves connecting to Hydra through its SOAP or JSON API.
Here we provide an example of simple app, which retrieves a network.
This example uses the SUDS python library to connect. SUDS is a very easy to use
library as it allows simple connection to a WSDL. For larger datasets, this 
may not be the library to use, however.

Creating a client
*****************

First, the WSDL must be identified and connected to the library.
By default, the hydra server is hosted on port 8080: ``http://localhost:8080``
::

    url = "http://localhost:8080/soap?wsdl"
    cli = Client(url)

Using the ``cli.service`` attribute, server functions can be called. The first one we must
use is ``login``.

Connect to the server
*********************
As the app connects to a remote server, a login is required so that data is protected.
For local use with one user, this can simply be read from a config file.
Once login is performed, the ``session_id`` must be stored and added to the request
header for all subsequent requests
::
    
    login_response = cli.service.login('myuser', 'Pa55w0rD')
    token = cli.factory.create('RequestHeader')
    token.session_id = login_response.session_id

    #Now set the request header using cli.set_options:
    cli.set_options(soapheaders=token)

    #Finally, for easier usage, make a sensible namespace:
    cli.add_prefix('hyd', 'server.complexmodels')

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
::

    proj      = self.client.factory.create('hyd:Project')
    proj.name = 'SOAP test %s'%(datetime.datetime.now())
    project   = self.client.service.add_project(proj)

A network can now be created
::

    net             = self.client.factory.create('hyd:Network')
    net.name        = args.network_name 
    net.description = "A network created by the example plugin"
    net.project_id  = project.id #Note that the project now has an ID, after adding it

Nodes can now be added to the project
::

    nodes = self.client.factory.create('hyd:NodeArray')

    node1 = self.client.factory.create('hyd:Node')
    node1.id = -1
    node1.name = "Node 1",
    node1.description = "A node representing a water resource",
    node1.x = 10
    node1.y = 10
    nodes.Node.append(node1)

    node2 = self.client.factory.create('hyd:Node')
    node1.id = -2
    node2.name = "Node 2",
    node2.description = "A node representing another water resource",
    node2.x = 20
    node2.y = 20
    nodes.Node.append(node2)

...and now we link the nodes
::

    links = self.client.factory.create('hyd:LinkArray')

    node1 = self.client.factory.create('hyd:Link')
    node1.name = "Link 1",
    node1.description = "A link between two water resources",
    node1.node_1_id = -1
    node1.node_2_id = -2
    nodes.Node.append(node1)

    network.nodes = nodes

One slight complication with linking nodes is that the
nodes do not yet have IDS. So how do the links what they are connecting? For this,
**temporary negative IDS** are used. Notice on the nodes above, they have been assigned negative IDS. These will be replaced by permenant, positive IDS once the data is inserted into hydra. *Negative IDs are only necessary if the object needs to be referred to and the referrer is not a direct descendant of the referee.*

Now the network can be created
::

    network = cli.service.add_network(net)


Attributes
**********
Hydra provides the feature to assign attributes to nodes and links.
For example, data associated with a node representing a water treatment plant
might be 'capacity', 'annual energy cost' or 'daily throughput'.

To achieve this, first the attributes themselves must be defined. Once an attribute
is defined, it does not need to be defined again. It can be used throughout Hydra.
A Name and Dimension uniquely define an attribute
::

    #Define the attribute details
    name      = "Capacity"
    dimension = Volume

    #Check the attribute does not already exist.
    attr = self.client.service.get_attribute(name, dimension)
    if attr is None:
        attr = cli.factory.create('hyd:Attr')
        attr.name  = name
        attr.dimen = dimension
        attr = self.client.service.add_attribute(attr)

Once the attribute has been defined, it can be assigned to the node.
Going back to the network creation example, a node is defined as follows
::

    node2 = self.client.factory.create('hyd:Node')
    node1.id = -2
    node2.name = "Node 2",
    node2.description = "A node representing another water resource",
    node2.x = 20
    node2.y = 20
    
An attribute is added to this node using a ``ResourceAttr`` object.
A ``ResourceAttr`` links a resource (a network, node or link) to a network. Each has
its own id and ref_key, which indicates whether it refers to a node, link or network.
In this example, the node ``Node 2`` is being given attribute ``Capacity``
::

    res_attr = cli.factory.create('hyd:ResourceAttr')
    res_attr.ref_key = 'NODE'
    res_attr.attr_id = attr.id
    res_attr.id      = -1
    node.attributes.ResourceAttr.append(res_attr)

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
::

    scenario = self.client.factory.create('hyd:Scenario')
    scenario.name        = 'Dry Year'
    scenario.description = 'Projected scenario of network in a dry year.'


Now data can be added
::
    
    rs = cli.factory.create('hyd:ResourceScenario')
    rs.resource_attr_id = -1 #This refers to the ID given to the resource attr earlier.

    dataset = cli.factory.create('hyd:Dataset')
    dataset.type = 'descriptor'
    dataset.name = 'Volume of water in a reservoir during a dry year'
    dataset.unit = 'ml'
    dataset.dimension = 'Volume' # THis must match the dimension of the attribute.
    dataset.hidden = 'N'
    dataset.value = {'desc_val':100000}

    scenario.resourcescenarios.ResourceScenario.append(dataset)

    net.scenarios.Scenario.append(scenario)
    #add the network...
