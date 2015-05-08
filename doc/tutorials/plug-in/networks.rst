.. _networks:

Working with networks
=====================

Adding networks
---------------


Before a network can be added, a project must first be created. See :ref:`projects`.


Networks are made up of nodes, links and scenarios, and can be created with
any number of each.

We use the word ``resource`` to describe things in hydra that can have attributes (see :ref:`attributes`). Resources include
Projects, Networks, Nodes, Links and Groups. A group is a grouping of nodes and/or links within a network. Groups are used to categorise sub-sections of a network, for example political jurisdictions.

The simplest way to get started is to add an 'empty' network

.. code-block:: python

 #Assume we have created a project
 project_id      = my_project.id

 net             = self.client.factory.create('hyd:Network')

 #necessary info:
 net.name        = "An empty network" 
 net.description = "A network with no nodes, links or data (scenarios)"
 net.project_id  = project.id

A network also has a 'layout' attribute, which can be any XML encoding. This
is used to help apps (the user interface) to store app-specific information
about the network

.. code-block:: python

 #Some helpful stuff for the UI, encoded in XML
 net.layout = {'background_colour': 'blue'}
    
Having defined the network, it is added

.. code-block:: python

 #add network
 empty_net = client.service.add_network(net)

 #The new project now has an ID
 print empty_net.id

 print empty_net.nodes      # None
 print empty_net.links      # None
 print empty_net.scenarios  # None
 print empty_net.attributes # None

Retrieving Networks
-------------------
Existing networks can be retrieved in a few ways:
The simplest and most common is by ID

.. code-block:: python

 my_network = client.service.get_network(my_network_id)

If you don't know the ID, but know the name, you can still find the network, but
you must know the project ID

.. code-block:: python

 my_network = client.service.get_network_by_name(my_project_id, "An empty network")

Failing that, just ask for all the networks a given project

.. code-block:: python

 my_networks = client.service.get_networks(project_id)

Deleting Networks
-----------------
If a project is being clogged up with too many networks, the networks can
be deleted

.. code-block:: python

 client.service.delete_network(network_id)

however, this will not permenantly delete the project. It will still
be retrievable using ``get_network`` but it will not be returned in ``get_networks``. A network can be reactivated by calling

.. code-block:: python

 client.service.activate_network(network_id)

To delete a network peremenantly, use

.. code-block:: python

 client.service.purge_network(network_id)

**Warning** This cannot be undone and all sub-nodes, links, scenarios and attributes will also be purged. This may also include the deletion of data (see :ref:`datasets`).

Sharing Networks
----------------
It is not uncommon for multiple people to work on the same network, so to facilitate this, Hydra allows users to share networks with other users

.. code-block:: python

 #connect...
 my_user_id      = 1
 friend_1_user_id = 2
 friend_2_user_id = 3
 my_network_id      = 999

 client.service.share_network(my_network_id, [friend_1_user_id, friend_2_user_id], 'Y', 'N')

In this example, user 1 is sharing network 999 with two other users. The first 'Y' parameter indicates that the network is 'read only', while the second indicates that the users are not allowed to re-share the network with other users. 

Nodes, Links and Groups
-----------------------
Nodes
*****
Nodes and links are the fundamental structure of a network. As mentioned, they
are *resources*, so they can have attributes.

A node is defined like this

.. code-block:: python

 node1 = self.client.factory.create('hyd:Node')
 node1.name = "Node 1",
 node1.description = "A node representing a water resource",
 node1.x = 10
 node1.y = 10

 node2 = self.client.factory.create('hyd:Node')
 node2.name = "Node 2",
 node2.description = "A node representing another water resource",
 node2.x = 20
 node2.y = 20


A common approach is to add nodes to a new network before creating it.
From our previous examples:

.. code-block:: python

 net             = self.client.factory.create('hyd:Network')
 
 #...create node1 and node2...
 nodes = self.client.factory.create('hyd:NodeArray')
 nodes.Node.append(node1)
 nodes.Node.append(node2)
 
 net.nodes = nodes

 net_with_nodes = client.service.add_network(net)

Nodes and links can be added to a network in two ways:
# Adding the node directly

.. code-block:: python

 network_id = 999
 my_new_node = client.service.add_node(network_id, node1)
 print my_new_node.id #will give a newly created ID

# Retrieving the network, adding the node, then updating the network.
As the server must process the entire incoming network to see what has changed, this should only be done if multiple changes are being made at the same time.

.. code-block:: python

 my_network = client.service.get_network(999)
 my_network.nodes.Node.append(node1)
 client.service.update_network(my_network)

Links
*****
Links connect nodes and can be added to a network in much the same way as nodes.
The only complication in adding links is that the linking is done based on node ID, so nodes have to exist in the network before they can be added.
Using the nodes we defined earlier:

.. code-block:: python

 net             = self.client.factory.create('hyd:Network')
 #...define network information... 

 #...create node1 and node2...
 nodes = self.client.factory.create('hyd:NodeArray')
 nodes.Node.append(node1)
 nodes.Node.append(node2)
 
 net.nodes = nodes
 
 #Add the network which gives IDs to each of the nodes.
 net_with_nodes = client.service.add_network(net)
 
 #Get the node IDS
 node_1_id = net_with_nodes.nodes.Node[0].id
 node_2_id = net_with_nodes.nodes.Node[1].id

 #Link the nodes
 link1 = self.client.factory.create('hyd:Link')
 link1.name = "Link 1",
 link1.description = "A link between two water resources",
 link1.node_1_id = node_1_id 
 link1.node_2_id = node_2_id
 links.Link.append(link1)
 net.links = links
 
 net_with_nodes_and_links = client.service.update_network(net)

This is clearly not a good solution, as we have to send the network to 
the server twice. To get around this, Hydra provides a mechanism whereby
links can be defined on nodes even if they don't yet exist on the server yet.
TO achieve this, we use **negative IDS**

.. code-block:: python

 net = self.client.factory.create('hyd:Network')
 #...define network information... 
 
 #...create node1 and node2...
 nodes = self.client.factory.create('hyd:NodeArray')
 
 node1.id = -1
 node2.id = -2

 nodes.Node.append(node1)
 nodes.Node.append(node2)
  
 net.nodes = nodes
 
 #Link the nodes
 link1 = self.client.factory.create('hyd:Link')
 link1.name = "Link 1",
 link1.description = "A link between two water resources",
 link1.node_1_id = -1
 link1.node_2_id = -2
 links.Link.append(link1)
 net.links = links

 net_with_nodes_and_links = client.service.add_network(net)

The add_network function will recognise that the IDS on the nodes
and links are negative, generate positive IDS and link everything correctly. This
approach also works when adding nodes & links to an existing network. Just use
negative IDS to refer to other resources locally and let the server sort out
the IDS when ``update_network`` is called. 

Groups
******
A ``group``, or ``resource group`` is not a structural part of a network. Instead
it allows nodes, links to be grouped together into high-level containers.
One example might be where a network representing a river might contain several
political juristidictions, where different rules may apply.

The contents of a group are called ``items`` or ``resource group items``. Group
items are defined on a scenario-by-scenario basis. See :ref:`scenarios`.

Groups are still resources, however, and act in much the same way as nodes and links.

Adding a group to a new network:

.. code-block:: python

 #...define network with nodes and links...

 groups            = self.client.factory.create('hyd:ResourceGroupArray')
 group             = self.client.factory.create('hyd:ResourceGroup')
 group.id          = -1 
 group.name        = "Test Group"
 group.description = "Test group description"
 groups.ResourceGroup.append(group)
 net.resourcegroups = groups

 net_with_nodes_links_groups = client.service.add_network(net)

Notice that this group has a negative ID. This is set so the group items know which group they are in. ALso notice that this uses '-1' even though this ID is also used on the node ID we set earlier. This doesn't matter so long as there is consistency between nodes and between groups.

Groups can also be added using ``add_resourcegroup``

.. code-block:: python

 #...define network with nodes and links...
 net_with_nodes_and_links = client.service.add_network(net)

 group             = self.client.factory.create('hyd:ResourceGroup')
 group.id          = -1 
 group.name        = "Test Group"
 group.description = "Test group description"

 new_group = client.service.add_resourcegroup(net.id, group)

Or update the network

.. code-block:: python

 #...define network with nodes and links...
 net_with_nodes_and_links = client.service.add_network(net)

 group             = self.client.factory.create('hyd:ResourceGroup')
 group.id          = -1 
 group.name        = "Test Group"
 group.description = "Test group description"

 net.resourcegroups.ResourceGroup.append(group)

 net_with_nodes_links_groups = client.service.update_network(net) 

Deletion of a group is done using ``delete_resourcegroup``

.. code-block:: python

 group_id = 123
 self.client.service.delete_resourcegroup(group_id)

and activate it again...

.. code-block:: python

 group_id = 123
 self.client.service.activate_resourcegroup(group_id)

and finally purge it

.. code-block:: python

 group_id = 123
 self.client.service.purge_resourcegroup(group_id)



