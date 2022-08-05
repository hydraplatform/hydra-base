.. _attributes:

Attributes
==========
As well as having a topological structure (nodes connected by links), a network
can also have associated information. For example, in a river network, a node
representing a hydro electric plant might have information such as its annual
cost of running measured in pounds sterling, maximum capacity measured in megawatts, 
and daily throughput of water measured in megalitres per second.

An *Attr* in hydra is the definition of an attribute which can be assigned
to a resource.
Attributes are then linked to resources (nodes or links) using ``ResourceAttributes``. 
ResourceAttributes can then be linked to a specific value through a scenario (see :ref:`scenarios`).

An attribute is uniquely identified by its name and dimension.

Before resources can be given attributes, the Attributes must be defined.

.. code-block:: python

    attr = client.factory.create('hyd:Attr')
    attr.name = "Daily Throughput"
    attr.dimension = "Volumetric flow rate"

    daily_throughput = client.service.add_attr(attr)
    print daily_throughput.id #Will print an ID

All the valid dimensions are valid :download:'here ../../../HydraLib/trunk/HydraLib/static/unit_definitions.xml`.

Resource Attributes
===================
Should a nodei (or lots of nodes) in your network need a 'Daily Throughput', then first the attribute needs
to be defined (see above). Next, the node(s) needs to be linked to the attribute.

This can be done when creating the network for the first time or retrospectively.
This example shows Daily Throughput from above being assigned to a node before the network
is added:

.. code-block:: python

    #Create the project
    ...
    #Define the network
    ...
    #Now define the network nodes.
    node1 = self.client.factory.create('hyd:Node')
    node1.name = "Node 1",
    node1.description = "A node representing a water resource",
    node1.x = 10
    node1.y = 10

    node1_throughput = client.factory.create('hyd:ResourceAttr')
    node1_throughput.attr_id = daily_throughput.id
    node1_throughput.ref_key = 'NODE'
    node1_throughput.ref_id  = None
    node1_throughput.attr_is_var = 'N'
    node1_throughput.id = -1
    node1.attributes.ResourceAttr.append(node1_throughput)

    #Add more nodes
    ...
    #Add some links
    ...
    #Create the network
    net_with_nodes_and_attributes = client.service.add_network(net)

There are a few interesting things to investigate here. For example, what is
`ref key` and why do we need to define it?

.. code-block:: python

    node1_throughput.ref_key = 'NODE'

A ResourceAttr is a general object which can be applied to several different
types of resource: A network, a node, a link, a group and even a project.
In order to keep track of which resource this ResourceAttr belongs to, a ref_key is
used. This is particularly interesting later, where many attributes can be loaded
from the server at once, so knowing which one points where is necessary.

Next, what is `attr_is_var`?

.. code-block:: python

    node1_throughpyt.attr_is_var = 'N'

This indicates whether data should be assigned to this resource attr or not. If
set to 'N', then we would expect data to be assigned to it in a scenario. If it is
set to 'Y', then it is expected that a model or some external entity should fill in 
this data.

Let's take an example:
If your work involves finding the model which optimises cost from several proposals
for a water company. First you would create a network and add lots of attributes to the nodes, including 'cost'.
All attributes except 'cost' would then be filled with data for your model to work with, but 'cost'
is left blank. The network can then be exported and run inside your cost saving model.
At this stage, the 'cost' attribute is calculated in the app and these attributes
can be filled in.

Finally, why is the id -1?

.. code-block:: python

    node1_throughput.id = -1

**Negative IDs** are the way Hydra deals with objects referencing each other but
whihout yet having permenant IDS.
When data is assigned to this ResourceAttr through a scenario, the scenario
must identify the ResourceAttr somehow. By giving the resourceAttr a temporary
negative ID, referencing becomes possible.
When the network is added, all ResourceAttrs will be given permenant, positive IDS.

