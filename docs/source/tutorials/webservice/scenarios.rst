.. _scenarios:

Working with scenarios
======================

Scenarios are the mechanism Hydra uses to assign ``Data`` to ``Resource Attributes``.

The concept is simple: A network's topology (the nodes and links and their locations)
tend not to change much, but what does change are the properties of that network.

Using scenarios, multiple permutations of a network's properties can be set without
altering its topology. The common example we use is a river network can have a 
'dry year' scenario and a 'wet year' scenario.

In both of these cases, the network stays the same, the attributes on all the nodes
and links stay the same. THe only thing that changes is the data.

A network can therefore have multiple scenarios and the consequently attributes of
resources (nodes and links) and data are not directly linked. 

Creating a scenario
*******************
Scenarios can be created as part of network creation (see :ref:`networks` for details
on how to create a network).

Scenarios are added to a network in the samee way as nodes and links

.. code-block:: python

    #Create a project
    ...
    #Define a basic network
    ...
    #Add nodes to network
    ...
    #Add links to network
    ...
    #Add groups to network
    ...

    #Now add a scenario
    network_scenarios = client.factory.create('hyd:ScenarioArray')
    scenario1 = client.factory.create('hyd:Scenario')
    scenario1.name = "Dry Year"
    scenario2.description = "Simulation of the data from a dry year in England."
    
Here we assume that a network with nodes and links has been defined. We then 
create an array of scenarios, into which we will put our 'Dry year' scenario.

The next step is to put data into the scenario and link this data to the attributes
of the nodes and links in the network. This is done using 'ResourceScenario' objects.
These are simply objects which link Datasets with ResourceAttrs.

First, we must create some data:

.. code-block:: python

    dataset = client.factory.create('hyd:Dataset')
    dataset.type = 'scalar' #This says that this dataset is a number
    dataset.name = 'Observed flow at hydro electric plant'
    dataset.unit = 'Ml day^-1' # Megalitres per day
    dataset.dimension = 'Volumetric flow rate' # This dimension MUST match that of the attribute
    dataset.value     = 1782.999

    #get the node first
    node = network.nodes.Node[0]
    #Next identify the attribute we are interested in:
    res_attr = node.attribute.ResourceAttr[0]

    resource_scenarios = client.factory.create('hyd:ResourceScenarioArray')
    resource_scenario1 = client.factory.create('hyd:ResourceScenario')
    resource_scenario1.resource_attr_id = res_attr.id
    resource_scenario1.value = dataset 

    scenario.resourcescenarios.ResourceScenario.append(resource_scenario1)

Every piece of data is contained in a 'Dataset' object. This holds not only the value
itself, but also its unit, dimension and name. In order to be assigned to a ResourceAttr,
a Dataset must have the same dimension as the Attr definition.. This stops users from setting
a 'speed' value on an attribute which is 'capacity'.
