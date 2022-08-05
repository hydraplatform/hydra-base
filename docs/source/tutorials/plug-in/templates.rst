.. _templates:

Working with templates
======================

Some plugins will be written to provide general functionality such as importing
and exporting networks to a certain data format. Others will be written to work
specifically with another software, which defines its network in a specific way. 

On example of this might be a GAMS model, which uses specific node & link types,
each with their own attributes individual to that model. In this case, a template
can be provided to ensure that the network in hydra will always be compatible
with this plugin.

A template defines node *types* and link *types*, including their name
and the attributes they posess. It also defines colours and even images for nodes
and links which the UI uses to display the network appropriately.

More details on templates can be found in :ref:`libraries_and_templates`

The template XSD can be found :download:`here <template.xsd>`

A simple example of a template can be found :download:`here <template_example.xml>`


