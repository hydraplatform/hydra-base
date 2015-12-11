.. _xml_example:

App XML
*******
Each app must be accompanied by an XML file defining its basic information and 
its inputs. This XML file can be used by a UI or another plugin to help format
inputs.

The <plugin_info> tag is the base.
Next, ``<plugin_name>``, ``<plugin_description>`` and ``<plugin_dir>`` describe the name
of the plugin to display, a description of what it does, and the location of the
actual executable file, if necessary.
For example

.. code-block:: xml 

    <plugin_info>
        <plugin_name>Test Plugin</plugin_name>
        <plugin_dir>TestPlugin/trunk/TestPlugin.py</plugin_dir>
        <plugin_description>Check for the existance of a network in hydra
                Written by Stephen Knox stephen.knox--at--manchester.ac.uk
                (c) Copyright 2013, University College London.
        </plugin_description>
        <plugin_epilog>For more information visit www.hydraplatform.com</plugin_epilog>
    ...
    </plugin_info>

Immediately after this, the app's inputs are defined.

There are three category of input: ``<mandatory_args>``, ``<non_mandatory_args>`` and ``<switches>``

.. code-block:: xml 

    <mandatory_args>
        <arg>
            <name>network_name</name>
            <switch>-t</switch>
            <multiple>N</multiple>
            <argtype>string</argtype>
            <help>The name of the network you are creating</help>
        </arg>
    </mandatory_args>
    <non_mandatory_args>
        <arg>
            <name>num_nodes</name>
            <switch>-n</switch>
            <multiple>N</multiple>
            <argtype>integer</argtype>
            <help>The number of nodes to create in the network</help>
        </arg>
        <arg>
            <name>scenario_name</name>
            <switch>-s</switch>
            <multiple>N</multiple>
            <argtype>string</argtype>
            <help>The name of the scenario to create. If none is specified, a default is used.</help>
        </arg>
    </non_mandatory_args> 
    <switches>
        <arg>
            <switch>-d</switch>
            <name>include-data</name>
            <help>If you want data in your network, use this switch.</help>
        </arg>
    </switches>

Within each category there is an ``<arg>``, inside which is defined a ``<name>``, command line ``<switch>``, whether ``<multiple>`` inputs
can be expected, what type input can be expected and a help string to describe
what it is.
