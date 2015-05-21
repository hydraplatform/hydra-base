.. _integrating:

Integrating the plug-in into Hydra Modeller
===========================================

Turning an app into a hydra modeller plugin requires creating an exe
of your code (see :ref:`creating_an_exe`), putting it into a pre-defined
folder structure (:see: `folder_structure`) along with a plugin xml.

.. _plugin_xml:

Plugin XML
----------

Hydra Modeller uses a plugin XML file to describe the required inputs to the
plugin as well as the icon it will use when displaying the button in the user
interface.

The main information in the XML file is as follows:
 - The plugin name
 - The plugin location (the location of the exe)
 - The plugin description
 - The epilog
 - Icons (large (32x32px) and small (16x16 px))
 - Mandatory arguments
 - Non-Mandatory arguments
 - Switches

For each argument, there are the following details. They should be familiar from
the arguments we defined in the plugin:

 - The name that will be displayed in the UI (human readable)
 - The switch (in our plugin, these would be '-n' and '-s')
 - The argtype (string, integer etc. There are a few wildcards such as 'network', 'scenario' and 'template' which allow the UI to provide a drop-down rather than asking the
   user to remember the ID of the network they are working on!
 - The help is a string of text that is displayed when the user  clicks on the argument in the UI

The Export JSON file is below in full. Notice that there is 1 switch defined.
This is here purely to show what a switch would look like and does not have
any reference to the code we have looked at until now.

.. code-block:: xml

 <plugin_info>
     <plugin_name>Export JSON</plugin_name>
     <plugin_dir>ExportCSV.exe</plugin_dir>
     <plugin_description>Export a network saved in Hydra to a JSON file.</plugin_description>
     <plugin_epilog>
         Written by Stephen Knox stephen.knox@manchester.ac.uk
         (c) Copyright 20135, University of Manchester.
         For more information visit www.hydra-network.com
     </plugin_epilog>
     <smallicon>icon16.png</smallicon>
     <largeicon>icon32.png</largeicon>
     <mandatory_args>
         <arg>
             <name>Network ID</name>
             <switch>-n</switch>
             <multiple>N</multiple>
             <argtype>network</argtype>
             <help>The ID of the network to be exported</help>
         </arg>
     </mandatory_args>
    <non_mandatory_args>
        <arg>
            <name>Scenario ID</name>
            <switch>-s</switch>
            <multiple>N</multiple>
            <argtype>scenario</argtype>
            <help>The ID of the scenario to be exported. If no
                  scenario is specified, all scenarios in the network will be
                  exported.
             </help>
         </arg>
        <arg>
            <name>Target Dir</name>
            <switch>-d</switch>
            <multiple>N</multiple>
            <argtype>string</argtype>
            <help>The directory / folder you wish the file to export to. This defaults
            to the Desktop.</help>
         </arg>
         <arg>
             <name>Server URL</name>
             <switch>-u</switch>
             <multiple>N</multiple>
             <argtype>string</argtype>
             <help>The URL of the server to which this
                         plug-in connects.</help>
         </arg>
         <arg>
             <name>Session ID</name>
             <switch>-c</switch>
             <multiple>N</multiple>
             <argtype>string</argtype>
             <help>The session ID for the connection. If not specified,
             the plugin will try to connect based on the credentials it finds in config</help>
         </arg>
     </non_mandatory_args> 
     <switches>
         <arg>
             <name>Export as XML</name>
             <switch>-x</switch>
             <help>Export as an XML file instead of a JSON file.</help>
         </arg>
     </switches>
  </plugin_info>


All arguments are defined inside an `<arg></arg>` tag, and must define 
a `<name>` That will be shown in the UI, `<switch>` (-n for network, for example),
`<multiple>` which defines whether multiple of this argument can be provided, 
`<allownew>` which if set to 'Y' allows the user in the UI create a new network
`<argtype>` which states the type of the argument, be it a date, string, integer
or even network or scenario. The latter options create a drop-down in the UI.
The `<help>` tag allows users of the UI to see exactly what the argument is for.

An example of a full `<arg>` tag might be:

.. code-block:: xml

 <arg>
    <name>nodes</name>
    <switch>-n</switch>
    <multiple>Y</multiple>
    <argtype>file</argtype>
    <help>One or multiple files containing nodes and
                attributes.</help>
 </arg>

There are 3 types of category these args can be put into within the plugin xml. *mandatory*,  *non-mandatory* and *switches*.

Mandatory inputs are must be included or the plugin will not even start. Normally
this will be a network ID, session ID, URL and any other necessary inputs.

Non-Mandatory inputs may be provided to the plugin to provide extra functionality, but
the plugin will still run without them. One example for an export plugin might
be to define a specific directory for export: 'export_dir=/tmp/'.

Switches arguments which do not require an input, but are simply 'on/off'. They
appear as check boxes in the UI. 

Plugin XML example:
^^^^^^^^^^^^^^^^^^^

Click :download:`here <plugin_input.xsd>` for the plugin xsd file

Click :download:`here <plugin.xml>` for a full example of the plugin xml

.. _folder_structure:

Folder Structure
----------------
There are 2 structures supported by Hydra Modeller.
If there is only one executable in the plugin, the the following structure
should be used::

 MyPlugin
   --> templates (optional)
      --> template.xml
   --> ExportJSON
      --> ExportJSON.exe
      --> plugin.xml
      --> icon.png

or alternatively, if the plugin package contains more than 1 executable (import
and export, for example)::

 MyPlugin
   --> templates (optional)
      --> template.xml
   --> plugins
      --> ExportJSON
        --> ExportJSON.exe
        --> plugin.xml
        --> icon32.png
        --> icon16.png
      --> ImportJSON 
        --> ImportJSON.exe
        --> plugin.xml
        --> icon32.png
        --> icon16.png

.. _session_and_url:

Session ID and Server URL
-------------------------
As Hydra apps connect to Hydra Platform via the same interface as the UI, they need to us the same credentials as the UI. To this end, all plugins should implement
two arguments, namely server_url (to tell the plugin where the server is) and session_id
to tell the server who is making the call. These two arguments should appear
in the non_mandatory_args section of the plugin.xml and look just like this:

.. code-block:: xml

 <arg>
     <name>server_url</name>
     <switch>-u</switch>
     <multiple>N</multiple>
     <argtype>string</argtype>
     <help>Specify the URL of the server to which this
                 plug-in connects.</help>
 </arg>
 <arg>
     <name>session_id</name>
     <switch>-c</switch>
     <multiple>N</multiple>
     <argtype>string</argtype>
     <help>Specify the session ID for the connection. If not specified,
     the plugin will try to connect based on the credentials it finds in config</help>
 </arg>


.. _creating_an_exe:

Creating an executable of your code
-----------------------------------

.. _pyinstaller: https://github.com/pyinstaller/pyinstaller/wiki

Hydra Modeller is windows-based and so requires all its plugins to be executable
in windows. *This still means that plugins can be written in cross-platform languages
like python, java or ruby.*. Taking python as an example, the pyinstaller_ package
allows a python script to be turned into an exe by simply typing::

 pyinstaller mypluginscript.py

Which produces a file called mypluginscript.exe

For more control, you can define a '.spec' file, which allows you to decide
what level of compression you want (more compression = less performance normally),
the name of the output file, whether it's a console application or not etc...


