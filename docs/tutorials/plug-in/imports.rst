Imports
=======

First, import the necessary classes and functions.
Note the ``HydraLib`` imports. HydraLib provides several functions which 
make connecting plugins to Hydra easier.

Of particular interest are ``JsonConnection`` which does all the hard work of 
connecting to Hydra and provides functions for making server function calls, namely the `call` function.

Next are the ``write_progress`` and ``write_output`` functions. These help Hydra
Modeller keep track of what is happening within the plugin as it's running. The
more of these you put in, the more a user will understand what is happening, which will help them if something goes wrong.
The outut of the two functions look like this, and are printed to stdout. Hydra
Modeller reads from stdout while the plugin is running and displays the output
of these two functions:

 - ``write_progress(1, 3)`` >> "!!Progres 1/3"
 - ``write_output("Parsing")`` >> "!!Output Parsing"

Finally there's the ``HydraPluginError``. When you want to raise an exception, this
is typically the one to raise as it specifically designed to work with these plugins.

.. code-block:: python
 
 #Library for parsing command-line arguments.
 import argparse as ap
 
 #Python utility libraries.
 from HydraLib.HydraException import HydraPluginError
 from HydraLib.PluginLib import JsonConnection,\
                               create_xml_response,\
                               write_progress,\
                               write_output
 #General library for working with JSON objects
 import json
 #Used for working with files.
 import os, sys
