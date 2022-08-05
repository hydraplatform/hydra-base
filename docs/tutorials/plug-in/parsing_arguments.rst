Parsing the Arguments
=====================
All Hydra plugins must run as command-line processes. They therefore must
parse the arguments and provide help functions to help uses figure out what
kind of arguments are needed. Luckily, Python provides a simple mechanism to
do this, using the `ArgumentParser` class.

This allows you to put in a short description of your plugin and define the
arguments using `add_argument`.

.. code-block:: python

 def commandline_parser():
     """
         Parse the arguments passed in from the command line
     """
     parser = ap.ArgumentParser(
         description="""Export a network in to a file in JSON format.
                     Written by Stephen Knox <stephen.knox@manchester.ac.uk>
                     (c) Copyright 2015, University of Manchester.
         """, epilog="For more information visit www.hydraplatform.org", 
        formatter_class=ap.RawDescriptionHelpFormatter)

     parser.add_argument('-n', '--network-id',
                         help='''Specify the network_id of the network to be exported.''')
     parser.add_argument('-s', '--scenario-id',
                         help='''Specify the ID of the scenario to be exported. If no
                         scenario is specified, all scenarios in the network will be
                         exported.
                         ''')
     parser.add_argument('-d', '--target-dir',
                         help='''Target directory''')
     parser.add_argument('-u', '--server-url',
                         help='''Specify the URL of the server to which this
                         plug-in connects.''')
     parser.add_argument('-c', '--session-id',
                         help='''Session ID. If this does not exist, a login will be
                         attempted based on details in config.''')
     return parser

When you run ``python ExportJSON.py -h`` to get some help, this will be printed:

::

 >> python ExportJSON.py -h

 usage: ExportJSON.py [-h] [-n NETWORK_ID] [-s SCENARIO_ID] [-d TARGET_DIR]
                      [-u SERVER_URL] [-c SESSION_ID]
 
 Export a network in to a file in JSON format. Written by Stephen Knox
 <stephen.knox@manchester.ac.uk> (c) Copyright 2015, University of Manchester.
 
 optional arguments:
   -h, --help            show this help message and exit
   -n NETWORK_ID, --network-id NETWORK_ID
                         Specify the network_id of the network to be exported.
   -s SCENARIO_ID, --scenario-id SCENARIO_ID
                         Specify the ID of the scenario to be exported. If no
                         scenario is specified, all scenarios in the network
                         will be exported.
   -d TARGET_DIR, --target-dir TARGET_DIR
                         Target directory
   -u SERVER_URL, --server-url SERVER_URL
                         Specify the URL of the server to which this plug-in
                         connects.
   -c SESSION_ID, --session-id SESSION_ID
                         Session ID. If this does not exist, a login will be
                         attempted based on details in config.
 
 For more information visit www.hydraplatform.org

