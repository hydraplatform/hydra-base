Documentation
=============
A very important aspect of plugins is documenation, saying what the plugin does and how it works. The convention we use here is shown below. This formatted using 
the `restructured text <http://docutils.sourceforge.net/rst.html>`_ convention, so that it can be converted to HTML easily.
Note how the table boundaries all line up.

This should be the very first lines in your file, after ``#!/usr/bin/env python``
and any licensing details.

As an example of this in action, and how useful it is, see :ref:`importcsv` which
was created from this approach.

::

 """
  A Hydra plug-in for exporting a hydra network to a JSON file.
 
 Basics
 ~~~~~~
 
 The plug-in for exporting a network to a JSON file.
 Basic usage::
 
        ExportJSON.py [-h] [-n network_id] [-s scenario_id] [-d target_dir] [-x]
 
 Options
 ~~~~~~~
 
 ====================== ====== ============ =======================================
 Option                 Short  Parameter    Description
 ====================== ====== ============ =======================================
 ``--help``             ``-h``              Show help message and exit.
 ``--network-id         ``-n`` NETWORK_ID   The ID of the network to be exported.
 ``--scenario-id        ``-s`` SCENARIO_ID  The ID of the scenario to be exported.
                                            (optional)
 ``--target-dir``       ``-d`` TARGET_DIR   Target directory 
 ``--server-url``       ``-u`` SERVER-URL   Url of the server the plugin will
                                            connect to.
                                            Defaults to localhost.
 ``--session-id``       ``-c`` SESSION-ID   Session ID used by the calling software.
                                            If left empty, the plugin will attempt
                                            to log in itself.
 ====================== ====== ============ =======================================
 
 """

