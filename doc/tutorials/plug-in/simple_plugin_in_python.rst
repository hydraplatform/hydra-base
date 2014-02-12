Developping a plug-in in Python
===============================
 
.. sidebar:: Concepts introduced in this tutorial are:

    .. toctree::
       :maxdepth: 2
       :numbered:

       connecting
       config_file
       projects
       networks
       templates
       datasets
       scenarios
       integration
   
This tutorial shows how a simple Hydra plug-in is developed in Python. Hydra
plug-ins connect to Hydra Platform through a `SOAP
<http://en.wikipedia.org/wiki/SOAP>`_ interface. Developing a plug-in in any
other programming language that provides SOAP libraries should be similar to the
code presented here.

It is assumed that the reader is familiar with Python or any other programming
language.


.. topic:: Example data

    In order to test the code introduced in this tutorial, a set of sample data
    is available. 
