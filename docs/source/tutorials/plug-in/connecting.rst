Connecting to Hydra Platform
============================
To start the Plugin, we create an ``ExportJSON`` class which will do our
work for us. 


The constructor of this class, ``__init__`` takes two arguments:
a URL and a session_id. The url tells the plugin where Hydra Platform is. If this is left as None, a default is found in config.

The second parameter is session_id. If a user is already connected to Hydra, (through Hydra Modeller) they already have a session ID.
By passing their existing session ID into the plugin, 
there is no need for the user to log in again.

The connection is created by creating a new ``JsonConnection`` object, passing
in the url and then calling ``login`` if necessary. This can take a username and password, but if not provided, these details are fetched from config.

Finally ``self.num_steps`` is defined. This simply allows us to keep track of how many steps there are in the plugin, so that we can use it as a way to display to the user at what point in the process the plugin is currently operating. 

.. code-block:: python
   
 class ExportJSON(object):
     """
        Exporter of Hydra networks to JSON or XML files.
     """
     def __init__(self, url=None, session_id=None):

         #Record the names of the files created by the plugin so we can
         #display them to the user.
         self.files    = []

         self.connection = JsonConnection(url)
         write_output("Connecting...")
         if session_id is not None:
             log.info("Using existing session %s", session_id)
             self.connection.session_id=session_id
         else:
             self.connection.login()

         self.num_steps = 3
