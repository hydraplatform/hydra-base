Exporting the Network
=====================

To export the function, we need 1 pieces of information, the ID of the nework.
The ID of the scenario within the network is not strictly necessary nor is the 
folder where you want the file to end up. If you do not specify either of these,
all the scenarios will be exported, and the file will save to the desktop.

As we are using a JSON connection, the network is returned as a JSON object. 
We can therefore just convert this to a string and send it to the file. No
parsing needed!

.. code-block:: python

    def export(self, network_id, scenario_id=None, target_dir=None):
        """
            Export a network in Hydra as a .json file
        """

        #Will print !!Output Retrieving Network
        write_output("Retrieving Network") 
        #Will print !!Progress 2/3
        write_progress(2, self.num_steps) 
        
        #Some basic error checking. 
        if network_id is not None:
            #The network ID can be specified to get the network...
            try:
                #Get the nework using the connection's 'call' function.
                #This returns the network as a JSON dictionary. Handy that!
                if scenario_id is None:
                    network = self.connection.call('get_network', 
                                                  {'network_id' : int(network_id)})
                else:
                    network = self.connection.call('get_network', 
                                                  {'network_id' : int(network_id),
                                                  'scenario_ids':[int(scenario_id)]})

                write_output("Network retrieved")
            except:
                raise HydraPluginError("Network %s not found."%network_id)

        else:
            raise HydraPluginError("A network ID must be specified!")
        
        #Default to $HOME/Desktop
        if target_dir is None:
            target_dir = os.path.join(os.path.expanduser('~'), 'Desktop')

        #If the target folder doesn't exist, create it.
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        #Write the network
        self.write_network(network, target_dir)


Once we have the network retrieved, save it to a file:
Note that the name of the file is derived from the name of the network.
As the network is a JSON dictionary, its name is accessed by doing ``network['name']``.
All the properties of the network can be listed by doing ``network.keys()``.

.. code-block:: python

    def write_network(self, network, target_dir):
        """
            Write a JSON-based network to a file.
        """

        write_output("Writing network to file")
        write_progress(3, self.num_steps) 

        file_name = "network_%s.json"%(network['name'])

        #To show to the user when the plugin is finished.
        self.files.append(os.path.join(target_dir, file_name))

        #os.path.join ensures cross-platform compatibility.
        network_file = open(os.path.join(target_dir, file_name), 'w')

        #Json.dumps turns a JSON dict into a string
        #Put ins some formatting to make the file easier to read
        network_file.write(json.dumps(network, sort_keys=True, indent=4, separators=(',', ': ')))

        write_output("Network Written to %s "%(target_dir))
