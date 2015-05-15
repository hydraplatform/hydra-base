Running the Code
================
The code now needs to be run, and this is best done inside a 'main', done
in python like so:

The basic flow of the plugin is:
 - Parse the arguments
 - Instantiate the exporter object (connect)
 - Run 'export'
 - Catch any errors
 - Print out a message of success or failure (done here in XML)

.. code-block:: python

 if __name__ == '__main__':
     #Parse the command line arguments
     parser = commandline_parser()
     args = parser.parse_args()

     #Create the json exporter
     json_exporter = ExportJSON(url=args.server_url, session_id=args.session_id)
     errors = []
     
     #It is always adviseable to put your code inside a try:catch so that
     #errors can be caught and displayed to user in a sensible way. THis will
     #save the user time and frustration as well as you, who has to fix the problem!
     try:
         write_output("Starting App")
         write_progress(1, json_exporter.num_steps) 
           
         #Call the export function
         json_exporter.export(args.network_id, args.scenario_id, args.target_dir)

         #The final message to be displayed to the user.
         message = "Export complete"
     except HydraPluginError as e:
         message="An error has occurred"
         errors = [e.message]
     except Exception, e:
         message="An error has occurred"
         errors = [e]
    
     #This creates an XML string which is parsed by Hydra Modeller and displayed
     #to the user in a nice pop-up box.
     xml_response = create_xml_response('ExportJSON',
                                                  args.network_id,
                                                  [],
                                                  errors,
                                                  [],
                                                  message,
                                                  json_exporter.files)

     #Print is used here as all the stuff parsed by Hydra Modeller (including
     #write_progress & write_output) need to be sent to sdtout, not stderr (which
     #is where logging goes).
     print xml_response

Now the code can be run.
First make sure the HydraPlatform server is running. For the purposes of this
test, it will be running locally and we won't be using Hydra Modeller so there
is no need to pass in a URL or session ID.

Export the network with all it's scenarios

::
    
    >> python ExportJSON.py -n 2

Export the network with one of its scenarios

::

    >> python ExportJSON.py -n 2 -s 2

Export the network to /tmp

::

    >> python ExportJSON.py -n 2 -s 2 -d /tmp
