"""
This is the main dispatcher module.

Dispatch works as follows:
Start at the RootController, the root controller must
have a _dispatch function, which defines how we move
from object to object in the system.
Continue following the dispatch mechanism for a given
controller until you reach another controller with a
_dispatch method defined.  Use the new _dispatch
method until anther controller with _dispatch defined
or until the url has been traversed to entirety.

"""

class Dispatcher(object):
    """
       Extend this class to define your own mechanism for dispatch.
    """

    def _dispatch(self, state, remainder=None):
        """override this to define how your controller should dispatch.
        returns: dispatcher, controller_path, remainder
        """
        raise NotImplementedError

    def _setup_wsgiorg_routing_args(self, path, remainder, params):
        """
        This is expected to be overridden by any subclass that wants to set
        the routing_args.
        """

    def _setup_wsgi_script_name(self, path, remainder, params):
        """
        This is expected to be overridden by any subclass that wants to set
        the script name.
        """

