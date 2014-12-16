"""
This is the main dispatcher module.

Dispatch works as follows:
Start at the Originating dispatcher, which must
have a _dispatch function, which defines how we move
from dispatch object to dispatch object in the system.
Continue following the dispatch mechanism for a given
controller until you reach another controller with a
_dispatch method defined.  Use the new _dispatch
method until another controller with _dispatch defined
or until the url has been traversed to entirety.

This module also contains the standard ObjectDispatch
class which provides the ordinary TurboGears mechanism.

"""

from crank.util import get_argspec, method_matches_args
from crank.dispatcher import Dispatcher
from webob.exc import HTTPNotFound
from inspect import ismethod

class ObjectDispatcher(Dispatcher):
    """
    Object dispatch (also "object publishing") means that each portion of the
    URL becomes a lookup on an object.  The next part of the URL applies to the
    next object, until you run out of URL.  Processing starts on a "Root"
    object.

    Thus, /foo/bar/baz become URL portion "foo", "bar", and "baz".  The
    dispatch looks for the "foo" attribute on the Root URL, which returns
    another object.  The "bar" attribute is looked for on the new object, which
    returns another object.  The "baz" attribute is similarly looked for on
    this object.

    Dispatch does not have to be directly on attribute lookup, objects can also
    have other methods to explain how to dispatch from them.  The search ends
    when a decorated controller method is found.

    The rules work as follows:

    1) If the current object under consideration is a decorated controller
       method, the search is ended.

    2) If the current object under consideration has a "default" method, keep a
       record of that method.  If we fail in our search, and the most recent
       method recorded is a "default" method, then the search is ended with
       that method returned.

    3) If the current object under consideration has a "lookup" method, keep a
       record of that method.  If we fail in our search, and the most recent
       method recorded is a "lookup" method, then execute the "lookup" method,
       and start the search again on the return value of that method.

    4) If the URL portion exists as an attribute on the object in question,
       start searching again on that attribute.

    5) If we fail our search, try the most recent recorded methods as per 2 and
       3.
    """

    #Change to True to allow extra params to pass thru the dispatch
    _use_lax_params = False
    _use_index_fallback = True

    def _is_exposed(self, controller, name):
        """Override this function to define how a controller method is
        determined to be exposed.

        :Arguments:
          controller - controller with methods that may or may not be exposed.
          name - name of the method that is tested.

        :Returns:
           True or None
        """
        return ismethod(getattr(controller, name, False))

    def __call__(self, state, remainder=None):
        return self._dispatch(state, remainder)

    def _perform_security_check(self, controller):
        #xxx do this better
        obj = getattr(controller, 'im_self', controller)

        security_check = getattr(obj, '_check_security', None)
        if security_check is not None:
            security_check()

    def _dispatch_controller(self, current_path, controller, state, remainder):
        """
           Essentially, this method defines what to do when we move to the next
           layer in the url chain, if a new controller is needed.
           If the new controller has a _dispatch method, dispatch proceeds to
           the new controller's mechanism.

           Also, this is the place where the controller is checked for
           controller-level security.
        """
        
        dispatcher = getattr(controller, '_dispatch', None)
        if dispatcher is not None:
            self._perform_security_check(controller)
            state.add_controller(current_path, controller)
            state.dispatcher = controller
            return dispatcher(state, remainder)
        state.add_controller(current_path, controller)
        return self._dispatch(state, remainder)

    def _dispatch_first_found_default_or_lookup(self, state, remainder):
        """
        When the dispatch has reached the end of the tree but not found an
        applicable method, so therefore we head back up the branches of the
        tree until we found a method which matches with a default or lookup method.
        """

        if not state._notfound_stack:
            if self._use_index_fallback:
                #see if there is an index
                current_controller = state.controller
                method = getattr(current_controller, 'index', None)
                if method:
                    if method_matches_args(method, state.params, remainder, self._use_lax_params):
                        state.add_method(current_controller.index, remainder)
                        return state
            raise HTTPNotFound
        else:
        
            m_type, meth, m_remainder, warning = state._notfound_stack.pop()

            if m_type == 'lookup':
                new_controller, new_remainder = meth(*m_remainder)
                state.add_controller(new_controller.__class__.__name__, new_controller)
                dispatcher = getattr(new_controller, '_dispatch', self._dispatch)
                r = dispatcher(state, new_remainder)
                return r
            elif m_type == 'default':
                state.add_method(meth, m_remainder)
                state.dispatcher = self
                return state
#        raise HTTPNotFound

    def _dispatch(self, state, remainder=None):
        """
        This method defines how the object dispatch mechanism works, including
        checking for security along the way.
        """
        if state.dispatcher is None:
            state.dispatcher = self
            state.add_controller('/', self)
        if remainder is None:
            remainder = state.path
            
        current_controller = state.controller

        #skip any empty urls
        if remainder and not(remainder[0]):
            return self._dispatch(state, remainder[1:])

        self._enter_controller(state, remainder)

        #we are plumb out of path, check for index
        if not remainder:
            if self._is_exposed(current_controller, 'index') and \
               method_matches_args(current_controller.index, state.params, remainder, self._use_lax_params):
                state.add_method(current_controller.index, remainder)
                return state
            #if there is no index, head up the tree
            #to see if there is a default or lookup method we can use
            return self._dispatch_first_found_default_or_lookup(state, remainder)


        current_path = state.path_translator(remainder[0])
        current_args = remainder[1:]

        #an exposed method matching the path is found
        if self._is_exposed(current_controller, current_path):
            #check to see if the argspec jives
            controller = getattr(current_controller, current_path)
            if method_matches_args(controller, state.params, current_args, self._use_lax_params):
                state.add_method(controller, current_args)
                return state

        #another controller is found
        current_controller = getattr(current_controller, current_path, None)
        if current_controller is not None:
            return self._dispatch_controller(current_path, current_controller,
                                             state, current_args)

        #dispatch not found
        return self._dispatch_first_found_default_or_lookup(state, remainder)

    def _enter_controller(self, state, remainder):
        '''Checks security and pushes any notfound (lookup or default) handlers
        onto the stack
        '''
        current_controller = state.controller
        self._perform_security_check(current_controller)
        if hasattr(current_controller, '_lookup') and self._is_exposed(current_controller, '_lookup'):
            state._notfound_stack.append(('lookup', current_controller._lookup, remainder, None))
        if hasattr(current_controller, '_default') and self._is_exposed(current_controller, '_default'):
            state._notfound_stack.append(('default', current_controller._default, remainder, None))
            
