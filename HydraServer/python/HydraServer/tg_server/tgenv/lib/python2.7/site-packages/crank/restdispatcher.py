"""
This module contains the RestDispatcher implementation

Rest controller provides a RESTful dispatch mechanism, and
combines controller decoration for TG-Controller behavior.
"""
from inspect import ismethod
from webob.exc import HTTPMethodNotAllowed
from crank.util import get_argspec, method_matches_args
from crank.objectdispatcher import ObjectDispatcher

class RestDispatcher(ObjectDispatcher):
    """Defines a restful interface for a set of HTTP verbs.
    Please see RestController for a rundown of the controller
    methods used.
    """

    def _find_first_exposed(self, controller, methods):
        for method in methods:
            if self._is_exposed(controller, method):
                return getattr(controller, method)

    def _handle_put_or_post(self, http_method, state, remainder):
        current_controller = state.controller
        if remainder:
            current_path = remainder[0]
            if self._is_exposed(current_controller, current_path):
                state.add_method(getattr(current_controller, current_path), remainder[1:])
                return state

            if self._is_controller(current_controller, current_path):
                current_controller = getattr(current_controller, current_path)
                return self._dispatch_controller(current_path, current_controller, state, remainder[1:])

        method = self._find_first_exposed(current_controller, [http_method])
        if method and method_matches_args(method, state.params, remainder, self._use_lax_params):
            state.add_method(method, remainder)
            return state

        return self._dispatch_first_found_default_or_lookup(state, remainder)

    def _handle_delete(self, http_method, state, remainder):
        current_controller = state.controller
        method = self._find_first_exposed(current_controller, ('post_delete', 'delete'))

        if method and method_matches_args(method, state.params, remainder, self._use_lax_params):
            state.add_method(method, remainder)
            return state

        #you may not send a delete request to a non-delete function
        if remainder and self._is_exposed(current_controller, remainder[0]):
            raise HTTPMethodNotAllowed

        # there might be a sub-controller with a delete method, let's go see
        if remainder:
            sub_controller = getattr(current_controller, remainder[0], None)
            if sub_controller:
                remainder = remainder[1:]
                state.current_controller = sub_controller
                state.path = remainder
                r = self._dispatch_controller(remainder[0], sub_controller, state, remainder)
                if r:
                    return r
        return self._dispatch_first_found_default_or_lookup(state, remainder)

    def _check_for_sub_controllers(self, state, remainder):
        current_controller = state.controller
        method = None
        for find in ('get_one', 'get'):
            if hasattr(current_controller, find):
                method = find
                break

        if method is None:
            return

        fixed_args, var_args, kws, kw_args = get_argspec(getattr(current_controller, method))
        fixed_arg_length = len(fixed_args)
        if var_args:
            for i, item in enumerate(remainder):
                item = state.path_translator(item)
                if hasattr(current_controller, item) and self._is_controller(current_controller, item):
                    current_controller = getattr(current_controller, item)
                    state.add_routing_args(item, remainder[:i], fixed_args, var_args)
                    return self._dispatch_controller(item, current_controller, state, remainder[i+1:])
        elif fixed_arg_length< len(remainder) and hasattr(current_controller, remainder[fixed_arg_length]):
            item = state.path_translator(remainder[fixed_arg_length])
            if hasattr(current_controller, item):
                if self._is_controller(current_controller, item):
                    state.add_routing_args(item, remainder, fixed_args, var_args)
                    return self._dispatch_controller(item, getattr(current_controller, item),
                                                     state, remainder[fixed_arg_length+1:])

    def _handle_delete_edit_or_new(self, state, remainder):
        method_name = remainder[-1]
        if method_name not in ('new', 'edit', 'delete'):
            return

        if method_name == 'delete':
            method_name = 'get_delete'

        current_controller = state.controller

        if self._is_exposed(current_controller, method_name):
            method = getattr(current_controller, method_name)
            new_remainder = remainder[:-1]
            if method and method_matches_args(method, state.params, new_remainder, self._use_lax_params):
                state.add_method(method, new_remainder)
                return state

    def _handle_custom_get(self, state, remainder):
        controller = state.controller
        method_name = remainder[-1]

        current_controller = state.controller

        get_method = self._find_first_exposed(current_controller, ('get_%s' % method_name, method_name))
        if get_method:
            new_remainder = remainder[:-1]
            if method_matches_args(get_method, state.params, new_remainder, self._use_lax_params):
                state.add_method(get_method, new_remainder)
                return state

    def _handle_custom_method(self, method, state, remainder):
        current_controller = state.controller
        method_name = method
        http_method = state.request.method
        method = self._find_first_exposed(current_controller, ('%s_%s' %(http_method, method_name), method_name, 'post_%s' %method_name))

        if method and method_matches_args(method, state.params, remainder, self._use_lax_params):
            state.add_method(method, remainder)
            return state

        # there might be a sub-controller with a custom method, let's go see
        if remainder:
            sub_controller = getattr(current_controller, remainder[0], None)
            if sub_controller:
                current = remainder[0]
                remainder = remainder[1:]
                state.current_controller = sub_controller
                state.path = remainder
                r = self._dispatch_controller(current, sub_controller, state, remainder)
                if r:
                    return r
        return self._dispatch_first_found_default_or_lookup(state, remainder)

    def _handle_get(self, method, state, remainder):
        current_controller = state.controller
        if not remainder:
            method = self._find_first_exposed(current_controller, ('get_all', 'get'))
            if method:
                state.add_method(method, remainder)
                return state
            if self._is_exposed(current_controller, 'get_one'):
                method = current_controller.get_one
                if method and method_matches_args(method, state.params, remainder, self._use_lax_params):
                    state.add_method(method, remainder)
                    return state
            return self._dispatch_first_found_default_or_lookup(state, remainder)

        #test for "delete", "edit" or "new"
        r = self._handle_delete_edit_or_new(state, remainder)
        if r is not None:
            return r

        #test for custom REST-like attribute
        r = self._handle_custom_get(state, remainder)
        if r is not None:
            return r

        current_path = state.path_translator(remainder[0])
        if self._is_exposed(current_controller, current_path):
            state.add_method(getattr(current_controller, current_path), remainder[1:])
            return state

        if self._is_controller(current_controller, current_path):
            current_controller = getattr(current_controller, current_path)
            return self._dispatch_controller(current_path, current_controller, state, remainder[1:])

        method = self._find_first_exposed(current_controller, ('get_one', 'get'))
        if method and method_matches_args(method, state.params, remainder, self._use_lax_params):
            state.add_method(method, remainder)
            return state

        return self._dispatch_first_found_default_or_lookup(state, remainder)

    _handler_lookup = {
        'put':_handle_put_or_post,
        'post':_handle_put_or_post,
        'delete':_handle_delete,
        'get':_handle_get,
        }

    def _is_controller(self, controller, name):
        """
        Override this function to define how an object is determined to be a
        controller.
        """
        method = getattr(controller, name, None)
        if method is not None:
            return not ismethod(method)

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
        self._perform_security_check(current_controller)

        #log.debug('Entering dispatch for remainder: %s in controller %s'%(remainder, self))
        if not hasattr(state, 'http_method'):
            method = state.request.method.lower()
            params = state.params

            #conventional hack for handling methods which are not supported by most browsers
            request_method = params.get('_method', None)
            if request_method:
                request_method = request_method.lower()
                #make certain that DELETE and PUT requests are not sent with GET
                if method == 'get' and request_method == 'put':
                    raise HTTPMethodNotAllowed
                if method == 'get' and request_method == 'delete':
                    raise HTTPMethodNotAllowed
                method = request_method
                del state.params['_method']
            state.http_method = method

        r = self._check_for_sub_controllers(state, remainder)
        if r is not None:
            return r

        if state.http_method in self._handler_lookup.keys():
            r = self._handler_lookup[state.http_method](self, state.http_method, state, remainder)
        else:
            r = self._handle_custom_method(state.http_method, state, remainder)
        return r
