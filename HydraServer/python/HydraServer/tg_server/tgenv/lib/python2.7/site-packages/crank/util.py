"""
Utilities used by crank.

Copyright (c) Chrispther Perkins
MIT License
"""

import collections, sys, string
from inspect import getargspec

__all__ = [
        'get_argspec', 'get_params_with_argspec', 'remove_argspec_params_from_params',
        'method_matches_args', 'Path', 'default_path_translator'
    ]


_PY2 = bool(sys.version_info[0] == 2)


_cached_argspecs = {}
def get_argspec(func):
    try:
        im_func = func.im_func
    except AttributeError:
        im_func = func

    try:
        argspec = _cached_argspecs[im_func]
    except KeyError:
        spec = getargspec(im_func)
        argvals = spec[3]

        # this is a work around for a crappy api choice in getargspec
        if argvals is None:
            argvals = []

        argspec = _cached_argspecs[im_func] = (spec[0][1:], spec[1], spec[2], argvals)

    return argspec

def get_params_with_argspec(func, params, remainder):
    argvars, var_args, argkws, argvals = get_argspec(func)

    if argvars and remainder:
        params = params.copy()
        remainder_len = len(remainder)
        for i, var in enumerate(argvars):
            if i >= remainder_len:
                break
            params[var] = remainder[i]
    return params

def remove_argspec_params_from_params(func, params, remainder):
    """Remove parameters from the argument list that are
       not named parameters
       Returns: params, remainder"""

    # figure out which of the vars in the argspec are required
    argvars, var_args, argkws, argvals = get_argspec(func)

    # if there are no required variables, or the remainder is none, we
    # have nothing to do
    if not argvars or not remainder:
        return params, remainder

    required_vars = argvars
    optional_vars = []
    if argvals:
        required_vars = argvars[:-len(argvals)]
        optional_vars = argvars[-len(argvals):]

    # make a copy of the params so that we don't modify the existing one
    params=params.copy()

    # replace the existing required variables with the values that come in
    # from params. these could be the parameters that come off of validation.
    remainder = list(remainder)
    remainder_len = len(remainder)
    for i, var in enumerate(required_vars):
        val = params.get(var, None)
        if i < remainder_len and val:
            remainder[i] = val
        elif val:
            remainder.append(val)
        if val:
            del params[var]

    # remove the optional positional variables (remainder) from the named parameters
    # until we run out of remainder, that is, avoid creating duplicate parameters
    for i, (original, var) in enumerate(zip(remainder[len(required_vars):],optional_vars)):
        if var in params:
            remainder[ len(required_vars)+i ] = params[var]
            del params[var]

    return params, tuple(remainder)


def method_matches_args(method, params, remainder, lax_params=False):
    """
    This method matches the params from the request along with the remainder to the
    method's function signiture.  If the two jive, it returns true.

    It is very likely that this method would go into ObjectDispatch in the future.
    """
    argvars, ovar_args, argkws, argvals = get_argspec(method)

    required_vars = argvars
    if argvals:
        required_vars = argvars[:-len(argvals)]

    params = params.copy()

    #remove the appropriate remainder quotient
    if len(remainder)<len(required_vars):
        #pull the first few off with the remainder
        required_vars = required_vars[len(remainder):]
    else:
        #there is more of a remainder than there is non optional vars
        required_vars = []

    #remove vars found in the params list
    for var in required_vars[:]:
        if var in params:
            required_vars.pop(0)
            # remove the param from the params so when we see if
            # there are params that arent in the non-required vars we
            # can evaluate properly
            del params[var]
        else:
            break;

    #remove params that have a default value
    vars_with_default = argvars[len(argvars)-len(argvals):]
    for var in vars_with_default:
        if var in params:
            del params[var]

    #make sure no params exist if keyword argumnts are missing
    if not lax_params and argkws is None and params:
        return False

    #make sure all of the non-optional-vars are there
    if not required_vars:
        #there are more args in the remainder than are available in the argspec
        if len(argvars)<len(remainder) and not ovar_args:
            return False
        return True


    return False


if _PY2: #pragma: no cover
    translation_dict = dict([(ord(c), unicode('_')) for c in unicode(string.punctuation)])
    translation_string = string.maketrans(string.punctuation,
                                          '_' * len(string.punctuation))
else: #pragma: no cover
    translation_dict = None
    translation_string = str.maketrans(string.punctuation,
                                       '_' * len(string.punctuation))


def default_path_translator(path_piece):
    if isinstance(path_piece, str):
        return path_piece.translate(translation_string)
    else: #pragma: no cover
        return path_piece.translate(translation_dict)


def noop_translation(path_piece):
    return path_piece


class Path(collections.deque):
    def __init__(self, value=None, separator='/'):
        self.separator = separator

        super(Path, self).__init__()

        if value is not None:
            self._assign(value)

    def _assign(self, value):
        separator = self.separator
        self.clear()

        if not _PY2: # pragma: no cover
            string_types = str
        else: # pragma: no cover
            string_types = basestring

        if isinstance(value, string_types):
            self.extend(value.split(separator))
            return

        self.extend(value)

    def __set__(self, obj, value):
        self._assign(value)

    def __str__(self):
        return str(self.separator).join(self)

    def __unicode__(self): # pragma: no cover
        #unused on PY3
        return unicode(self.separator).join(self)

    def __repr__(self):
        return "<Path %r>" % super(Path, self).__repr__()

    def __eq__(self, other):
        return type(other)(self) == other

    def __getitem__(self, i):
        try:
            return super(Path, self).__getitem__(i)

        except TypeError:
            return Path([self[i] for i in range(*i.indices(len(self)))])
