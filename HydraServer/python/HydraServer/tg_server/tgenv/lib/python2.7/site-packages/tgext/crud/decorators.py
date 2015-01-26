"""
"""
from decorator import decorator
from tg.decorators import validate as tgValidate
from tg import flash, config, request, response, tmpl_context
from tgext.crud.validators import report_json_error
from tgext.crud.utils import DisabledPager
from tg.decorators import paginate
from tg.util import Bunch
from tgext.crud._compat import im_func, im_self, string_type, unicode_text

try:
    import transaction
except ImportError:
    transaction = None

class registered_validate(tgValidate):
    """
    Use a validator registered within the controller to validate.
    This is especially useful when you have a controller lookup that instantiates
    a controller who's forms are created at execution time.  This
    allows controller methods to validate on forms which are different.
    Otherwise, each method of the controller would have to share the 
    same method of validation.
    
    :Usage:
     
    >>> from tg.controllers import TGController
    >>> class MyController(TGController):
    >>>     
    >>>     def __init__(self, params):
    >>>         self.form = MyForm(params)
    >>>         register_validators(self, 'eval_form', self.form)
    >>>     
    >>>     @expose('myproject.templates.error_handler')
    >>>     def render_form(self):
    >>>         tg.tg_context.form = self.form
    >>>         return
    >>>     
    >>>     @registered_validate(error_controller=render_form)
    >>>     @expose()
    >>>     def eval_form(self):
    >>>         raise Exception
    """
    def __init__(self, error_handler=None, *args,**kw):
        self._error_handler = error_handler
        self.needs_controller = True
        class Validators(object):
            def validate(self, controller, params, state):
                func_name = im_func(controller).__name__
                validators = im_self(controller).__validators__
                if func_name in validators:
                    v = validators[func_name].validate(params, state)
                    return v
        self.validators = Validators()

    @property
    def error_handler(self):
        try:
            response_type = request.response_type
        except:
            response_type = None

        if response_type == 'application/json':
            return report_json_error
        else:
            return self._error_handler

        
def register_validators(controller, name, validators):
    """
    Helper function which sets the validator lookup for the controller.
    """
    if not hasattr(controller, '__validators__'):
        controller.__validators__ = {}
    controller.__validators__[name] = validators

try:
    from sqlalchemy.exc import IntegrityError, DatabaseError, ProgrammingError
    sqla_errors =  (IntegrityError, DatabaseError, ProgrammingError)
except ImportError:
    sqla_errors = ()

def catch_errors(error_types=None, error_handler=None):
    """
    A validator which catches the Exceptions in the error_types.
    When an exception occurs inside the decorated function, the error_handler
    is called, and the message from the exception is flashed to the screen.
    
    :Usage:
    
    >>> from tg.controllers import TGController
    >>> class MyController(TGController):
    >>>     @expose('myproject.templates.error_handler')
    >>>     def error_handler(self):
    >>>     return
    >>>     
    >>>     @catch_errors(Exception, error_handler=error_handler)
    >>>     @expose()
    >>>     def method_with_exception(self):
    >>>         raise Exception
    """
    def wrapper(func, self, *args, **kwargs):
        """Decorator Wrapper function"""
        try:
            value = func(self, *args, **kwargs)
        except error_types as e:
            try:
                message = unicode_text(e)
            except:
                message = None

            if message:
                if request.response_type == 'application/json':
                    response.status_code = 400
                    return dict(message=message)

                flash(message, status="status_alert")
                # have to get the instance that matches the error handler.  This is not a great solution, but it's 
                # what we've got for now.
                if isinstance(error_handler, string_type):
                    name = error_handler
                else:
                    name = error_handler.__name__
                func = getattr(self, name)
                remainder = []
                remainder.extend(args)

                if isinstance(e, sqla_errors):
                    #if the error is a sqlalchemy error suppose we need to rollback the transaction
                    #so that the error handler can perform queries.
                    if transaction is not None and config.get('tgext.crud.abort_transactions', False):
                        #This is in case we need to support multiple databases or two phase commit.
                        transaction.abort()
                    else:
                        self.session.rollback()

                return self._call(func, params=kwargs, remainder=remainder)

        return value
    return decorator(wrapper)


class optional_paginate(paginate):
    def before_render(self, remainder, params, output):
        paginator = request.paginators[self.name]
        if paginator.paginate_items_per_page < 0:
            if not getattr(tmpl_context, 'paginators', None):
                tmpl_context.paginators = Bunch()
            tmpl_context.paginators[self.name] = DisabledPager()
            return

        super(optional_paginate, self).before_render(remainder, params, output)
