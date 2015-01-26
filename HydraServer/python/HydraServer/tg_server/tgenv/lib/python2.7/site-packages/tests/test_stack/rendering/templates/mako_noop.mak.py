# -*- encoding:utf-8 -*-
from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 8
_modified_time = 1348162811.51627
_enable_loop = True
_template_filename = '/home/amol/tg/turbogears2-tg2-py3/tests/test_stack/rendering/templates/mako_noop.mak'
_template_uri = 'mako_noop.mak'
_source_encoding = 'utf-8'
from markupsafe import escape_silent as escape
_exports = []


def render_body(context,**pageargs):
    __M_caller = context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        __M_writer = context.writer()
        # SOURCE LINE 2
        __M_writer('\n<p>This is the mako index page</p>')
        return ''
    finally:
        context.caller_stack._pop_frame()


