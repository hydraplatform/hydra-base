# -*- encoding:utf-8 -*-
from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 8
_modified_time = 1348162811.56516
_enable_loop = True
_template_filename = '/home/amol/tg/turbogears2-tg2-py3/tests/test_stack/rendering/templates/mako_custom_format.mak'
_template_uri = 'mako_custom_format.mak'
_source_encoding = 'utf-8'
from markupsafe import escape_silent as escape
_exports = []


def render_body(context,**pageargs):
    __M_caller = context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        status = context.get('status', UNDEFINED)
        format = context.get('format', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 1
        __M_writer('<?xml version="1.0"?>\n<response>\n    <status>')
        # SOURCE LINE 3
        __M_writer(escape(status))
        __M_writer('</status>\n    <format>')
        # SOURCE LINE 4
        __M_writer(escape(format))
        __M_writer('</format>\n</response>')
        return ''
    finally:
        context.caller_stack._pop_frame()


