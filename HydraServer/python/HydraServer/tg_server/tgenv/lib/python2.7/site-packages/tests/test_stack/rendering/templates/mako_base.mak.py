# -*- encoding:utf-8 -*-
from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 8
_modified_time = 1348162811.530505
_enable_loop = True
_template_filename = '/home/amol/tg/turbogears2-tg2-py3/tests/test_stack/rendering/templates/mako_base.mak'
_template_uri = '/mako_base.mak'
_source_encoding = 'utf-8'
from markupsafe import escape_silent as escape
_exports = []


def render_body(context,**pageargs):
    __M_caller = context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        self = context.get('self', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 2
        __M_writer('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"\n"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">\n<html>\n  <head>\n    ')
        # SOURCE LINE 6
        __M_writer(escape(self.head_tags()))
        __M_writer('\n  </head>\n  <body>\n  \t<p>Inside parent template</p>\n    ')
        # SOURCE LINE 10
        __M_writer(escape(self.body()))
        __M_writer('\n  </body>\n</html>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


