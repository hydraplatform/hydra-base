from tg import expose, response, tmpl_context
from formencode import FancyValidator, Invalid

@expose('json:', content_type='application/json')
def report_json_error(*args, **kw):
    response.status_code = 400
    return tmpl_context.form_errors


class EntityValidator(FancyValidator):
    def __init__(self, provider, model):
        super(FancyValidator, self).__init__()
        self.model = model
        self.provider = provider
        self.primary_field = self.provider.get_primary_field(self.model)

    def _to_python(self, value, state):
        try:
            return self.provider.get_obj(self.model, {self.primary_field:value})
        except:
            return None

    def validate_python(self, value, state):
        if not value:
            raise Invalid('object not found', value, state)