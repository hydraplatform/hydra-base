"""Admin Controller"""
import logging
log = logging.getLogger('tgext.admin')

from tg.controllers import TGController
from tg.decorators import with_trailing_slash, override_template, expose
from tg.exceptions import HTTPNotFound
from tg import config as tg_config, request
from tg import tmpl_context

from .config import AdminConfig
from .utils import make_pager_args

try:
    from tg.predicates import in_group
except ImportError:
    from repoze.what.predicates import in_group

try:
    from tg.configuration import milestones
except ImportError:
    milestones = None


class AdminController(TGController):
    """
    A basic controller that handles User Groups and Permissions for a TG application.
    """
    allow_only = in_group('managers')

    def __init__(self, models, session, config_type=None, translations=None):
        super(AdminController, self).__init__()
        if translations is None:
            translations = {}
        if config_type is None:
            config = AdminConfig(models, translations)
        else:
            config = config_type(models, translations)

        if config.allow_only:
            self.allow_only = config.allow_only

        self.config = config
        self.session = session
        self.missing_template = False

        if self.config.default_index_template:
            expose(self.config.default_index_template)(self.index)
        else:
            if milestones is None:
                self._choose_index_template()
            else:
                milestones.renderers_ready.register(self._choose_index_template)

        self.controllers_cache = {}

    def _choose_index_template(self):
        default_renderer = getattr(tg_config, 'default_renderer', 'genshi')
        if default_renderer not in ['genshi', 'mako', 'jinja']:
            if 'genshi' in tg_config.renderers:
                default_renderer = 'genshi'
            elif 'mako' in tg_config.renderers:
                default_renderer = 'mako'
            elif 'jinja' in tg_config.renderers:
                default_renderer = 'jinja'
            else:
                log.warn('TurboGears admin supports only Genshi, Mako and Jinja, please make sure you add at \
least one of those to your config/app_cfg.py base_config.renderers list.')
                self.missing_template = True

        index_template = ':'.join((default_renderer, self.config.layout.template_index))
        expose(index_template)(self.index)

    @with_trailing_slash
    @expose()
    def index(self):
        if self.missing_template:
            raise Exception('TurboGears admin supports only Genshi, Mako and Jinja, please make sure you add at \
    least one of those to your config/app_cfg.py base_config.renderers list.')

        return dict(models=[model.__name__ for model in self.config.models.values()])

    def _make_controller(self, config, session):
        m = config.model
        Controller = config.defaultCrudRestController

        class ModelController(Controller):
            model        = m
            table        = config.table_type(session)
            table_filler = config.table_filler_type(session)
            new_form     = config.new_form_type(session)
            new_filler   = config.new_filler_type(session)
            edit_form    = config.edit_form_type(session)
            edit_filler  = config.edit_filler_type(session)
            allow_only   = config.allow_only

            if hasattr(config.layout, 'crud_resources'):
                resources = config.layout.crud_resources

            def _before(self, *args, **kw):
                super(self.__class__, self)._before(*args, **kw)

                tmpl_context.make_pager_args = make_pager_args

                if request.response_type not in ('application/json',):
                    default_renderer = getattr(tg_config, 'default_renderer', 'genshi')
                    for layout_template in ('get_all', 'new', 'edit'):
                        for template in config.layout.crud_templates.get(layout_template, []):
                            if template.startswith(default_renderer):
                                override_template(getattr(self, layout_template), template)

        menu_items = None
        if self.config.include_left_menu:
            menu_items = self.config.models
        return ModelController(session, menu_items)

    @expose()
    def _lookup(self, model_name, *args):
        model_name = model_name[:-1]
        try:
            model = self.config.models[model_name]
        except KeyError:
            raise HTTPNotFound().exception

        try:
            controller = self.controllers_cache[model_name]
        except KeyError:
            config = self.config.lookup_controller_config(model_name)
            controller = self.controllers_cache[model_name] = self._make_controller(config, self.session)

        return controller, args

    @expose()
    def lookup(self, model_name, *args):
        return self._lookup(model_name, *args)
