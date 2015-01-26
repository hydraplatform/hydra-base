from tg.i18n import lazy_ugettext as l_
from tgext.crud.utils import sprox_with_tw2

try:
    from tgext.crud.utils import SortableTableBase as TableBase
except:
    from sprox.tablebase import TableBase

try:
    from tgext.crud.utils import RequestLocalTableFiller as TableFiller
except:
    from sprox.fillerbase import TableFiller

from sprox.formbase import AddRecordForm, EditableForm
from markupsafe import Markup


__all__ = ['AdminTableBase', 'AdminAddRecordForm', 'AdminEditableForm', 'AdminTableFiller',
           'BoostrapAdminTableBase', 'BootstrapAdminAddRecordForm', 'BootstrapAdminEditableForm',
           'BootstrapAdminTableFiller']


def _merge_dicts(d1, d2):
    # A both dicts should be string keys only, dict(d1, **d2)
    # should always work, but this is actually the safest way.
    d = d1.copy()
    d.update(d2)
    return d


class AdminTableBase(TableBase):
    pass


class AdminAddRecordForm(AddRecordForm):
    FIELD_OPTIONS = {}


class AdminEditableForm(EditableForm):
    FIELD_OPTIONS = {}


class AdminTableFiller(TableFiller):
    pass


if sprox_with_tw2():
    from tw2.core import ChildParam
    from tw2.forms.widgets import BaseLayout
    from tw2.forms import SubmitButton

    class _BootstrapFormLayout(BaseLayout):
        resources = []
        template = 'tgext.admin.templates.bootstrap_form_layout'

        field_wrapper_attrs = ChildParam('Extra attributes to include in the element wrapping '
                                         'the widget itself.', default={})
        field_label_attrs = ChildParam('Extra attributes to include in the label of '
                                       'the widget itself.', default={})

    class _BootstrapAdminFormMixin(object):
        # Implemented as a MixIn instead of FormBase subclass
        # to avoid third party users confusion over MRO when subclassing.
        FIELD_OPTIONS = {'css_class': 'form-control',
                         'container_attrs': {'class': 'form-group'},
                         'label_attrs': {'class': 'control-label'}}

        def _admin_init_attrs(self):
            if 'child' not in self.__base_widget_args__:
                self.__base_widget_args__['child'] = _BootstrapFormLayout

            if 'submit' not in self.__base_widget_args__:
                if isinstance(self, AddRecordForm):
                    submit_text = l_('Create')
                else:
                    submit_text = l_('Save')

                self.__base_widget_args__['submit'] = SubmitButton(css_class='btn btn-primary',
                                                                   value=submit_text)

            for f in self.__fields__:
                self.__field_widget_args__[f] = _merge_dicts(self.FIELD_OPTIONS,
                                                             self.__field_widget_args__.get(f, {}))

    class BoostrapAdminTableBase(AdminTableBase):
        def _do_init_attrs(self):
            super(BoostrapAdminTableBase, self)._do_init_attrs()

            if 'css_class' not in self.__base_widget_args__:
                self.__base_widget_args__['css_class'] = 'table table-striped'

    class BootstrapAdminAddRecordForm(_BootstrapAdminFormMixin, AdminAddRecordForm):
        def _do_init_attrs(self):
            super(BootstrapAdminAddRecordForm, self)._do_init_attrs()
            self._admin_init_attrs()

    class BootstrapAdminEditableForm(_BootstrapAdminFormMixin, AdminEditableForm):
        def _do_init_attrs(self):
            super(BootstrapAdminEditableForm, self)._do_init_attrs()
            self._admin_init_attrs()

    class BootstrapAdminTableFiller(AdminTableFiller):
        def __actions__(self, obj):
            primary_fields = self.__provider__.get_primary_fields(self.__entity__)
            pklist = '/'.join(map(lambda x: str(getattr(obj, x)), primary_fields))

            return Markup('''
    <a href="%(pklist)s/edit" class="btn btn-primary">
        <span class="glyphicon glyphicon-pencil"></span>
    </a>
    <div class="hidden-lg hidden-md">&nbsp;</div>
    <form method="POST" action="%(pklist)s" style="display: inline">
        <input type="hidden" name="_method" value="DELETE" />
        <button type="submit" class="btn btn-danger" onclick="return confirm('%(msg)s')">
            <span class="glyphicon glyphicon-trash"></span>
        </button>
    </form>
''' % dict(msg=l_('Are you sure?'),
           pklist=pklist))

else:
    BoostrapAdminTableBase = AdminTableBase
    BootstrapAdminAddRecordForm = AdminAddRecordForm
    BootstrapAdminEditableForm = AdminEditableForm
    BootstrapAdminTableFiller = AdminTableFiller