from tg import expose, redirect
from tgext.crud import CrudRestController
from tgext.crud.utils import sprox_with_tw2
from tgext.crud.decorators import registered_validate
from .config import AdminConfig, CrudRestControllerConfig
from sprox.fillerbase import EditFormFiller
from sprox.formbase import FilteringSchema
from formencode.validators import FieldsMatch

if sprox_with_tw2():
    from tw2.forms import TextField, PasswordField
else:
    from tw.forms import TextField, PasswordField

from sprox.fillerbase import RecordFiller, AddFormFiller
from .layouts import BootstrapAdminLayout


class UserControllerConfig(CrudRestControllerConfig):
    def _do_init_with_translations(self, translations):
        user_id_field      = translations.get('user_id',       'user_id')
        user_name_field    = translations.get('user_name',     'user_name')
        email_field        = translations.get('email_address', 'email_address')
        password_field     = translations.get('password',      'password')
        display_name_field = translations.get('display_name',  'display_name')

        if not getattr(self, 'table_type', None):
            class Table(self.layout.TableBase):
                __entity__ = self.model
                __omit_fields__ = [user_id_field, '_groups', '_password', password_field]
                __url__ = '../users.json'

            self.table_type = Table

        if not getattr(self, 'table_filler_type', None):
            class MyTableFiller(self.layout.TableFiller):
                __entity__ = self.model
                __omit_fields__ = ['_password', password_field, '_groups']
            self.table_filler_type = MyTableFiller

        if hasattr(TextField, 'req'):
            edit_form_validator = FieldsMatch('password', 'verify_password',
                                              messages={'invalidNoMatch': 'Passwords do not match'})
        else:
            edit_form_validator = FilteringSchema(chained_validators=[FieldsMatch('password',
                                                                           'verify_password',
                                                  messages={'invalidNoMatch':
                                                            'Passwords do not match'})])

        if not getattr(self, 'edit_form_type', None):
            class EditForm(self.layout.EditableForm):
                __entity__ = self.model
                __require_fields__     = [user_name_field, email_field]
                __omit_fields__        = ['created', '_password', '_groups']
                __hidden_fields__      = [user_id_field]
                __field_order__        = [user_id_field, user_name_field, email_field,
                                          display_name_field, 'password', 'verify_password',
                                          'groups']
                password = PasswordField('password', value='',
                                         **self.layout.EditableForm.FIELD_OPTIONS)
                verify_password = PasswordField('verify_password',
                                                **self.layout.EditableForm.FIELD_OPTIONS)
                __base_validator__ = edit_form_validator

            if email_field is not None:
                setattr(EditForm, email_field, TextField)
            if display_name_field is not None:
                setattr(EditForm, display_name_field, TextField)
            self.edit_form_type = EditForm

        if not getattr(self, 'edit_filler_type', None):
            class UserEditFormFiller(EditFormFiller):
                __entity__ = self.model

                def get_value(self, *args, **kw):
                    v = super(UserEditFormFiller, self).get_value(*args, **kw)
                    del v['password']
                    return v

            self.edit_filler_type = UserEditFormFiller

        if not getattr(self, 'new_form_type', None):
            class NewForm(self.layout.AddRecordForm):
                __entity__ = self.model
                __require_fields__     = [user_name_field, email_field]
                __omit_fields__        = [password_field, 'created', '_password', '_groups']
                __hidden_fields__      = [user_id_field]
                __field_order__        = [user_name_field, email_field, display_name_field,
                                          'groups']

            if email_field is not None:
                setattr(NewForm, email_field, TextField)
            if display_name_field is not None:
                setattr(NewForm, display_name_field, TextField)
            self.new_form_type = NewForm

    class defaultCrudRestController(CrudRestController):
        @expose(inherit=True)
        def edit(self, *args, **kw):
            return CrudRestController.edit(self, *args, **kw)

        @expose()
        @registered_validate(error_handler=edit)
        def put(self, *args, **kw):
            """update"""
            if 'password' in kw and not kw['password']:
                del kw['password']

            pks = self.provider.get_primary_fields(self.model)
            for i, pk in enumerate(pks):
                if pk not in kw and i < len(args):
                    kw[pk] = args[i]

            self.provider.update(self.model, params=kw)
            redirect('../')


class GroupControllerConfig(CrudRestControllerConfig):
    def _do_init_with_translations(self, translations):
        group_id_field       = translations.get('group_id', 'group_id')
        group_name_field     = translations.get('group_name', 'group_name')

        class GroupTable(self.layout.TableBase):
            __model__ = self.model
            __limit_fields__ = [group_name_field, 'permissions']
            __url__ = '../groups.json'
        self.table_type = GroupTable

        class GroupTableFiller(self.layout.TableFiller):
            __model__ = self.model
            __limit_fields__ = [group_id_field, group_name_field, 'permissions']
        self.table_filler_type = GroupTableFiller

        class GroupNewForm(self.layout.AddRecordForm):
            __model__ = self.model
            __limit_fields__ = [group_name_field, 'permissions']
            __field_order__ = [group_name_field, 'permissions']
        self.new_form_type = GroupNewForm

        class GroupEditForm(self.layout.EditableForm):
            __model__ = self.model
            __limit_fields__ = [group_id_field, group_name_field, 'permissions']
            __field_order__ = [group_id_field, group_name_field, 'permissions']
        self.edit_form_type = GroupEditForm


class PermissionControllerConfig(CrudRestControllerConfig):
    def _do_init_with_translations(self, translations):
        permission_id_field              = translations.get('permission_id', 'permission_id')
        permission_name_field            = translations.get('permission_name', 'permission_name')
        permission_description_field     = translations.get('permission_description', 'description')

        class PermissionTable(self.layout.TableBase):
            __model__ = self.model
            __limit_fields__ = [permission_name_field, permission_description_field, 'groups']
            __url__ = '../permissions.json'
        self.table_type = PermissionTable

        class PermissionTableFiller(self.layout.TableFiller):
            __model__ = self.model
            __limit_fields__ = [permission_id_field, permission_name_field, permission_description_field, 'groups']
        self.table_filler_type = PermissionTableFiller

        class PermissionNewForm(self.layout.AddRecordForm):
            __model__ = self.model
            __limit_fields__ = [permission_name_field, permission_description_field, 'groups']
        self.new_form_type = PermissionNewForm

        class PermissionEditForm(self.layout.EditableForm):
            __model__ = self.model
            __limit_fields__ = [permission_name_field, permission_description_field,'groups']
        self.edit_form_type = PermissionEditForm

        class PermissionEditFiller(RecordFiller):
            __model__ = self.model
        self.edit_filler_type = PermissionEditFiller


class TGAdminConfig(AdminConfig):
    user       = UserControllerConfig
    group      = GroupControllerConfig
    permission = PermissionControllerConfig


class BootstrapTGAdminConfig(TGAdminConfig):
    layout     = BootstrapAdminLayout
