from .viewbase import ViewBase
from .metadata import FieldsMetadata
from .widgetselector import RecordViewWidgetSelector
from .widgets import RecordViewWidget

class RecordViewBase(ViewBase):
    """This class allows you to create a view for a single record.

    :Modifiers:

    see modifiers in :mod:`sprox.viewbase`

    Here is an example listing of the first user in the test database.

    >>> from sprox.test.base import User
    >>> from sprox.recordviewbase import RecordViewBase

    >>> class UserRecordView(RecordViewBase):
    ...     __model__ = User
    ...     __omit_fields__ = ['created']
    >>> user_view = UserRecordView(session)
    >>> from sprox.fillerbase import RecordFiller
    >>> class UserRecordFiller(RecordFiller):
    ...     __model__ = User
    >>> user_filler = UserRecordFiller(session)
    >>> value = user_filler.get_value({'user_id':1})
    >>> print(user_view(value=value))
    <table>
    <tr><th>Name</th><th>Value</th></tr>
    <tr class="recordfieldwidget" id="sx__password">
        <td>
            <b>_password</b>
        </td>
        <td>
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_user_id">
        <td>
            <b>user_id</b>
        </td>
        <td> 1
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_user_name">
        <td>
            <b>user_name</b>
        </td>
        <td> asdf
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_email_address">
        <td>
            <b>email_address</b>
        </td>
        <td> asdf@asdf.com
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_display_name">
        <td>
            <b>display_name</b>
        </td>
        <td>
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_town_id">
        <td>
            <b>town_id</b>
        </td>
        <td> 1
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_town">
        <td>
            <b>town</b>
        </td>
        <td> 1
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_password">
        <td>
            <b>password</b>
        </td>
        <td>
        </td>
    </tr>
    <tr class="recordfieldwidget" id="sx_groups">
        <td>
            <b>groups</b>
        </td>
        <td> 5
        </td>
    </tr>
    </table>

    """


    __metadata_type__         = FieldsMetadata
    __widget_selector_type__  = RecordViewWidgetSelector
    __base_widget_type__      = RecordViewWidget

    def _do_get_field_widget_args(self, field_name, field):
        """Override this method do define how this class gets the field
        widget arguemnts
        """
        args = super(RecordViewBase, self)._do_get_field_widget_args( field_name, field)
        args['field_name'] = field_name
        if self.__provider__.is_relation(self.__entity__, field_name):
            args['entity'] = self.__entity__
            args['field_name'] = field_name
        return args
