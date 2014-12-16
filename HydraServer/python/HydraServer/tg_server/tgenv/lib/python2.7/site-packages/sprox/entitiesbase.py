from sprox.widgets import ContainerWidget, TableWidget
from .viewbase import ViewBase
from .widgetselector import EntitiesViewWidgetSelector, EntityDefWidgetSelector
from sprox.metadata import EntitiesMetadata, FieldsMetadata

class EntityDefBase(ViewBase):
    """This view can display all of the entities for a given provider.

    :Modifiers:
     see :mod:`sprox.viewbase.ViewBase`

    :Usage:

    >>> from sprox.entitiesbase import EntityDefBase
    >>> class UserEntityDef(EntityDefBase):
    ...     __entity__ = User
    >>> base = UserEntityDef(session)
    >>> print(base())
    <table>
    <tr><th>Name</th><th>Definition</th></tr>
    <tr class="odd" id="sx__password">
        <td>
            password
        </td>
        <td>
            VARCHAR(40)
        </td>
    </tr>
    <tr class="even" id="sx_user_id">
        <td>
            user_id
        </td>
        <td>
            INTEGER
        </td>
    </tr>
    <tr class="odd" id="sx_user_name">
        <td>
            user_name
        </td>
        <td>
            VARCHAR(16)
        </td>
    </tr>
    <tr class="even" id="sx_email_address">
        <td>
            email_address
        </td>
        <td>
            VARCHAR(255)
        </td>
    </tr>
    <tr class="odd" id="sx_display_name">
        <td>
            display_name
        </td>
        <td>
            VARCHAR(255)
        </td>
    </tr>
    <tr class="even" id="sx_created">
        <td>
            created
        </td>
        <td>
            DATETIME
        </td>
    </tr>
    <tr class="odd" id="sx_town_id">
        <td>
            town_id
        </td>
        <td>
            INTEGER
        </td>
    </tr>
    <tr class="even" id="sx_town">
        <td>
            town
        </td>
        <td>
            relation
        </td>
    </tr>
    <tr class="odd" id="sx_password">
        <td>
            password
        </td>
        <td>
            relation
        </td>
    </tr>
    <tr class="even" id="sx_groups">
        <td>
            groups
        </td>
        <td>
            relation
        </td>
    </tr>
    </table>

    """

    __base_widget_type__       = TableWidget
    __widget_selector_type__   = EntityDefWidgetSelector
    __metadata_type__          = FieldsMetadata

from .dummyentity import DummyEntity

class EntitiesBase(ViewBase):
    """This view can display all of the entities for a given provider.

    :Modifiers:
     see :mod:`sprox.viewbase.ViewBase`

    :Usage:

    >>> from sprox.entitiesbase import EntitiesBase
    >>> class MyEntitiesBase(EntitiesBase):
    ...     __entity__ = User
    >>> base = MyEntitiesBase(session, metadata=metadata)
    >>> print(base())
    <div class="containerwidget">
        <div class="entitylabelwidget" id="sx_Department">
        <a href="Department/">Department</a>
    </div>
        <div class="entitylabelwidget" id="sx_Document">
        <a href="Document/">Document</a>
    </div>
        <div class="entitylabelwidget" id="sx_DocumentCategory">
        <a href="DocumentCategory/">DocumentCategory</a>
    </div>
        <div class="entitylabelwidget" id="sx_DocumentCategoryReference">
        <a href="DocumentCategoryReference/">DocumentCategoryReference</a>
    </div>
        <div class="entitylabelwidget" id="sx_DocumentCategoryTag">
        <a href="DocumentCategoryTag/">DocumentCategoryTag</a>
    </div>
        <div class="entitylabelwidget" id="sx_Example">
        <a href="Example/">Example</a>
    </div>
        <div class="entitylabelwidget" id="sx_File">
        <a href="File/">File</a>
    </div>
        <div class="entitylabelwidget" id="sx_Group">
        <a href="Group/">Group</a>
    </div>
        <div class="entitylabelwidget" id="sx_Permission">
        <a href="Permission/">Permission</a>
    </div>
        <div class="entitylabelwidget" id="sx_Town">
        <a href="Town/">Town</a>
    </div>
        <div class="entitylabelwidget" id="sx_User">
        <a href="User/">User</a>
    </div>
        <div class="entitylabelwidget" id="sx_WithoutName">
        <a href="WithoutName/">WithoutName</a>
    </div>
        <div class="entitylabelwidget" id="sx_WithoutNameOwner">
        <a href="WithoutNameOwner/">WithoutNameOwner</a>
    </div>
    </div>

    """
    __entity__ = DummyEntity
    __base_widget_type__       = ContainerWidget
    __widget_selector_type__   = EntitiesViewWidgetSelector
    __metadata_type__          = EntitiesMetadata

