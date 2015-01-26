from tw.dojo import DojoJsonRestStore
from sprox.widgets.tw1widgets.dojo import SproxEditableDojoGrid, SproxDojoGrid
from sprox.tablebase import TableBase
from sprox.metadata import FieldsMetadata

class DojoTableBase(TableBase):
    """This class allows you to credate a table widget.

    :Modifiers:
      +-----------------------------------+--------------------------------------------+------------------------------+
      | Name                              | Description                                | Default                      |
      +===================================+============================================+==============================+
      | __url__                           | url that points to the method for data     | None                         |
      |                                   | filler for this table                      |                              |
      +-----------------------------------+--------------------------------------------+------------------------------+
      | __column_options__                | a pass-thru to the Dojo Table Widget       | {}                           |
      |                                   | column_options attribute                   |                              |
      +-----------------------------------+--------------------------------------------+------------------------------+
  
    also see modifiers in :mod:`sprox.tablebase`

    """

    #object overrides
    __base_widget_type__ = SproxDojoGrid
    __url__ = None
    __column_options__ = {}
    def _do_get_widget_args(self):
        args = super(DojoTableBase, self)._do_get_widget_args()

        if self.__url__ is not None:
            args['action'] = self.__url__
        args['columns'] = self.__fields__
        args['column_options'] = self.__column_options__
        args['headers'] = self.__headers__
        args['jsId'] = self.__sprox_id__
        return args

"""
Experimental for next version.  Will not be included in 0.5"""

class DojoEditableTableBase(TableBase):
    __base_widget_type__ = SproxEditableDojoGrid
    __url__ = None
    __column_options__ = {}

    def _do_get_widget_args(self):
        args = super(DojoEditableTableBase, self)._do_get_widget_args()
        if self.__url__ is not None:
            args['action'] = self.__url__
        args['columns'] = self.__fields__
        args['column_options'] = self.__column_options__
        args['headers'] = self.__headers__
        args['jsId'] = self.__sprox_id__
        return args

