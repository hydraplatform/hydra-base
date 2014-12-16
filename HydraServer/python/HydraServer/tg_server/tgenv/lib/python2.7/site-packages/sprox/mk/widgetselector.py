"""
widgetselecter Module

this contains the class which allows the ViewConfig to select the appropriate widget for the given field

Classes:
Name                               Description
MongoWidgetSelecter                Aid for the selection of a widget to correspond to a Mongo Document field

Exceptions:
None

Functions:
None


Copyright (c) 2009 Christopher Perkins
Original Version by Christopher Perkins 2009
Released under MIT license.

Mongo Contributions by Jorge Vargas
"""

from sprox.widgetselector import WidgetSelector
from sprox.widgets import *
from tw.forms import TextField, FileField

import datetime
import pymongo
class MongoKitWidgetSelector(WidgetSelector):
    #XXX this is a copy of authorized_types from monogokit/pylons/document
    default_widgets = {
#    type(None): 
    bool: SproxCheckBox,
    int: TextField,
    float: TextField,
    unicode: TextField,
#    list: 
#    dict:
    datetime.datetime: SproxCalendarDateTimePicker,
    pymongo.binary.Binary:FileField
#    pymongo.objectid.ObjectId:
#    pymongo.dbref.DBRef:
#    pymongo.code.Code:
#    type(re.compile("")):
#    MongoDocument:
    }
    def select(self,field):
        return self.default_widgets[field]

