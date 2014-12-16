"""
fillerbase Module

Classes to help fill widgets with data

Copyright (c) 2008 Christopher Perkins
Original Version by Christopher Perkins 2008
Released under MIT license.
"""

from sprox.fillerbase import TableFiller

class DojoTableFiller(TableFiller):

    def get_value(self, value=None, **kw):
        offset = kw.pop('start', None)
        limit  = kw.pop('count', None)
        order_by = kw.pop('sort', None)
        desc = False
        if order_by is not None and order_by.startswith('-'):
            order_by = order_by[1:]
            desc = True
        items = super(DojoTableFiller, self).get_value(value, limit=limit, offset=offset, order_by=order_by, desc=desc, **kw)
        count = self.get_count()
        identifier = self.__provider__.get_primary_field(self.__entity__)
        return dict(identifier=identifier, numRows=count, totalCount=len(items), items=items)

