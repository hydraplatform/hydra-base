#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from ..base import *
from .resourceattr import ResourceAttr

__all__ = ['Resource']

class Resource:
    def get_attributes(
            self,
            include_outputs=True,
            include_inputs=True
    ):
        """
        Get the resource attributes for this resource, with 
        args:
            include_outputs (bool): Include Resource Attributes tagged with attr_is_var='Y'
            include_inputs (bool): Include Resource Attributes tagged with attr_is_var='N'
        """

        ra_qry = get_session().query(ResourceAttr).filter(
            ResourceAttr.ref_id == self.id,
            ResourceAttr.ref_key == self.ref_key
        )

        if include_inputs is not True:
            ra_qry = ra_qry.filter(ResourceAttr.attr_is_var == 'N')
        
        if include_outputs is not True:
            ra_qry = ra_qry.filter(ResourceAttr.attr_is_var == 'Y')

        resource_attribtes = ra_qry.all()

        return resource_attribtes