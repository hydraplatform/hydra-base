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
            ResourceAttr.ref_key == self.ref_key
        )

        if self.ref_key == 'NETWORK':
            ra_qry = ra_qry.filter(ResourceAttr.network_id==self.id)
        elif self.ref_key == 'NODE':
            ra_qry = ra_qry.filter(ResourceAttr.node_id==self.id)
        elif self.ref_key == 'LINK':
            ra_qry = ra_qry.filter(ResourceAttr.link_id==self.id)
        elif self.ref_key == 'GROUP':
            ra_qry = ra_qry.filter(ResourceAttr.group_id==self.id)

        if include_inputs is False or include_outputs is False:
            if include_inputs is False:
                #only include outputs
                ra_qry = ra_qry.filter(ResourceAttr.attr_is_var == 'Y')
            
            if include_outputs is False:
                #only include inputs
                ra_qry = ra_qry.filter(ResourceAttr.attr_is_var == 'N')

        return ra_qry.all()