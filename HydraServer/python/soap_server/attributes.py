# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from spyne.model.primitive import Integer, Boolean, Unicode
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Attr
from hydra_complexmodels import Resource, ResourceAttr 

from hydra_base import HydraService

from lib import attributes

class AttributeService(HydraService):
    """
        The attribute SOAP service
    """

    @rpc(Attr, _returns=Attr)
    def add_attribute(ctx, attr):
        """
        Add a generic attribute, which can then be used in creating
        a resource attribute, and put into a type.

        .. code-block:: python

            (Attr){
                id = 1020
                name = "Test Attr"
                dimen = "very big"
            }

        """
        attr = attributes.add_attribute(attr, **ctx.in_header.__dict__)
        return Attr(attr)

    @rpc(SpyneArray(Attr), _returns=SpyneArray(Attr))
    def add_attributes(ctx, attrs):
        """
        Add a generic attribute, which can then be used in creating
        a resource attribute, and put into a type.

        .. code-block:: python

            (Attr){
                id = 1020
                name = "Test Attr"
                dimen = "very big"
            }

        """

        attrs = attributes.add_attributes(attrs, **ctx.in_header.__dict__)
        ret_attrs = [Attr(attr) for attr in attrs]
        return ret_attrs

    @rpc(_returns=SpyneArray(Attr))
    def get_attributes(ctx):
        """
            Get all attributes
        """

        attrs = attributes.get_attributes(**ctx.in_header.__dict__)
        ret_attrs = [Attr(attr) for attr in attrs]
        return ret_attrs

    @rpc(Integer, _returns=Attr)
    def get_attribute_by_id(ctx, ID):
        """
            Get a specific attribute by its ID.
        """
        attr = attributes.get_attribute_by_id(ID, **ctx.in_header.__dict__)

        if attr:
            return Attr(attr)

        return None

    @rpc(Unicode, Unicode, _returns=Attr)
    def get_attribute(ctx, name, dimension):
        """
            Get a specific attribute by its name.
        """
        attr = attributes.get_attribute_by_name_and_dimension(name,
                                                              dimension,
                                                              **ctx.in_header.__dict__)
        if attr:
            return Attr(attr)
        
        return None

    @rpc(Integer, _returns=Unicode)
    def delete_attribute(ctx, attr_id):
        """
            Set the status of an attribute to 'X'
        """
        success = 'OK'
        attributes.delete_attribute(attr_id, **ctx.in_header.__dict__)
        return success


    @rpc(Unicode, Integer, Integer, Boolean, _returns=Resource)
    def add_resource_attribute(ctx,resource_type, resource_id, attr_id, is_var):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        resource_attr_dict = attributes.add_resource_attribute(
                                                       resource_type,
                                                       resource_id,
                                                       attr_id,
                                                       is_var,
                                                       **ctx.in_header.__dict__)

        return ResourceAttr(resource_attr_dict)


    @rpc(Integer, Unicode, Integer, _returns=Resource)
    def add_node_attrs_from_type(ctx, type_id, resource_type, resource_id):
        """
            adds all the attributes defined by a type to a node.
        """
        resource_attr_dict = attributes.add_node_attrs_from_type(
                                                        type_id,
                                                        resource_type,
                                                        resource_id,
                                                        **ctx.in_header.__dict__)
        return ResourceAttr(resource_attr_dict)
