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
from spyne.model.primitive import Integer, Unicode
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Attr
from hydra_complexmodels import ResourceAttr 

from hydra_base import HydraService

from HydraServer.lib import attributes

import logging
log = logging.getLogger(__name__)

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
                description = "I am a very big attribute"
            }

        """
        attr = attributes.add_attribute(attr, **ctx.in_header.__dict__)
        return Attr(attr)

    @rpc(Attr, _returns=Attr)
    def update_attribute(ctx, attr):
        """
        Update a generic attribute, which can then be used in creating
        a resource attribute, and put into a type.

        .. code-block:: python

            (Attr){
                id = 1020
                name = "Test Attr"
                dimen = "very big"
                description = "I am a very big attribute"
            }

        """
        attr = attributes.update_attribute(attr, **ctx.in_header.__dict__)
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
                description = "I am a very big attribute"
            }

        """

        attrs = attributes.add_attributes(attrs, **ctx.in_header.__dict__)
        ret_attrs = [Attr(attr) for attr in attrs]
        return ret_attrs

    @rpc(_returns=SpyneArray(Attr))
    def get_all_attributes(ctx):
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

    @rpc(SpyneArray(Attr), _returns=SpyneArray(Attr))
    def get_attributes(ctx,attrs):
        """
            Get a list of attribute, by their names and dimension. Takes a list
            of attribute objects, picks out their name & dimension, finds the appropriate
            attribute in the DB and updates the incoming attribute with an ID.
            The same attributes go out that came in, except this time with an ID.
        """
        ret_attrs = []
        for a in attrs:
            log.info("Getting attribute %s, %s", a.name, a.dimen)
            attr = attributes.get_attribute_by_name_and_dimension(a.name,
                                                              a.dimen,
                                                              **ctx.in_header.__dict__)
            if attr:
                a.id = attr.attr_id
                a.cr_date = str(attr.cr_date)
                a.name = attr.attr_name
                a.dimen = attr.attr_dimen
                a.description = attr.attr_description
                ret_attrs.append(a)
            else:
                ret_attrs.append(None)
        
        return ret_attrs

    @rpc(Integer, Unicode, _returns=ResourceAttr)
    def update_resource_attribute(ctx, resource_attr_id, is_var):
        """
            Update the 'is_var' flag on a resource attribute
        """
        updated_ra = attributes.update_resource_attribute(resource_attr_id,
                                                          is_var,
                                                          **ctx.in_header.__dict__)
        return ResourceAttr(updated_ra)


    @rpc(Integer, _returns=Unicode)
    def delete_resourceattr(ctx, resource_attr_id):
        """
            Deletes a resource attribute and all associated data.
        """
        attributes.delete_resource_attribute(resource_attr_id, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=Unicode)
    def delete_resource_attribute(ctx,resource_attr_id):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        attributes.delete_resource_attribute(resource_attr_id,                                                                       **ctx.in_header.__dict__)

        return "OK" 


    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_network_attribute(ctx,network_id, attr_id, is_var):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        new_ra = attributes.add_resource_attribute(
                                                       'NETWORK',
                                                       network_id,
                                                       attr_id,
                                                       is_var,
                                                       **ctx.in_header.__dict__)

        return ResourceAttr(new_ra)


    @rpc(Integer, Integer, _returns=SpyneArray(ResourceAttr))
    def add_network_attrs_from_type(ctx, type_id, network_id):
        """
            adds all the attributes defined by a type to a node.
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'NETWORK',
                                                        network_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_network_attributes(ctx, network_id, type_id):
        resource_attrs = attributes.get_resource_attributes(
                'NETWORK',
                network_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]


    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_node_attribute(ctx,node_id, attr_id, is_var):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        new_ra = attributes.add_resource_attribute(
                                                       'NODE',
                                                       node_id,
                                                       attr_id,
                                                       is_var,
                                                       **ctx.in_header.__dict__)

        return ResourceAttr(new_ra)


    @rpc(Integer, Integer, _returns=SpyneArray(ResourceAttr))
    def add_node_attrs_from_type(ctx, type_id, node_id):
        """
            adds all the attributes defined by a type to a node.
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'NODE',
                                                        node_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_node_attributes(ctx, node_id, type_id):
        resource_attrs = attributes.get_resource_attributes(
                'NODE',
                node_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_link_attribute(ctx,link_id, attr_id, is_var):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        new_ra = attributes.add_resource_attribute(
                                                       'LINK',
                                                       link_id,
                                                       attr_id,
                                                       is_var,
                                                       **ctx.in_header.__dict__)

        return ResourceAttr(new_ra)


    @rpc(Integer, Integer, _returns=SpyneArray(ResourceAttr))
    def add_link_attrs_from_type(ctx, type_id, link_id):
        """
            adds all the attributes defined by a type to a link.
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'LINK',
                                                        link_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_link_attributes(ctx, link_id, type_id):
        resource_attrs = attributes.get_resource_attributes(
                'LINK',
                link_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_group_attribute(ctx,group_id, attr_id, is_var):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        new_ra = attributes.add_resource_attribute(
                                                       'GROUP',
                                                       group_id,
                                                       attr_id,
                                                       is_var,
                                                       **ctx.in_header.__dict__)

        return ResourceAttr(new_ra)


    @rpc(Integer, Integer, _returns=SpyneArray(ResourceAttr))
    def add_group_attrs_from_type(ctx, type_id, group_id):
        """
            adds all the attributes defined by a type to a group.
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'GROUP',
                                                        group_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_group_attributes(ctx, group_id, type_id):
        resource_attrs = attributes.get_resource_attributes(
                'GROUP',
                group_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, _returns=Unicode)
    def check_attr_dimension(ctx, attr_id):
        """
            Check that the dimension of the resource attribute data is consistent
            with the definition of the attribute.
            If the attribute says 'volume', make sure every dataset connected
            with this attribute via a resource attribute also has a dimension
            of 'volume'.
        """

        attributes.check_attr_dimension(attr_id, **ctx.in_header.__dict__)

        return 'OK'
