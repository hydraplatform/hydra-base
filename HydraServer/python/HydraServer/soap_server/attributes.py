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
from hydra_complexmodels import ResourceAttrMap

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
                name = "Test Attr"
                dimen = "very big"
                description = "I am a very big attribute"
            }

        Args:
            attr (HydraServer.soap_server.hydra_complexmodels.Attr): An attribute object, as described above.

        Returns:
            hydra_complexmodels.Attr: An attribute object, similar to the one sent in but with an ID. 
        """

        attr = attributes.add_attribute(attr, **ctx.in_header.__dict__)
        return Attr(attr)

    @rpc(Attr, _returns=Attr)
    def update_attribute(ctx, attr):
        """
        Update a generic attribute, which can then be used in creating a resource attribute, and put into a type.

        .. code-block:: python

            (Attr){
                id = 1020
                name = "Test Attr"
                dimen = "very big"
                description = "I am a very big attribute"
            }

        Args:
            attr (HydraServer.soap_server.hydra_complexmodels.Attr): An attribute complex model, as described above.

        Returns:
            hydra_complexmodels.Attr: An attribute complex model, reflecting the one sent in. 

        """
        attr = attributes.update_attribute(attr, **ctx.in_header.__dict__)
        return Attr(attr)

    @rpc(SpyneArray(Attr), _returns=SpyneArray(Attr))
    def add_attributes(ctx, attrs):
        """
        Add multiple generic attributes

        Args:
            attrs (List[Attr]): A list of attribute complex models, 
                as described above.

        Returns:
            List[Attr]: A list of attribute complex models,
                reflecting the ones sent in. 

        """

        attrs = attributes.add_attributes(attrs, **ctx.in_header.__dict__)
        ret_attrs = [Attr(attr) for attr in attrs]
        return ret_attrs

    @rpc(_returns=SpyneArray(Attr))
    def get_all_attributes(ctx):
        """
        Get all the attributes in the system

        Args:
            None

        Returns:
            List[Attr]: A list of attribute complex models
        """

        attrs = attributes.get_attributes(**ctx.in_header.__dict__)
        ret_attrs = [Attr(attr) for attr in attrs]
        return ret_attrs

    @rpc(Integer, _returns=Attr)
    def get_attribute_by_id(ctx, ID):
        """
        Get a specific attribute by its ID.

        Args:
            ID (int): The ID of the attribute

        Returns:
            hydra_complexmodels.Attr: An attribute complex model.
                Returns None if no attribute is found.
        """
        attr = attributes.get_attribute_by_id(ID, **ctx.in_header.__dict__)

        if attr:
            return Attr(attr)

        return None

    @rpc(Unicode, Unicode, _returns=Attr)
    def get_attribute(ctx, name, dimension):
        """
        Get a specific attribute by its name and dimension (this combination
        is unique for attributes in Hydra Platform).

        Args:
            name (unicode): The name of the attribute
            dimension (unicode): The dimension of the attribute

        Returns:
            hydra_complexmodels.Attr: An attribute complex model.
                Returns None if no attribute is found.

        """
        attr = attributes.get_attribute_by_name_and_dimension(name,
                                                              dimension,
                                                              **ctx.in_header.__dict__)
        if attr:
            return Attr(attr)

        return None

    @rpc(Integer, _returns=SpyneArray(Attr))
    def get_template_attributes(ctx, template_id):
        """
            Get all the attributes in a template.
            Args

                param (int) template_id

            Returns

                List(Attr)
        """
        attrs = attributes.get_template_attributes(template_id,**ctx.in_header.__dict__)

        return [Attr(a) for a in attrs]

    @rpc(SpyneArray(Attr), _returns=SpyneArray(Attr))
    def get_attributes(ctx,attrs):
        """
        Get a list of attribute, by their names and dimension. Takes a list
            of attribute objects, picks out their name & dimension, finds the appropriate
            attribute in the DB and updates the incoming attribute with an ID.
            The same attributes go out that came in, except this time with an ID.
            If one of the incoming attributes does not match, this attribute is not
            returned.

        Args:
            attrs (List(Attr)): A list of attribute complex models

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.Attr): List of Attr complex models
        
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

    @rpc(Integer, Unicode(pattern="['YN']"), _returns=ResourceAttr)
    def update_resource_attribute(ctx, resource_attr_id, is_var):
        """
        Update a resource attribute (which means update the is_var flag
        as this is the only thing you can update on a resource attr)

        Args:
            resource_attr_id (int): ID of the complex model to be updated
            is_var           (unicode): 'Y' or 'N'

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): Updated ResourceAttr

        Raises:
            ResourceNotFoundError if the resource_attr_id is not in the DB
        """
        updated_ra = attributes.update_resource_attribute(resource_attr_id,
                                                          is_var,
                                                          **ctx.in_header.__dict__)
        return ResourceAttr(updated_ra)


    @rpc(Integer, _returns=Unicode)
    def delete_resourceattr(ctx, resource_attr_id):
        """
        Deletes a resource attribute and all associated data.
        ***WILL BE DEPRECATED***

        Args:
            resource_attr_id (int): ID of the complex model to be deleted 

        Returns:
            unicode: 'OK'

        Raises:
            ResourceNotFoundError if the resource_attr_id is not in the DB

        """
        attributes.delete_resource_attribute(resource_attr_id, **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer, _returns=Unicode)
    def delete_resource_attribute(ctx,resource_attr_id):
        """
        Add a resource attribute attribute to a resource (Duplicate of delete_resourceattr)

        Args:
            resource_attr_id (int): ID of the complex model to be deleted 

        Returns:
            unicode: 'OK'

        Raises:
            ResourceNotFoundError if the resource_attr_id is not in the DB

        """
        attributes.delete_resource_attribute(resource_attr_id,                                                                       **ctx.in_header.__dict__)

        return "OK"


    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_network_attribute(ctx,network_id, attr_id, is_var):
        """
        Add a resource attribute to a network.

        Args:
            network_id (int): ID of the network 
            attr_id    (int): ID of the attribute to assign to the network
            is_var     (string) 'Y' or 'N'. Indicates whether this attribute is
                a variable or not. (a variable is typically the result of a model run,
                so therefore doesn't need data assigned to it initially)

        Returns:
            HydraServer.soap_server.hydra_complexmodels.ResourceAttr: A complex model of the newly created resource attribute.
        Raises:
            ResourceNotFoundError: If the network or attribute are not in the DB.
            HydraError           : If the attribute is already on the network.

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
        Adds all the attributes defined by a type to a network.

        Args:
            type_id    (int): ID of the type used to get the resource attributes from
            network_id (int): ID of the network 

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the newly created network attributes

        Raises:
            ResourceNotFoundError if the type_id or network_id are not in the DB
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'NETWORK',
                                                        network_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_network_attributes(ctx, network_id, type_id):
        """
        Get all a network's attributes (not the attributes of the nodes and links. just the network itself).

        Args:
            network_id (int): ID of the network 
            type_id    (int) (optional): ID of the type. If specified will only return the resource attributes relative to that type

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the network attributes

        Raises:
            ResourceNotFoundError if the type_id or network_id are not in the DB

 
        """
        resource_attrs = attributes.get_resource_attributes(
                'NETWORK',
                network_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]


    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_node_attribute(ctx,node_id, attr_id, is_var):
        """
        Add a resource attribute to a node.

        Args:
            node_id (int): The ID of the Node
            attr_id (int): THe ID if the attribute being added.
            is_var (char): Y or N. Indicates whether the attribute is a variable or not.

        Returns:
            HydraServer.soap_server.hydra_complexmodels.ResourceAttr: The newly created node attribute

        Raises:
            ResourceNotFoundError: If the node or attribute do not exist
            HydraError: If this addition causes a duplicate attribute on the node.

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
        Adds all the attributes defined by a type to a node.

        Args:
            type_id (int): ID of the type used to get the resource attributes from
            node_id (int): ID of the node 

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the newly created node attributes

        Raises:
            ResourceNotFoundError if the type_id or node_id are not in the DB
 
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'NODE',
                                                        node_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_node_attributes(ctx, node_id, type_id):
        """
        Get all a node's attributes.

        Args:
            node_id (int): ID of the node 
            type_id (int) (optional): ID of the type. If specified will only return the resource attributes relative to that type

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the node's attributes

        Raises:
            ResourceNotFoundError if the type_id or node_id do not exist.
        """
 
        resource_attrs = attributes.get_resource_attributes(
                'NODE',
                node_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_all_node_attributes(ctx, network_id, template_id):
        """
        Get all the resource attributes for all the nodes in the network.

        Args:
            network_id (int): The ID of the network that you want the node attributes from
            template_id (int) (optional): If this is specified, then it will only return the attributes in this template.

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): The resource attributes of all the nodes in the network.
        """
        resource_attrs = attributes.get_all_resource_attributes(
                'NODE',
                network_id,
                template_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_link_attribute(ctx,link_id, attr_id, is_var):
        """
        Add a resource attribute to a link.

        Args:
            link_id (int): The ID of the Link
            attr_id (int): THe ID if the attribute being added.
            is_var (char): Y or N. Indicates whether the attribute is a variable or not.

        Returns:
            HydraServer.soap_server.hydra_complexmodels.ResourceAttr: The newly created link attribute

        Raises:
            ResourceNotFoundError: If the link or attribute do not exist
            HydraError: If this addition causes a duplicate attribute on the link.

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
        Adds all the attributes defined by a type to a link.

        Args:
            type_id (int): ID of the type used to get the resource attributes from
            link_id (int): ID of the link

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the newly created link attributes

        Raises:
            ResourceNotFoundError if the type_id or link_id are not in the DB
 
        """

        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'LINK',
                                                        link_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_link_attributes(ctx, link_id, type_id):
        """
        Get all a link's attributes.

        Args:
            link_id (int): ID of the link 
            type_id (int) (optional): ID of the type. If specified will only return the resource attributes relative to that type

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the link's attributes

        Raises:
            ResourceNotFoundError if the type_id or link_id do not exist.
        """

        resource_attrs = attributes.get_resource_attributes(
                'LINK',
                link_id,
                type_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_all_link_attributes(ctx, network_id, template_id):
        """
        Get all the resource attributes for all the links in the network.

        Args:
            network_id (int): The ID of the network that you want the link attributes from
            template_id (int) (optional): If this is specified, then it will only return the attributes in this template.

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): The resource attributes of all the links in the network.
        """

        resource_attrs = attributes.get_all_resource_attributes(
                'LINK',
                network_id,
                template_id)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer, Unicode(pattern="['YN']", default='N'), _returns=ResourceAttr)
    def add_group_attribute(ctx,group_id, attr_id, is_var):
        """
        Add a resource attribute to a group.

        Args:
            group_id (int): The ID of the Group
            attr_id (int): THe ID if the attribute being added.
            is_var (char): Y or N. Indicates whether the attribute is a variable or not.

        Returns:
            HydraServer.soap_server.hydra_complexmodels.ResourceAttr: The newly created group attribute

        Raises:
            ResourceNotFoundError: If the group or attribute do not exist
            HydraError: If this addition causes a duplicate attribute on the group.

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
        Adds all the attributes defined by a type to a group.

        Args:
            type_id (int): ID of the type used to get the resource attributes from
            group_id (int): ID of the group

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the newly created group attributes

        Raises:
            ResourceNotFoundError if the type_id or group_id are not in the DB
 
        """
        new_resource_attrs = attributes.add_resource_attrs_from_type(
                                                        type_id,
                                                        'GROUP',
                                                        group_id,
                                                        **ctx.in_header.__dict__)
        return [ResourceAttr(ra) for ra in new_resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_group_attributes(ctx, group_id, type_id):
        """
        Get all a group's attributes.

        Args:
            group_id (int): ID of the group 
            type_id (int) (optional): ID of the type. If specified will only return the resource attributes relative to that type

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): All the group's attributes

        Raises:
            ResourceNotFoundError if the type_id or group_id do not exist.
        """

        resource_attrs = attributes.get_resource_attributes(
                'GROUP',
                group_id,
                type_id,
                **ctx.in_header.__dict__)

        return [ResourceAttr(ra) for ra in resource_attrs]

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttr))
    def get_all_group_attributes(ctx, network_id, template_id):
        """
        Get all the resource attributes for all the groups in the network.

        Args:
            network_id (int): The ID of the network that you want the group attributes from
            template_id (int) (optional): If this is specified, then it will only return the attributes in this template.

        Returns:
            List(HydraServer.soap_server.hydra_complexmodels.ResourceAttr): The resource attributes of all the groups in the network.
        """


        resource_attrs = attributes.get_all_resource_attributes(
                'GROUP',
                network_id,
                template_id,
                **ctx.in_header.__dict__)

        return [ResourceAttr(ra) for ra in resource_attrs]


    @rpc(Integer, _returns=Unicode)
    def check_attr_dimension(ctx, attr_id):
        """
        Check that the dimension of the resource attribute data is consistent
        with the definition of the attribute.
        If the attribute says 'volume', make sure every dataset connected
        with this attribute via a resource attribute also has a dimension
        of 'volume'.
        
        Args:
            attr_id (int): The ID of the attribute you want to check

        Returns:
            string: 'OK' if all is well.

        Raises:
            HydraError: If a dimension mismatch is found.
        """

        attributes.check_attr_dimension(attr_id, **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer, Integer, _returns=Unicode)
    def set_attribute_mapping(ctx, resource_attr_a, resource_attr_b):
        """
        Define one resource attribute from one network as being the same as
        that from another network.

        Args:
            resource_attr_a (int): The ID of the source resoure attribute
            resource_attr_b (int): The ID of the target resoure attribute

        Returns:
            string: 'OK' if all is well.

        Raises:
            ResourceNotFoundError: If either resource attribute is not found.
        """
        attributes.set_attribute_mapping(resource_attr_a, resource_attr_b, **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer, Integer, _returns=Unicode)
    def delete_attribute_mapping(ctx, resource_attr_a, resource_attr_b):
        """
        Delete a mapping which said one resource attribute from one network 
        was the same as the resource attribute in another.

        Args:
            resource_attr_a (int): The ID of the source resoure attribute
            resource_attr_b (int): The ID of the target resoure attribute

        Returns:
            string: 'OK' if all is well. If the mapping isn't there, it'll still return 'OK', so make sure the IDs are correct! 

        """
        attributes.delete_attribute_mapping(resource_attr_a, resource_attr_b, **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttrMap))
    def get_mappings_in_network(ctx, network_id, network_2_id):
        """
        Get all the resource attribute mappings in a network (both from and to). If another network
        is specified, only return the mappings between the two networks.

        Args:
            network_id (int): The network you want to check the mappings of (both from and to)
            network_2_id (int) (optional): The partner network

        Returns:
            List(hydra_complexmodels.ResourceAttrMap): All the mappings to and from the network(s) in question.
        """
        mapping_rs = attributes.get_mappings_in_network(network_id, network_2_id, **ctx.in_header.__dict__)

        mappings = [ResourceAttrMap(m) for m in mapping_rs]
        return mappings

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=Unicode)
    def delete_mappings_in_network(ctx, network_id, network_2_id):
        """
        Delete all the resource attribute mappings in a network. If another network
        is specified, only delete the mappings between the two networks.

        Args:
            network_id (int): The network you want to delete the mappings from (both from and to)
            network_2_id (int) (optional): The partner network

        Returns:
            string: 'OK'
        """
        attributes.delete_mappings_in_network(network_id, network_2_id, **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceAttrMap))
    def get_node_mappings(ctx, node_id, node_2_id):
        """
        Get the mappings for all the attributes of a given node. If a second node
        is specified, return only the mappings between these nodes..

        Args:
            node_id (int): The node you want to delete the mappings from (both from and to)
            node_2_id (int) (optional): The partner node

        Returns:
            List(hydra_complexmodels.ResourceAttrMap): All the mappings to and from the node(s) in question.
        """
        mapping_rs = attributes.get_node_mappings(node_id, node_2_id, **ctx.in_header.__dict__)

        mappings = [ResourceAttrMap(m) for m in mapping_rs]
        return mappings


    @rpc(Integer, Integer, _returns=Unicode)
    def check_mapping_exists(ctx, resource_attr_id_source, resource_attr_id_target):
        """
        Check whether a mapping exists between two resource attributes
        This does not check whether a reverse mapping exists, so order is important here.

        Args:
            resource_attr_id_source (int): The source resource attribute 
            resource_attr_id_target (int) (optional): The target resource attribute

        Returns:
            string: 'Y' if a mapping between the source and target exists. 'N' in every other case.

        """
        is_mapped = attributes.check_attribute_mapping_exists(resource_attr_id_source, resource_attr_id_target,**ctx.in_header.__dict__)

        return is_mapped
