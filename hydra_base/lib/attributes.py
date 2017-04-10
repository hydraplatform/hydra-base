#dd_
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
import logging
log = logging.getLogger(__name__)

from HydraServer.db.model import Attr,\
        Node,\
        Link,\
        ResourceGroup,\
        Network,\
        Project,\
        Scenario,\
        TemplateType,\
        ResourceAttr,\
        TypeAttr,\
        ResourceAttrMap,\
        ResourceScenario,\
        Dataset
from HydraServer.db import DBSession
from sqlalchemy.orm.exc import NoResultFound
from HydraLib.HydraException import HydraError, ResourceNotFoundError
from sqlalchemy import or_, and_
from sqlalchemy.orm import aliased

def _get_resource(ref_key, ref_id):
    try:
        if ref_key == 'NODE':
            return DBSession.query(Node).filter(Node.node_id == ref_id).one()
        elif ref_key == 'LINK':
            return DBSession.query(Link).filter(Link.link_id == ref_id).one()
        if ref_key == 'GROUP':
            return DBSession.query(ResourceGroup).filter(ResourceGroup.group_id == ref_id).one()
        elif ref_key == 'NETWORK':
            return DBSession.query(Network).filter(Network.network_id == ref_id).one()
        elif ref_key == 'SCENARIO':
            return DBSession.query(Scenario).filter(Scenario.scenario_id == ref_id).one()
        elif ref_key == 'PROJECT':
            return DBSession.query(Project).filter(Project.project_id == ref_id).one()
        else:
            return None
    except NoResultFound:
        raise ResourceNotFoundError("Resource %s with ID %s not found"%(ref_key, ref_id))

def get_attribute_by_id(attr_id, **kwargs):
    """
        Get a specific attribute by its ID.
    """

    try:
        attr_i = DBSession.query(Attr).filter(Attr.attr_id==attr_id).one()
        return attr_i
    except NoResultFound:
        return None

def get_template_attributes(template_id, **kwargs):
    """
        Get a specific attribute by its ID.
    """

    try:
        attrs_i = DBSession.query(Attr).filter(TemplateType.template_id==template_id).filter(TypeAttr.type_id==TemplateType.type_id).filter(Attr.attr_id==TypeAttr.attr_id).all()
        log.info(attrs_i)
        return attrs_i
    except NoResultFound:
        return None



def get_attribute_by_name_and_dimension(name, dimension='dimensionless',**kwargs):
    """
        Get a specific attribute by its name.
    """

    try:
        attr_i = DBSession.query(Attr).filter(and_(Attr.attr_name==name, or_(Attr.attr_dimen==dimension, Attr.attr_dimen == ''))).one()
        log.info("Attribute retrieved")
        return attr_i
    except NoResultFound:
        return None

def add_attribute(attr,**kwargs):
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
    log.debug("Adding attribute: %s", attr.name)

    if attr.dimen is None or attr.dimen.lower() == 'dimensionless':
        log.info("Setting 'dimesionless' on attribute %s", attr.name)
        attr.dimen = 'dimensionless'

    try:
        attr_i = DBSession.query(Attr).filter(Attr.attr_name == attr.name,
                                              Attr.attr_dimen == attr.dimen).one()
        log.info("Attr already exists")
    except NoResultFound:
        attr_i = Attr(attr_name = attr.name, attr_dimen = attr.dimen)
        attr_i.attr_description = attr.description
        DBSession.add(attr_i)
        DBSession.flush()
        log.info("New attr added")
    return attr_i

def update_attribute(attr,**kwargs):
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

    if attr.dimen is None or attr.dimen.lower() == 'dimensionless':
        log.info("Setting 'dimesionless' on attribute %s", attr.name)
        attr.dimen = 'dimensionless'

    log.debug("Adding attribute: %s", attr.name)
    attr_i = _get_attr(Attr.attr_id)
    attr_i.attr_name = attr.name
    attr_i.attr_dimen = attr.dimension
    attr_i.attr_description = attr.description

    #Make sure an update hasn't caused an inconsistency.
    check_attr_dimension(attr_i.attr_id)

    DBSession.flush()
    return attr_i

def add_attributes(attrs,**kwargs):
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

    log.debug("Adding s: %s", [attr.name for attr in attrs])
    #Check to see if any of the attributs being added are already there.
    #If they are there already, don't add a new one. If an attribute
    #with the same name is there already but with a different dimension,
    #add a new attribute.

    all_attrs = DBSession.query(Attr).all()
    attr_dict = {}
    for attr in all_attrs:
        attr_dict[(attr.attr_name, attr.attr_dimen)] = attr

    attrs_to_add = []
    existing_attrs = []
    for potential_new_attr in attrs:
        if potential_new_attr.dimen is None or potential_new_attr.dimen.lower() == 'dimensionless':
            potential_new_attr.dimen = 'dimensionless'

        if attr_dict.get((potential_new_attr.name, potential_new_attr.dimen)) is None:
            attrs_to_add.append(potential_new_attr)
        else:
            existing_attrs.append(attr_dict.get((potential_new_attr.name, potential_new_attr.dimen)))

    new_attrs = []
    for attr in attrs_to_add:
        attr_i = Attr()
        attr_i.attr_name = attr.name
        attr_i.attr_dimen = attr.dimen
        attr_i.attr_description = attr.description
        DBSession.add(attr_i)
        new_attrs.append(attr_i)

    DBSession.flush()
    
    new_attrs = new_attrs + existing_attrs

    return new_attrs

def get_attributes(**kwargs):
    """
        Get all attributes
    """

    attrs = DBSession.query(Attr).all()

    return attrs

def _get_attr(attr_id):
    try:
        attr = DBSession.query(Attr).filter(Attr.attr_id == attr_id).one()
        return attr
    except NoResultFound:
        raise ResourceNotFoundError("Attribute with ID %s not found"%(attr_id,))

def _get_templatetype(type_id):
    try:
        typ = DBSession.query(TemplateType).filter(TemplateType.type_id == type_id).one()
        return typ
    except NoResultFound:
        raise ResourceNotFoundError("Template Type with ID %s not found"%(type_id,))

def update_resource_attribute(resource_attr_id, is_var, **kwargs):
    """
        Deletes a resource attribute and all associated data.
    """
    user_id = kwargs.get('user_id')
    try:
        ra = DBSession.query(ResourceAttr).filter(ResourceAttr.resource_attr_id == resource_attr_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Resource Attribute %s not found"%(resource_attr_id))

    ra.check_write_permission(user_id)

    ra.is_var = is_var

    return 'OK'

def delete_resource_attribute(resource_attr_id, **kwargs):
    """
        Deletes a resource attribute and all associated data.
    """
    user_id = kwargs.get('user_id')
    try:
        ra = DBSession.query(ResourceAttr).filter(ResourceAttr.resource_attr_id == resource_attr_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Resource Attribute %s not found"%(resource_attr_id))

    ra.check_write_permission(user_id)
    DBSession.delete(ra)
    DBSession.flush()
    return 'OK'

def add_resource_attribute(resource_type, resource_id, attr_id, is_var,**kwargs):
    """
        Add a resource attribute attribute to a resource.

        attr_is_var indicates whether the attribute is a variable or not --
        this is used in simulation to indicate that this value is expected
        to be filled in by the simulator.
    """

    attr = DBSession.query(Attr).filter(Attr.attr_id==attr_id).first()

    if attr is None:
        raise ResourceNotFoundError("Attribute with ID %s does not exist."%attr_id)

    resource_i = _get_resource(resource_type, resource_id)

    for ra in resource_i.attributes:
        if ra.attr_id == attr_id:
            raise HydraError("Duplicate attribute. %s %s already has attribute %s"
                             %(resource_type, resource_i.get_name(), attr.attr_name))

    attr_is_var = 'Y' if is_var else 'N'

    new_ra = resource_i.add_attribute(attr_id, attr_is_var)
    DBSession.flush()

    return new_ra

def add_resource_attrs_from_type(type_id, resource_type, resource_id,**kwargs):
    """
        adds all the attributes defined by a type to a node.
    """
    type_i = _get_templatetype(type_id)

    resource_i = _get_resource(resource_type, resource_id)

    attrs = {}
    for attr in resource_i.attributes:
        attrs[attr.attr_id] = attr

    new_resource_attrs = []
    for item in type_i.typeattrs:
        if attrs.get(item.attr_id) is None:
            ra = resource_i.add_attribute(item.attr_id)
            new_resource_attrs.append(ra)

    DBSession.flush()

    return new_resource_attrs

def get_all_resource_attributes(ref_key, network_id, template_id=None, **kwargs):
    """
        Get all the resource attributes for a given resource type in the network.
        That includes all the resource attributes for a given type within the network.
        For example, if the ref_key is 'NODE', then it will return all the attirbutes
        of all nodes in the network. This function allows a front end to pre-load an entire
        network's resource attribute information to reduce on function calls.
        If type_id is specified, only
        return the resource attributes within the type.
    """

    user_id = kwargs.get('user_id')
    
    resource_attr_qry = DBSession.query(ResourceAttr).\
            outerjoin(Node, Node.node_id==ResourceAttr.node_id).\
            outerjoin(Link, Link.link_id==ResourceAttr.link_id).\
            outerjoin(ResourceGroup, ResourceGroup.group_id==ResourceAttr.group_id).filter(
        ResourceAttr.ref_key == ref_key,
        or_(
            and_(ResourceAttr.node_id != None, 
                    ResourceAttr.node_id == Node.node_id,
                                        Node.network_id==network_id),

            and_(ResourceAttr.link_id != None,
                    ResourceAttr.link_id == Link.link_id,
                                        Link.network_id==network_id),

            and_(ResourceAttr.group_id != None,
                    ResourceAttr.group_id == ResourceGroup.group_id,
                                        ResourceGroup.network_id==network_id)
        ))

    if template_id is not None:
        attr_ids = []
        rs = DBSession.query(TypeAttr).join(TemplateType, 
                                            TemplateType.type_id==TypeAttr.type_id).filter(
                                                TemplateType.template_id==template_id).all()
        for r in rs:
            attr_ids.append(r.attr_id)

        resource_attr_qry = resource_attr_qry.filter(ResourceAttr.attr_id.in_(attr_ids))
    
    resource_attrs = resource_attr_qry.all()

    return resource_attrs

def get_resource_attributes(ref_key, ref_id, type_id=None, **kwargs):
    """
        Get all the resource attributes for a given resource. 
        If type_id is specified, only
        return the resource attributes within the type.
    """

    user_id = kwargs.get('user_id')
    
    resource_attr_qry = DBSession.query(ResourceAttr).filter(
        ResourceAttr.ref_key == ref_key,
        or_(
            ResourceAttr.network_id==ref_id,
            ResourceAttr.node_id==ref_id,
            ResourceAttr.link_id==ref_id,
            ResourceAttr.group_id==ref_id
        ))
     
    if type_id is not None:
        attr_ids = []
        rs = DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id).all()
        for r in rs:
            attr_ids.append(r.attr_id)

        resource_attr_qry = resource_attr_qry.filter(ResourceAttr.attr_id.in_(attr_ids))
    
    resource_attrs = resource_attr_qry.all()

    return resource_attrs

def check_attr_dimension(attr_id, **kwargs):
    """
        Check that the dimension of the resource attribute data is consistent
        with the definition of the attribute.
        If the attribute says 'volume', make sure every dataset connected
        with this attribute via a resource attribute also has a dimension
        of 'volume'.
    """
    attr_i = _get_attr(attr_id)

    datasets = DBSession.query(Dataset).filter(Dataset.dataset_id==ResourceScenario.dataset_id,
                                               ResourceScenario.resource_attr_id == ResourceAttr.resource_attr_id,
                                               ResourceAttr.attr_id == attr_id).all()
    bad_datasets = []
    for d in datasets:
        if d.data_dimen != attr_i.attr_dimen:
            bad_datasets.append(d.dataset_id)
   
    if len(bad_datasets) > 0:
        raise HydraError("Datasets %s have a different dimension to attribute %s"%(bad_datasets, attr_id))
    
    return 'OK'

def get_resource_attribute(resource_attr_id, **kwargs):
    """
        Get a specific resource attribte, by ID
        If type_id is Gspecified, only
        return the resource attributes within the type.
    """

    resource_attr_qry = DBSession.query(ResourceAttr).filter(
        ResourceAttr.resource_attr_id == resource_attr_id,
        )
     
    resource_attr = resource_attr_qry.first()

    if resource_attr is None:
        raise ResourceNotFoundError("Resource attribute %s does not exist", resource_attr_id)

    return resource_attr
    
def set_attribute_mapping(resource_attr_a, resource_attr_b, **kwargs):
    """
        Define one resource attribute from one network as being the same as
        that from another network.
    """
    user_id = kwargs.get('user_id')
    ra_1 = get_resource_attribute(resource_attr_a)
    ra_2 = get_resource_attribute(resource_attr_b)

    mapping = ResourceAttrMap(resource_attr_id_a = resource_attr_a,
                             resource_attr_id_b  = resource_attr_b,
                             network_a_id     = ra_1.get_network().network_id,
                             network_b_id     = ra_2.get_network().network_id )
    
    DBSession.add(mapping)

    return mapping

def delete_attribute_mapping(resource_attr_a, resource_attr_b, **kwargs):
    """
        Define one resource attribute from one network as being the same as
        that from another network.
    """
    user_id = kwargs.get('user_id')

    rm = aliased(ResourceAttrMap, name='rm')
    
    log.info("Trying to delete attribute map. %s -> %s", resource_attr_a, resource_attr_b)
    mapping = DBSession.query(rm).filter(
                             rm.resource_attr_id_a == resource_attr_a,
                             rm.resource_attr_id_b == resource_attr_b).first()

    if mapping is not None:
        log.info("Deleting attribute map. %s -> %s", resource_attr_a, resource_attr_b)
        DBSession.delete(mapping)
        DBSession.flush()

    return 'OK'

def delete_mappings_in_network(network_id, network_2_id=None, **kwargs):
    """
        Delete all the resource attribute mappings in a network. If another network
        is specified, only delete the mappings between the two networks.
    """
    qry = DBSession.query(ResourceAttrMap).filter(or_(ResourceAttrMap.network_a_id == network_id, ResourceAttrMap.network_b_id == network_id))

    if network_2_id is not None:
        qry = qry.filter(or_(ResourceAttrMap.network_a_id==network_2_id, ResourceAttrMap.network_b_id==network_2_id))

    mappings = qry.all()

    for m in mappings:
        DBSession.delete(m)
    DBSession.flush()

    return 'OK'

def get_mappings_in_network(network_id, network_2_id=None, **kwargs):
    """
        Get all the resource attribute mappings in a network. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = DBSession.query(ResourceAttrMap).filter(or_(ResourceAttrMap.network_a_id == network_id, ResourceAttrMap.network_b_id == network_id))

    if network_2_id is not None:
        qry = qry.filter(or_(ResourceAttrMap.network_a_id==network_2_id, ResourceAttrMap.network_b_id==network_2_id))

    return qry.all()

def get_node_mappings(node_id, node_2_id=None, **kwargs):
    """
        Get all the resource attribute mappings in a network. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = DBSession.query(ResourceAttrMap).filter(
        or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == ResourceAttr.resource_attr_id,
                ResourceAttr.node_id == node_id), 
            and_(
                ResourceAttrMap.resource_attr_id_b == ResourceAttr.resource_attr_id,
                ResourceAttr.node_id == node_id)))

    if node_2_id is not None:
        aliased_ra = aliased(ResourceAttr, name="ra2")
        qry = qry.filter(or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == aliased_ra.resource_attr_id,
                aliased_ra.node_id == node_2_id), 
            and_(
                ResourceAttrMap.resource_attr_id_b == aliased_ra.resource_attr_id,
                aliased_ra.node_id == node_2_id)))
    
    return qry.all()

def get_link_mappings(link_id, link_2_id=None, **kwargs):
    """
        Get all the resource attribute mappings in a network. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = DBSession.query(ResourceAttrMap).filter(
        or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == ResourceAttr.resource_attr_id,
                ResourceAttr.link_id == link_id), 
            and_(
                ResourceAttrMap.resource_attr_id_b == ResourceAttr.resource_attr_id,
                ResourceAttr.link_id == link_id)))

    if link_2_id is not None:
        aliased_ra = aliased(ResourceAttr, name="ra2")
        qry = qry.filter(or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == aliased_ra.resource_attr_id,
                aliased_ra.link_id == link_2_id), 
            and_(
                ResourceAttrMap.resource_attr_id_b == aliased_ra.resource_attr_id,
                aliased_ra.link_id == link_2_id)))
    
    return qry.all()


def get_network_mappings(network_id, network_2_id=None, **kwargs):
    """
        Get all the mappings of network resource attributes, NOT ALL THE MAPPINGS
        WITHIN A NETWORK. For that, ``use get_mappings_in_network``. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = DBSession.query(ResourceAttrMap).filter(
        or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == ResourceAttr.resource_attr_id,
                ResourceAttr.network_id == network_id), 
            and_(
                ResourceAttrMap.resource_attr_id_b == ResourceAttr.resource_attr_id,
                ResourceAttr.network_id == network_id)))

    if network_2_id is not None:
        aliased_ra = aliased(ResourceAttr, name="ra2")
        qry = qry.filter(or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == aliased_ra.resource_attr_id,
                aliased_ra.network_id == network_2_id), 
            and_(
                ResourceAttrMap.resource_attr_id_b == aliased_ra.resource_attr_id,
                aliased_ra.network_id == network_2_id)))
    
    return qry.all()

def check_attribute_mapping_exists(resource_attr_id_source, resource_attr_id_target, **kwargs):
    """
        Check whether an attribute mapping exists between a source and target resource attribute.
        returns 'Y' if a mapping exists. Returns 'N' in all other cases.
    """
    qry = DBSession.query(ResourceAttrMap).filter(
                ResourceAttrMap.resource_attr_id_a == resource_attr_id_source,
                ResourceAttrMap.resource_attr_id_b == resource_attr_id_target).all()

    if len(qry) > 0:
        return 'Y'
    else:
        return 'N'
