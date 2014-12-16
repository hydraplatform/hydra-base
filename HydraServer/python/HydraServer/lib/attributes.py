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
import logging
log = logging.getLogger(__name__)

from HydraServer.db.model import Attr, Node, Link, Network, Project, Scenario, TemplateType
from HydraServer.db import DBSession
from sqlalchemy.orm.exc import NoResultFound
from HydraLib.HydraException import ResourceNotFoundError
from sqlalchemy import or_

def _get_resource(ref_key, ref_id):
    try:
        if ref_key == 'NODE':
            return DBSession.query(Node).filter(Node.node_id == ref_id).one()
        elif ref_key == 'LINK':
            return DBSession.query(Link).filter(Link.link_id == ref_id).one()
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

def get_attribute_by_name_and_dimension(name, dimension,**kwargs):
    """
        Get a specific attribute by its name.
    """

    try:
        attr_i = DBSession.query(Attr).filter(Attr.attr_name==name, or_(Attr.attr_dimen==dimension, Attr.attr_dimen == '')).one()
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
    try:
        attr_i = DBSession.query(Attr).filter(Attr.attr_name == attr.name,
                                              Attr.attr_dimen == attr.dimen).one()
        log.info("Attr already exists")
    except NoResultFound:
        attr_i = Attr(attr_name = attr.name, attr_dimen = attr.dimen)
        DBSession.add(attr_i)
        DBSession.flush()
        log.info("New attr added")
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
        if attr_dict.get((potential_new_attr.name, potential_new_attr.dimen)) is None:
            attrs_to_add.append(potential_new_attr)
        else:
            existing_attrs.append(attr_dict.get((potential_new_attr.name, potential_new_attr.dimen)))
    new_attrs = []
    for attr in attrs_to_add:
        attr_i = Attr()
        attr_i.attr_name = attr.name
        attr_i.attr_dimen = attr.dimen
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



def delete_attribute(attr_id,**kwargs):
    """
        Set the status of an attribute to 'X'
    """
    success = True
    x = DBSession.query(Attr).filter(Attr.attr_id == attr_id).one()
    x.status = 'X'
    DBSession.flush()
    return success


def add_resource_attribute(resource_type, resource_id, attr_id, is_var,**kwargs):
    """
        Add a resource attribute attribute to a resource.

        attr_is_var indicates whether the attribute is a variable or not --
        this is used in simulation to indicate that this value is expected
        to be filled in by the simulator.
    """
    resource_i = _get_resource(resource_type, resource_id)

    attr_is_var = 'Y' if is_var else 'N'

    resource_i.add_attribute(attr_id, attr_is_var)
    DBSession.flush()

    return resource_i


def add_node_attrs_from_type(type_id, resource_type, resource_id,**kwargs):
    """
        adds all the attributes defined by a type to a node.
    """
    type_i = _get_templatetype(type_id)

    resource_i = _get_resource(resource_type, resource_id)

    attrs = {}
    for attr in resource_i.attributes:
        attrs[attr.attr_id] = attr


    for item in type_i.typeattrs:
        if attrs.get(item.attr_id) is None:
            resource_i.add_attribute(item.attr_id)

    DBSession.flush()

    return resource_i
