# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2020 University of Manchester
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

import json
import logging
import re
from decimal import Decimal

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import noload, joinedload
from sqlalchemy import or_, and_

from hydra_base import db
from hydra_base.db.model import (Template, TemplateOwner, TemplateType, TypeAttr, Attr)
from hydra_base.lib.objects import JSONObject, Dataset
from hydra_base.lib.data import add_dataset
from hydra_base.exceptions import HydraError, ResourceNotFoundError
from hydra_base import config
from hydra_base.util import dataset_util, get_json_as_string, get_json_as_dict
from hydra_base.lib import units
from hydra_base.util.permissions import required_perms

from hydra_base.lib.template.utils import check_dimension, get_attr_by_name_and_dimension

from hydra_base.lib.template.xml import (get_template_as_xml, import_template_xml,
    get_network_as_xml_template)

from hydra_base.lib.template.resource import (get_types_by_attr,
    apply_template_to_network,
    set_network_template,
    remove_template_from_network,
    get_matching_resource_types, assign_types_to_resources,
    check_type_compatibility,
    assign_type_to_resource,
    set_resource_type,
    get_network_template,
    remove_type_from_resource,
    validate_attr,
    validate_attrs,
    validate_scenario,
    validate_resource,
    validate_resourcescenario,
    validate_network)

log = logging.getLogger(__name__)

def parse_json_typeattr(type_i, typeattr_j, attribute_j, default_dataset_j, user_id=None):
    dimension_i = None
    if attribute_j.dimension_id is not None:
        # The dimension_id of the attribute is not None
        dimension_i = units.get_dimension(attribute_j.dimension_id)
    elif attribute_j.dimension is not None:
        # The dimension name of the attribute is not None
        dimension_name = attribute_j.dimension.strip()
        if dimension_name.lower() in ('dimensionless', ''):
            dimension_name = 'dimensionless'
        dimension_i = units.get_dimension_by_name(dimension_name.strip())
    elif attribute_j.unit_id is not None:
        # The unit_id of the attribute is not None
        dimension_i = units.get_dimension_by_unit_id(attribute_j.unit_id)
    elif attribute_j.unit not in ('', None):
        # The unit of the attribute is not None
        attribute_unit_id = units.get_unit_by_abbreviation(attribute_j.unit).id
        attribute_j.unit_id = attribute_unit_id
        dimension_i = units.get_dimension_by_unit_id(attribute_j.unit_id)

    attribute_name = attribute_j.name.strip()

    if dimension_i is None:
        # In this case we must get the attr with dimension id not set
        attr_i = get_attr_by_name_and_dimension(attribute_name, None)
    else:
        attr_i = get_attr_by_name_and_dimension(attribute_name, dimension_i.id)

    #Get an ID for the attribute
    db.DBSession.flush()

    for ta in type_i.typeattrs:
        if ta.attr_id == attr_i.id:
           typeattr_i = ta
           break
    else:
        typeattr_i = TypeAttr()
        log.debug("Creating type attr: type_id=%s, attr_id=%s", type_i.id, attr_i.id)
        typeattr_i.type_id = type_i.id
        typeattr_i.attr_id = attr_i.id
        typeattr_i.attr_is_var = typeattr_j.attr_is_var
        typeattr_i.attr = attr_i
        typeattr_i.status = 'A'
        type_i.typeattrs.append(typeattr_i)
        db.DBSession.add(typeattr_i)


    unit_id = None
    if attribute_j.unit_id is not None:
        typeattr_i.unit_id = typeattr_j.unit_id

    check_dimension(typeattr_i)

    if typeattr_j.description is not None:
        typeattr_i.description = typeattr_j.description

    if typeattr_j.properties is not None:
        if isinstance(typeattr_j.properties, dict):
            typeattr_i.properties = json.dumps(typeattr_j.properties)
        else:
            typeattr_i.properties = typeattr_j.properties

    if typeattr_j.is_var is not None:
        typeattr_i.attr_is_var = typeattr_j.is_var

    if typeattr_j.data_type is not None:
        typeattr_i.data_type = typeattr_j.data_type

    if default_dataset_j is not None:
        default = default_dataset_j

        unit = default.unit
        unit_id = None
        if unit not in (None, ''):
            unit_id = units.get_unit_by_abbreviation(unit).id

        if unit_id is None and typeattr_i.unit_id is not None:
            unit_id = typeattr_i.unit_id

        if unit_id is not None:
            check_dimension(typeattr_i, unit_id)

        if unit_id is not None and typeattr_i.unit_id is not None:
            if unit_id != typeattr_i.unit_id:
                raise HydraError("Default value has a unit of %s but the attribute"
                             " says the unit should be: %s"%(typeattr_i.unit_id, unit_id))

        val  = default.value

        data_type = default.type
        name = default.name if default.name not in (None, '') else "%s Default"%attr_i.name

        dataset_i = add_dataset(data_type,
                               val,
                               unit_id,
                               name= name,
                               user_id=user_id)
        typeattr_i.default_dataset_id = dataset_i.id


    if typeattr_j.restriction is not None or typeattr_j.data_restriction is not None:
        restriction = typeattr_j.restriction if typeattr_j.restriction is not None else typeattr_j.data_restriction
        if isinstance(restriction, dict):
            typeattr_i.data_restriction = json.dumps(restriction)
        else:
            typeattr_i.data_restriction = restriction
    else:
        typeattr_i.data_restriction = None

    return typeattr_i

@required_perms("get_template")
def get_template_as_json(template_id, **kwargs):
    """
        Get a template (including attribute and dataset definitions) as a JSON
        string. This is just a wrapper around the get_template_as_dict function.
    """
    user_id = kwargs['user_id']
    return json.dumps(get_template_as_dict(template_id, user_id=user_id))

@required_perms("get_template")
def get_template_as_dict(template_id, **kwargs):
    attr_dict = {}
    dataset_dict = {}

    template_i = db.DBSession.query(Template).filter(
                    Template.id==template_id).options(
                        joinedload('templatetypes')\
                        .joinedload('typeattrs')\
                        .joinedload('default_dataset')\
                        .joinedload('metadata')
                    ).one()



    #Load all the attributes
    for type_i in template_i.templatetypes:
        for typeattr_i in type_i.typeattrs:
            typeattr_i.attr

    template_j = JSONObject(template_i)

    for tmpltype_j in template_j.templatetypes:
        ##Try to load the json into an object, as it will be re-encoded as json,
        ##and we don't want double encoding:
        if tmpltype_j.layout is not None:
            tmpltype_j.layout = get_json_as_dict(tmpltype_j.layout)

        for typeattr_j in tmpltype_j.typeattrs:
            typeattr_j.attr_id = str(typeattr_j.attr_id*-1)
            attr_dict[typeattr_j.attr_id] = JSONObject(
                                   {'name': typeattr_j.attr.name,
                                    'dimension_id':typeattr_j.attr.dimension_id
                                    })

            if typeattr_j.default_dataset_id is not None:
                typeattr_j.default_dataset_id = str(typeattr_j.default_dataset_id * -1)
                dataset_dict[typeattr_j.default_dataset_id] = JSONObject(
                            {
                                'name'     : typeattr_j.default_dataset.name,
                                'type'     : typeattr_j.default_dataset.type,
                                'unit_id'  : typeattr_j.default_dataset.unit_id,
                                'value'    : typeattr_j.default_dataset.value,
                                'metadata' : typeattr_j.default_dataset.metadata
                            })

            if hasattr(typeattr_j, 'default_dataset') and typeattr_j.default_dataset is not None:
                del(typeattr_j['default_dataset'])

            if hasattr(typeattr_j, 'attr') and typeattr_j.attr is not None:
                del(typeattr_j['attr'])

    output_data = {'attributes': attr_dict, 'datasets':dataset_dict, 'template': template_j}

    return output_data

@required_perms("add_template")
def import_template_json(template_json_string, allow_update=True, **kwargs):
    """
        Add the template, type and typeattrs described in a JSON file.

        Delete type, typeattr entries in the DB that are not in the JSON file.
        The assumption is that they have been deleted and are no longer required.

        The allow_update indicates whether an existing template of the same name should
        be updated, or whether it should throw an 'existing name' error.
    """

    user_id = kwargs.get('user_id')

    try:
        template_dict = json.loads(template_json_string)
    except:
        raise HydraError("Unable to parse JSON string. Plese ensure it is JSON compatible.")

    return import_template_dict(template_dict, allow_update=allow_update, user_id=user_id)

@required_perms("add_template")
def import_template_dict(template_dict, allow_update=True, **kwargs):

    user_id = kwargs.get('user_id')

    template_file_j = template_dict

    file_attributes = template_file_j.get('attributes')
    file_datasets   = template_file_j.get('datasets', {})
    template_j = JSONObject(template_file_j.get('template', {}))

    #default datasets are optional, so don't force them to exist in the structure
    default_datasets_j = {}
    for k, v in file_datasets.items():
        default_datasets_j[int(k)] = Dataset(v)

    if file_attributes is None or len(template_j) == 0:
        raise HydraError("Invalid template. The template must have the following structure: " + \
                            "{'attributes':\\{...\\}, 'datasets':\\{...\\}, 'template':\\{...\\}}")


    #Normalise attribute IDs so they're always ints (in case they're specified as strings)
    attributes_j = {}
    for k, v in file_attributes.items():
        attributes_j[int(k)] = JSONObject(v)

    template_name = template_j.name
    template_description = template_j.description

    template_layout = None
    if template_j.layout is not None:
        if isinstance(template_j.layout, dict):
            template_layout = json.dumps(template_j.layout)
        else:
            template_layout = template_j.layout

    try:
        template_i = db.DBSession.query(Template).filter(
            Template.name==template_name).options(
                joinedload('templatetypes')
                .joinedload('typeattrs')
                .joinedload('attr')).one()
        if allow_update == False:
            raise HydraError("Existing Template Found with name %s"%(template_name,))
        else:
            template_i.layout = template_layout
            template_i.description = template_description
    except NoResultFound:
        log.debug("Template not found. Creating new one. name=%s", template_name)
        template_i = Template(name=template_name, description=template_description, layout=template_layout)
        db.DBSession.add(template_i)

    types_j = template_j.templatetypes
    type_id_map = {r.id:r for r in template_i.templatetypes}
    #Delete any types which are in the DB but no longer in the JSON file
    type_name_map = {r.name:r.id for r in template_i.templatetypes}
    attr_name_map = {}
    for type_i in template_i.templatetypes:
        for typeattr in type_i.typeattrs:
            attr_name_map[typeattr.attr.name] = (typeattr.attr_id, typeattr.type_id)

    existing_types = set([r.name for r in template_i.templatetypes])
    log.debug(["%s : %s" %(tt.name, tt.id) for tt in template_i.templatetypes])
    log.debug("Existing types: %s", existing_types)

    new_types = set([t.name for t in types_j])
    log.debug("New Types: %s", new_types)

    types_to_delete = existing_types - new_types
    log.debug("Types to delete: %s", types_to_delete)
    log.debug(type_name_map)
    for type_to_delete in types_to_delete:
        type_id = type_name_map[type_to_delete]
        try:
            for i, tt in enumerate(template_i.templatetypes):
                if tt.id == type_id:
                    type_i = template_i.templatetypes[i]

                    #first remove all the type attributes associated to the type
                    for ta_i in type_i.typeattrs:
                        db.DBSession.delete(ta_i)

                    del(template_i.templatetypes[i])
                    log.debug("Deleting type %s (%s)", type_i.name, type_i.id)
                    del(type_name_map[type_to_delete])
                    db.DBSession.delete(type_i)
        except NoResultFound:
            pass


    #Add or update types.
    for type_j in types_j:
        type_name = type_j.name

        #check if the type is already in the DB. If not, create a new one.
        type_is_new = False
        if type_name in existing_types:
            type_id = type_name_map[type_name]
            type_i = type_id_map[type_id]
        else:
            log.debug("Type %s not found, creating new one.", type_name)
            type_i = TemplateType()
            type_i.name = type_name
            template_i.templatetypes.append(type_i)
            type_i.status = 'A' ## defaults to active
            type_is_new = True

        if type_j.description is not None:
            type_i.description = type_j.description

        if type_j.alias is not None:
            type_i.alias = type_j.alias

        #Allow 'type' or 'resource_type' to be accepted
        if type_j.type is not None:
            type_i.resource_type = type_j.type
        elif type_j.resource_type is not None:
            type_i.resource_type = type_j.resource_type

        if type_j.resource_type is None:
            raise HydraError("No resource type specified."
                             " 'NODE', 'LINK', 'GROUP' or 'NETWORK'")

        if type_j.layout is not None:
            if isinstance(type_j, dict):
                type_i.layout = json.dumps(type_j.layout)
            else:
                type_i.layout = type_j.layout

        #delete any TypeAttrs which are in the DB but not in the JSON file
        existing_attrs = []
        if not type_is_new:
            for r in template_i.templatetypes:
                if r.name == type_name:
                    for typeattr in r.typeattrs:
                        existing_attrs.append(typeattr.attr.name)

        existing_attrs = set(existing_attrs)

        type_attrs = []
        for typeattr_j in type_j.typeattrs:
            if typeattr_j.attr_id is not None:
                attr_j = attributes_j[typeattr_j.attr_id].name
            elif typeattr_j.attr is not None:
                attr_j = typeattr_j.attr.name
            type_attrs.append(attr_j)

        type_attrs = set(type_attrs)

        attrs_to_delete = existing_attrs - type_attrs
        for attr_to_delete in attrs_to_delete:
            attr_id, type_id = attr_name_map[attr_to_delete]
            try:
                attr_i = db.DBSession.query(TypeAttr).filter(TypeAttr.attr_id==attr_id, TypeAttr.type_id==type_id).options(joinedload('attr')).one()
                db.DBSession.delete(attr_i)
                log.debug("Attr %s in type %s deleted",attr_i.attr.name, attr_i.templatetype.name)
            except NoResultFound:
                log.debug("Attr %s not found in type %s"%(attr_id, type_id))
                continue

        #Add or update type typeattrs
        #Support an external attribute dict or embedded attributes.
        for typeattr_j  in type_j.typeattrs:
            if typeattr_j.attr_id is not None:
                attr_j = attributes_j[typeattr_j.attr_id]
            elif typeattr_j.attr is not None:
                attr_j = typeattr_j.attr

            default_dataset_j = None
            if typeattr_j.default_dataset is not None:
                default_dataset_j = typeattr_j.default_dataset
            elif typeattr_j.default is not None: # for backward compatibility
                default_dataset_j = typeattr_j.default
            elif typeattr_j.default_dataset_id is not None:
                default_dataset_j = default_datasets_j[int(typeattr_j.default_dataset_id)]

            parse_json_typeattr(type_i, typeattr_j, attr_j, default_dataset_j, user_id=user_id)

    db.DBSession.flush()

    return template_i

def _parse_data_restriction(restriction_dict):
    if restriction_dict is None or len(restriction_dict) == 0:
        return None

    #replace soap text with an empty string
    #'{soap_server.hydra_complexmodels}' -> ''
    dict_str = re.sub('{[a-zA-Z._]*}', '', str(restriction_dict))

    if isinstance(restriction_dict, dict):
        new_dict = restriction_dict
    else:
        try:
            new_dict = json.loads(restriction_dict)
        except:
            raise HydraError(f"Unable to parse the JSON in the restriction data: {restriction_dict}")

    #Evaluate whether the dict actually contains anything.
    if not isinstance(new_dict, dict) or len(new_dict) == 0:
        log.critical('A restriction was specified, but it is null')
        return None

    ret_dict = {}
    for k, v in new_dict.items():
        if (isinstance(v, str) or isinstance(v, list)) and len(v) == 1:
            ret_dict[k] = v[0]
        else:
            ret_dict[k] = v

    return json.dumps(ret_dict)

@required_perms("add_template")
def add_template(template, **kwargs):
    """
        Add template and a type and typeattrs.
    """
    tmpl = Template()
    tmpl.name = template.name
    if template.parent_id:
        tmpl.parent_id = template.parent_id
    if template.description:
        tmpl.description = template.description
    if template.layout:
        tmpl.layout = get_json_as_string(template.layout)

    db.DBSession.add(tmpl)

    if template.templatetypes is not None:
        types = template.templatetypes
        for templatetype in types:
            ttype = _update_templatetype(templatetype, **kwargs)
            tmpl.templatetypes.append(ttype)

    db.DBSession.flush()

    log.info("[Added template]\n{}".format(template))

    return tmpl


def _get_network_type(template_id):
    """find the network type in a template"""

    templatetypes_i = db.DBSession.query(TemplateType).filter(TemplateType.template_id == template_id).all()

    for templatetype in templatetypes_i:
        if templatetype.resource_type == 'NETWORK':
            return templatetype
    return None

def add_child_template(parent_id, name, description=None, **kwargs):
    """
        Add template and a type and typeattrs.
    """
    parent_template = db.DBSession.query(Template).filter(Template.id == parent_id).one()

    tmpl = Template()
    tmpl.name = name
    if description is not None:
        tmpl.description = description
    else:
        tmpl.description = parent_template.description
    tmpl.layout = parent_template.layout
    tmpl.parent_id = parent_id

    #now add a default network type, but only if the parent has one defined
    parent_type = _get_network_type(parent_id)

    if parent_type is not None:

        network_type = TemplateType()

        network_type.name = "{}-network".format(tmpl.name)
        network_type.resource_type = 'NETWORK'
        network_type.parent_id = parent_type.id
        network_type.status = 'A'

        tmpl.templatetypes.append(network_type)
    else:
        log.warn("Unable to set a network type on this template as its "
                 "parent %s does not contain one, so cannot create a "
                 "parent-child relationship.", parent_id)

    db.DBSession.add(tmpl)

    db.DBSession.flush()

    return tmpl

def _set_template_status(template_id, status, **kwargs):
    """
        Set the status of a template to the specified status.
        These can be 'A' or 'X'
    """
    tmpl = db.DBSession.query(Template).filter(Template.id == template_id).one()

    tmpl.status = status

    db.DBSession.flush()

@required_perms("edit_template")
def activate_template(template_id, **kwargs):
    """
        Set the status of a template to active
    """

    _set_template_status(template_id, 'A')

@required_perms("edit_template")
def deactivate_template(template_id, **kwargs):
    """
        Set the status of a template to inactive
    """

    _set_template_status(template_id, 'X')

@required_perms("edit_template")
def update_template(template, auto_delete=False, **kwargs):
    """
        Update template and a type and typeattrs.
        args:
            template (JSONObject): The template to update
            auto_delete (bool): A flag to indicated whether missing types from `template.templatetypes`
                                should be deleted automatically. This flag is also
                                used when updating the typeattrs of type. Defaults to False.
    """
    tmpl = db.DBSession.query(Template).filter(Template.id == template.id).one()
    tmpl.name = template.name

    if template.status is not None:
        tmpl.status = template.status

    if template.description:
        tmpl.description = template.description

    template_types = tmpl.get_types()

    if template.layout:
        tmpl.layout = get_json_as_string(template.layout)

    type_dict = dict([(t.id, t) for t in template_types])

    #a list of all the templatetypes in the incoming template object
    req_templatetype_ids = []

    if template.types is not None or template.templatetypes is not None:
        types = template.types if template.types is not None else template.templatetypes
        for templatetype in types:

            if templatetype.id is not None and templatetype.template_id != tmpl.id:
                log.debug("Type %s is a part of a parent template. Ignoring.", templatetype.id)
                req_templatetype_ids.append(type_i.id)
                continue

            if templatetype.id is not None:
                type_i = type_dict[templatetype.id]
                _update_templatetype(templatetype, auto_delete=auto_delete, **kwargs)
                req_templatetype_ids.append(type_i.id)
            else:
                #Give it a template ID if it doesn't have one
                templatetype.template_id = template.id
                new_templatetype_i = _update_templatetype(templatetype, auto_delete=auto_delete, **kwargs)
                req_templatetype_ids.append(new_templatetype_i.id)

    if auto_delete is True:
        for ttype in template_types:
            if ttype.id not in req_templatetype_ids:
                delete_templatetype(ttype.id, **kwargs)

    db.DBSession.flush()

    updated_templatetypes = tmpl.get_types()

    tmpl_j = JSONObject(tmpl)

    tmpl_j.templatetypes = updated_templatetypes

    #db.DBSession.expunge(tmpl)

    return tmpl_j

@required_perms("delete_template")
def delete_template(template_id, delete_resourcetypes=False, **kwargs):
    """
        Delete a template and its type and typeattrs.
        The 'delete_resourcetypes' flag forces the template to remove any resource types
        associated to the types in the network. Use with caution!
    """
    try:
        tmpl = db.DBSession.query(Template).filter(Template.id == template_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Template %s not found"%(template_id,))

    for templatetype in tmpl.templatetypes:
        delete_templatetype(templatetype.id, flush=False, delete_resourcetypes=delete_resourcetypes, user_id=kwargs.get('user_id'))

    db.DBSession.delete(tmpl)
    db.DBSession.flush()
    return 'OK'


def _set_template_owners(templates_i):
    flush = False
    for tmpl_i in templates_i:
        if not tmpl_i.owners:
            owner = tmpl_i.set_owner(tmpl_i.created_by)
            db.DBSession.add(owner)
            flush = True
    if flush:
        db.DBSession.flush()


def _get_template_owners(template_id):
    """
        Get all the owners of a template
    """
    owners_i = db.DBSession.query(TemplateOwner).filter(
        TemplateOwner.template_id == template_id).options(noload('template')).options(joinedload('user')).all()

    owners = [JSONObject(owner_i) for owner_i in owners_i]

    return owners


@required_perms("get_template")
def get_templates(load_all=True, include_inactive=False, include_shared_templates=True, uid=None, template_ids=None,
                  project_id=None, **kwargs):
    """
        Get all templates.
        Args:
            load_all Boolean: Returns just the template entry or the full
            template structure (template types and type attrs)
            include_inactive Boolean: If true, returns all templates. If false, returns
            only templates with a status of 'A'
        Returns:
            List of Template objects
    """

    tpl_query = db.DBSession.query(Template)
    if uid:
        if include_shared_templates:
            tpl_query = tpl_query.join(TemplateOwner).filter(or_(TemplateOwner.user_id==uid, Template.created_by==uid))
        else:
            tpl_query = tpl_query.join(TemplateOwner).filter(Template.created_by == uid)
    if project_id:
        tpl_query = tpl_query.filter(Template.project_id == project_id)

    if template_ids is not None:
        tpl_query = tpl_query.filter(Template.id.in_(template_ids))

    templates_i = tpl_query.options(joinedload('templatetypes')).all()
    _set_template_owners(templates_i)

    if load_all is True:
        full_templates = []
        for template_i in templates_i:
            full_template = get_template(template_i.id, **kwargs)
            full_templates.append(full_template)
    else:
        full_templates = [JSONObject(template_i) for template_i in templates_i]


    #Filter out all the inactive templates
    if include_inactive is False:
        full_templates = list(filter(lambda x:x.status == 'A', full_templates))

    return full_templates

@required_perms("edit_template")
def remove_attr_from_type(type_id, attr_id, **kwargs):
    """
        Remove an attribute from a type
    """
    typeattr_i = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id == type_id,
                                                     TypeAttr.attr_id == attr_id).one()
    db.DBSession.delete(typeattr_i)

@required_perms("get_template")
def get_template(template_id, **kwargs):
    """
        Get a specific resource template, by ID.
    """
    try:
        tmpl_i = db.DBSession.query(Template).filter(
            Template.id == template_id).one()


        tmpl_j = JSONObject(tmpl_i)

        tmpl_j.templatetypes = tmpl_i.get_types()

        #ignore the messing around we've been doing to the ORM objects
        #db.DBSession.expunge(tmpl_i)

        return tmpl_j
    except NoResultFound:
        raise HydraError("Template %s not found"%template_id)

@required_perms("get_template")
def get_template_by_name(name, **kwargs):
    """
        Get a specific resource template, by name.
    """
    try:
        tmpl_i = db.DBSession.query(Template).filter(
            Template.name == name).one()

        tmpl_j = JSONObject(tmpl_i)

        tmpl_j.templatetypes = tmpl_i.get_types()

        return tmpl_j
    except NoResultFound:
        log.info("%s is not a valid identifier for a template", name)
        raise HydraError('Template "%s" not found'%name)

@required_perms("edit_template")
def add_templatetype(templatetype, **kwargs):
    """
        Add a template type with typeattrs.
    """

    type_i = _update_templatetype(templatetype, **kwargs)

    db.DBSession.flush()

    return type_i

def add_child_templatetype(parent_id, child_template_id, **kwargs):
    """
        Add a child templatetype
    """
    #check if the type is already there:
    #this means we can only add one child type per template
    existing_child = db.DBSession.query(TemplateType).filter(
        TemplateType.parent_id == parent_id,
        TemplateType.template_id == child_template_id).first()

    if existing_child is not None:
        return existing_child

    #Now check that we're not adding a child template type to a template type
    #which is already a child i.e. the template ID of the proposed parent is
    #the same as the one we want to put the child into
    parent_type = db.DBSession.query(TemplateType).filter(
        TemplateType.id == parent_id).one()

    if parent_type.template_id == child_template_id:
        return parent_type

    #The child doesn't exist already, so create it.
    child_type_i = TemplateType()
    child_type_i.template_id = child_template_id
    child_type_i.parent_id = parent_id

    db.DBSession.add(child_type_i)

    db.DBSession.flush()

    return child_type_i

def add_child_typeattr(parent_id, child_template_id, **kwargs):
    """
        Add template and a type and typeattrs.
    """

    #does this child already exist in this template?
    existing_child_typeattr = db.DBSession.query(TypeAttr).join(TemplateType).filter(
        TypeAttr.parent_id == parent_id).filter(
            TemplateType.template_id == child_template_id).first()

    if existing_child_typeattr is not None:
        return existing_child_typeattr

    parent_typeattr = db.DBSession.query(TypeAttr)\
            .filter(TypeAttr.id == parent_id).one()

    child_type = add_child_templatetype(parent_typeattr.type_id, child_template_id)

    child_typeattr_i = TypeAttr()
    child_typeattr_i.attr_id = parent_typeattr.attr_id
    child_typeattr_i.type_id = child_type.id
    child_typeattr_i.parent_id = parent_id

    db.DBSession.add(child_typeattr_i)

    db.DBSession.flush()

    return child_typeattr_i



@required_perms("edit_template")
def update_templatetype(templatetype, auto_delete=False, **kwargs):
    """
        Update a resource type and its typeattrs.
        New typeattrs will be added. typeattrs not sent will be ignored.
        To delete typeattrs, call delete_typeattr

        args:
            templatetype: A template type JSON object
            auto_delete (bool): Flag to indicate whether non-presence of
                                typeattrs in incoming object should delete them.
                                Default to False
    """

    tmpltype_i = db.DBSession.query(TemplateType).filter(TemplateType.id == templatetype.id).one()

    _update_templatetype(templatetype, tmpltype_i, auto_delete=auto_delete, **kwargs)

    db.DBSession.flush()

    updated_type = tmpltype_i.template.get_type(tmpltype_i.id)

#    db.DBSession.expunge(updated_type)

    return updated_type

def _set_typeattr(typeattr, existing_ta=None):
    """
        Add or update a type attribute.
        If an existing type attribute is provided, then update.

        Checks are performed to ensure that the dimension provided on the
        type attr (not updateable) is the same as that on the referring attribute.
        The unit provided (stored on tattr) must conform to the dimension stored
        on the referring attribute (stored on tattr).

        This is done so that multiple templates can all use the same attribute,
        but specify different units.

        If no attr_id is provided, but an attr_name and dimension are provided,
        then a new attribute can be created (or retrieved) and used. I.e., no
        attribute ID must be specified if attr_name and dimension are specified.

        ***WARNING***
        Setting ID to null means a new type attribute (and even a new attr)
        may be added, None are removed or replaced. To remove other type attrs, do it
        manually using delete_typeattr
    """
    if existing_ta is None:

        #check for an existing TA
        check_existing_ta = db.DBSession.query(TypeAttr)\
            .filter(TypeAttr.attr_id == typeattr.attr_id, TypeAttr.type_id == typeattr.type_id).first()

        #There's already a TA with this attr_id in this type
        if check_existing_ta is not None:
            ta = check_existing_ta
        else:
            ta = TypeAttr(attr_id=typeattr.attr_id)
            ## default new type attrs to 'active'.
            ##This has replaced the database default because for child typeattrs,
            ##we need the status to be NULL so it can inherit from its parent
            ta.status = 'A'
    else:
        if typeattr.id is not None:
            ta = db.DBSession.query(TypeAttr).filter(TypeAttr.id == typeattr.id).one()
        else:
            ta = existing_ta

    ta.attr_id = typeattr.attr_id
    ta.unit_id = typeattr.unit_id
    ta.type_id = typeattr.type_id
    ta.data_type = typeattr.data_type
    ta.status = typeattr.status if typeattr.status is not None else 'A'

    if hasattr(typeattr, 'default_dataset_id') and typeattr.default_dataset_id is not None:
        ta.default_dataset_id = typeattr.default_dataset_id

    ta.description = typeattr.description

    ta.properties = typeattr.get_properties()

    #support legacy use of 'is_var' instead of 'attr_is_var'
    if hasattr(typeattr, 'is_var') and typeattr.is_var is not None:
        typeattr.attr_is_var = typeattr.is_var

    ta.attr_is_var = typeattr.attr_is_var if typeattr.attr_is_var is not None else 'N'

    ta.data_restriction = _parse_data_restriction(typeattr.data_restriction)

    if typeattr.unit_id is None or typeattr.unit_id == '':
        # All right. Check passed
        ta.unit_id = None
        pass
    else:
        unit = units.get_unit(typeattr.unit_id)
        dimension = units.get_dimension(unit.dimension_id)
        if typeattr.attr_id is not None and typeattr.attr_id > 0:
            # Getting the passed attribute, so we need to check consistency
            # between attr dimension id and typeattr dimension id
            attr = db.DBSession.query(Attr).filter(Attr.id==ta.attr_id).first()
            if attr is not None and attr.dimension_id is not None and\
               attr.dimension_id != dimension.id or \
               attr is not None and attr.dimension_id is None:

                attr_dimension = units.get_dimension(attr.dimension_id)
                # In this case there is an inconsistency between
                # attr.dimension_id and typeattr.unit_id
                raise HydraError("Unit mismatch between type and attirbute."+
                                 f"Type attribute for {attr.name} specifies "+
                                 f"unit {unit.name}, dimension {dimension.name}."+
                                 f"The attribute specifies a dimension of {attr_dimension.name}"+
                                 "Cannot set a unit on a type attribute which "+
                                 "does not match its attribute.")
        elif typeattr.attr_id is None and typeattr.name is not None:
            # Getting/creating the attribute by typeattr dimension id and typeattr name
            # In this case the dimension_id "null"/"not null" status is ininfluent
            attr = get_attr_by_name_and_dimension(typeattr.name, dimension.id)

            ta.attr_id = attr.id
            ta.attr = attr

    check_dimension(ta)

    if existing_ta is None:
        log.debug("Adding ta to DB")
        db.DBSession.add(ta)

    attr = db.DBSession.query(Attr).filter(Attr.id == ta.attr_id).one()
    ta.attr = attr

    return ta

def _set_cols(source, target, reference=None):
    """
        Set the value on the column of a target row.
        This checks if the value is the same as that of another reference row,
        and if it is the same, it sets that column to None.
        Ex:
            reference is: {'status': 'x'} and value is 'x'}, then
            target will be set to {'status': None}. If status is 'y',
            then target will be {'status', 'y'}
        Args:
            target: DB row to write the column to
            source: The incoming object (a JSONObject) containing the request data
            reference: DB row used for comparison
            colnames: The column names to set
            value: The value to set.
    """
    for colname in source:

        if colname not in target.__table__.columns:
            continue

        if colname in ['cr_date', 'updated_at']:
            continue

        if hasattr(reference, '_protected_columns')\
           and colname in reference._protected_columns:
            #as a child, you can't change stuff like IDS, cr dates etc.
            continue

        if target.parent_id is not None\
           and hasattr(reference, '_hierarchy_columns')\
           and colname in reference._hierarchy_columns:
            #as a child, you can't change stuff like IDS, cr dates etc.
            continue

        newval = getattr(source, colname)

        if colname == 'layout':
            newval = get_json_as_string(newval)

        if reference is None:
            setattr(target, colname, newval)
            continue

        refval = getattr(reference, colname)

        if newval != refval:
            setattr(target, colname, newval)


def _update_templatetype(templatetype, existing_tt=None, auto_delete=False, **kwargs):
    """
        Add or update a templatetype. If an existing template type is passed in,
        update that one. Otherwise search for an existing one. If not found, add.
    """
    #flag to indicate if this update results in an insert
    is_new = False

    if existing_tt is None:
        if "id" in templatetype and templatetype.id is not None:
            tmpltype_i = db.DBSession.query(TemplateType).filter(
                TemplateType.id == templatetype.id).one()
        else:
            is_new = True
            tmpltype_i = TemplateType()
            tmpltype_i.template_id = templatetype.template_id

            ## default new template types to active
            ## This has replaced the database default because for child typeattrs,
            ## we need the status to be NULL so it can inherit from its parent
            tmpltype_i.status = 'A'
    else:
        tmpltype_i = existing_tt

    _set_cols(templatetype, tmpltype_i, existing_tt)

    ta_dict = {}
    for t in tmpltype_i.get_typeattrs():
        ta_dict[t.attr_id] = t

    existing_attrs = []

    if templatetype.typeattrs is not None:
        for typeattr in templatetype.typeattrs:
            if typeattr.attr_id in ta_dict:
                #this belongs to a parent. Ignore.
                if typeattr.type_id is not None and typeattr.type_id != tmpltype_i.id:
                    continue

                ta = _set_typeattr(typeattr, ta_dict[typeattr.attr_id])
                existing_attrs.append(ta.attr_id)
            else:
                ta = _set_typeattr(typeattr)
                tmpltype_i.typeattrs.append(ta)
                existing_attrs.append(ta.attr_id)

    log.debug("Deleting any type attrs not sent")
    if auto_delete is True:
        for ta in ta_dict.values():
            if ta.attr_id not in existing_attrs:
                delete_typeattr(ta, **kwargs)

    if is_new is True:
        db.DBSession.add(tmpltype_i)

    return tmpltype_i

@required_perms("edit_template")
def delete_templatetype(type_id, template_i=None, delete_resourcetypes=False, flush=True, delete_children=False, **kwargs):
    """
        Delete a template type and its typeattrs.
    """

    try:
        tmpltype_i = db.DBSession.query(TemplateType)\
                .filter(TemplateType.id == type_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Template Type %s not found"%(type_id,))

    if template_i is None:
        template_i = db.DBSession.query(Template).filter(
            Template.id == tmpltype_i.template_id).one()

    if len(tmpltype_i.get_children()) > 0 and delete_children is False:
        raise HydraError(f"Unable to delete type. Template type {tmpltype_i.name} (ID: {type_id}) has"
                         f"children. If you want to delete this, use the 'delete_children' flag.")

    if delete_children is True:
        tmpltype_i.delete_children(delete_resourcetypes=delete_resourcetypes)

    tmpltype_i.check_can_delete_resourcetypes(delete_resourcetypes=delete_resourcetypes)

    if delete_resourcetypes is True:
        tmpltype_i.delete_resourcetypes()

    #first remove the templatetypes
    for ta_i in tmpltype_i.typeattrs:
        db.DBSession.delete(ta_i)

    db.DBSession.delete(tmpltype_i)

    if flush:
        db.DBSession.flush()

@required_perms("get_template")
def get_templatetype(type_id, include_parent_data=True, **kwargs):
    """
        Get a specific template type by ID. As types can be inherited, this
        type may contain data from its parent. If the 'include_parent_type' is false,
        then just the data for this templatetype is returned.
    """

    #First get the DB entry
    templatetype = db.DBSession.query(TemplateType).filter(
        TemplateType.id == type_id).options(noload("typeattrs")).one()

    if include_parent_data is False:
        return templatetype

    #then get the template
    template = db.DBSession.query(Template).filter(
        Template.id == templatetype.template_id).one()

    #then get the type, but this time with inherited data.
    inherited_templatetype = template.get_type(type_id)

    #ignore the messing around we've been doing to the ORM objects
#    db.DBSession.expunge(inherited_templatetype)

    return inherited_templatetype

@required_perms("get_template")
def get_typeattr(typeattr_id, include_parent_data=True, **kwargs):
    """
        Get a specific resource type by ID.
    """

    typeattr = db.DBSession.query(TypeAttr)\
            .filter(TypeAttr.id == typeattr_id)\
            .options(joinedload("default_dataset")).one()

    if include_parent_data is False:
        return typeattr

    template = typeattr.templatetype.template

    #then get the type, but this time with inherited data.
    inherited_typeattr = template.get_typeattr(typeattr_id)

#    db.DBSession.expunge(inherited_typeattr)

    return inherited_typeattr



@required_perms("get_template")
def get_templatetype_by_name(template_id, type_name, **kwargs):
    """
        Get a specific resource type by name.
    """

    try:
        templatetype = db.DBSession.query(TemplateType).filter(
            TemplateType.template_id == template_id,
            TemplateType.name == type_name).one()
    except NoResultFound:
        raise HydraError("%s is not a valid identifier for a type"%(type_name))

    inherited_templatetype = get_templatetype(templatetype.id, **kwargs)

    return inherited_templatetype

@required_perms("edit_template")
def add_typeattr(typeattr, **kwargs):
    """
        Add an typeattr to an existing type.
    """

    tmpltype = get_templatetype(typeattr.type_id, user_id=kwargs.get('user_id'))

    ta = _set_typeattr(typeattr)

    db.DBSession.flush()

    return ta

@required_perms("edit_template")
def update_typeattr(typeattr, **kwargs):
    """
        Update an existing an typeattr with updated values.
    """
    #First check if an existing one already exists.
    existing_ta = None
    if typeattr.id is not None:
        existing_ta = db.DBSession.query(TypeAttr)\
                .filter(TypeAttr.id == typeattr.id).first()
    else:
        existing_ta = db.DBSession.query(TypeAttr)\
            .filter(TypeAttr.type_id == typeattr.type_id,
                    TypeAttr.attr_id == typeattr.attr_id).first()

    typeattr_updated = _set_typeattr(typeattr, existing_ta)

    db.DBSession.flush()

    return typeattr_updated

@required_perms("edit_template")
def delete_typeattr(typeattr_id, **kwargs):
    """
        Remove an typeattr from an existing type
    """

    ta = db.DBSession.query(TypeAttr).filter(TypeAttr.id == typeattr_id).one()

    db.DBSession.delete(ta)

    db.DBSession.flush()

    return 'OK'
