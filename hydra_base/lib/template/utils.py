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

from collections.abc import Sized

from hydra_base import db
from hydra_base.db.model import Attr
from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import HydraError
from hydra_base.util import dataset_util
from hydra_base.lib import units

log = logging.getLogger(__name__)

def get_attr(attr_id):
    attr = db.DBSession.query(Attr).filter(Attr.id == attr_id).one()
    return JSONObject(attr)

def check_dimension(typeattr, unit_id=None):
    """
        Check that the unit and dimension on a type attribute match.
        Alternatively, pass in a unit manually to check against the dimension
        of the type attribute
    """
    if unit_id is None:
        unit_id = typeattr.unit_id

    dimension_id = get_attr(typeattr.attr_id).dimension_id

    if unit_id is not None and dimension_id is None:
        # First error case
        unit_dimension_id = units.get_dimension_by_unit_id(unit_id).id
        unit = units.get_unit(unit_id)
        dimension = units.get_dimension(unit_dimension_id, do_accept_dimension_id_none=True)
        raise HydraError(f"Unit {unit_id} ({unit.abbreviation}) has dimension_id"+
                         f" {dimension.id}(name=dimension.name),"+
                         " but attribute has no dimension")
    elif unit_id is not None and dimension_id is not None:
        unit_dimension_id = units.get_dimension_by_unit_id(unit_id).id
        unit = units.get_unit(unit_id)
        dimension1 = units.get_dimension(unit_dimension_id, do_accept_dimension_id_none=True)
        dimension2 = units.get_dimension(unit_dimension_id, do_accept_dimension_id_none=True)
        if unit_dimension_id != dimension_id:
            # Only error case
            raise HydraError(f"Unit {unit_id} ({unit.abbreviation}) has dimension_id"+
                             f" {dimension1.id}(name=dimension1.name),"+
                             f" but attribute has id: {dimension2.id}({dimension2.name})")

def get_attr_by_name_and_dimension(name, dimension_id):
    """
        Search for an attribute with the given name and dimension_id.
        If such an attribute does not exist, create one.
    """

    attr = db.DBSession.query(Attr).filter(
        Attr.name == name,
        Attr.dimension_id == dimension_id).first()

    if attr is None:
        # In this case the attr does not exists so we must create it
        attr = Attr()
        attr.dimension_id = dimension_id
        attr.name = name

        log.debug("Attribute not found, creating new attribute: name:%s, dimen:%s",
                  attr.name, attr.dimension_id)

        db.DBSession.add(attr)

    return attr

def parse_data_restriction(restriction_dict):
    if restriction_dict is None or len(restriction_dict) == 0:
        return None

    #replace soap text with an empty string
    #'{soap_server.hydra_complexmodels}' -> ''
    dict_str = re.sub('{[a-zA-Z._]*}', '', str(restriction_dict))

    if isinstance(restriction_dict, dict):
        new_dict = restriction_dict
    else:
        new_dict = json.loads(restriction_dict)

    #Evaluate whether the dict actually contains anything.
    if not isinstance(new_dict, dict) or len(new_dict) == 0:
        log.critical('A restriction was specified, but it is null')
        return None

    ret_dict = {}
    for k, v in new_dict.items():
        if isinstance(v, Sized) and len(v) == 1:
            ret_dict[k] = v[0]
        else:
            ret_dict[k] = v

    return json.dumps(ret_dict)

def validate_resourcescenario(resourcescenario, template_id=None, **kwargs):
    """
        Perform a check to ensure a resource scenario's datasets are correct given what the
        definition of that resource (its type) specifies.
    """
    res = resourcescenario.resourceattr.get_resource()

    types = res.types

    dataset = resourcescenario.dataset

    if len(types) == 0:
        return

    if template_id is not None:
        if template_id not in [r.templatetype.template_id for r in res.types]:
            raise HydraError("Template %s is not used for resource attribute %s in scenario %s"%\
                             (template_id, resourcescenario.resourceattr.attr.name,
                             resourcescenario.scenario.name))

    #Validate against all the types for the resource
    for resourcetype in types:
        #If a specific type has been specified, then only validate
        #against that type and ignore all the others
        if template_id is not None:
            if resourcetype.templatetype.template_id != template_id:
                continue
        #Identify the template types for the template
        tmpltype = resourcetype.templatetype
        for ta in tmpltype.typeattrs:
            #If we find a template type which mactches the current attribute.
            #we can do some validation.
            if ta.attr_id == resourcescenario.resourceattr.attr_id:
                if ta.data_restriction:
                    log.debug("Validating against %s", ta.data_restriction)
                    validation_dict = json.loads(ta.data_restriction)
                    dataset_util.validate_value(validation_dict, dataset.get_val())

def validate_resource(resource, tmpl_types, resource_scenarios=[], **kwargs):
    errors = []
    resource_type = None

    #No validation required if the resource has no type.
    if len(resource.types) == 0:
        return []

    for rt in resource.types:
        if tmpl_types.get(rt.type_id) is not None:
            resource_type = tmpl_types[rt.type_id]
            break
        else:
            errors.append("Type %s not found on %s %s"%
                          (tmpl_types, resource_type, resource.get_name()))

    ta_dict = {}
    for ta in resource_type.typeattrs:
        ta_dict[ta.attr_id] = ta

    #Make sure the resource has all the attributes specified in the template
    #by checking whether the template attributes are a subset of the resource
    #attributes.
    type_attrs = set([ta.attr_id for ta in resource_type.typeattrs])

    resource_attrs = set([ra.attr_id for ra in resource.attributes])

    if not type_attrs.issubset(resource_attrs):
        for ta in type_attrs.difference(resource_attrs):
            errors.append("Resource %s does not have attribute %s"%
                          (resource.get_name(), ta_dict[ta].attr.name))

    resource_attr_ids = set([ra.id for ra in resource.attributes])
    #if data is included, check to make sure each dataset conforms
    #to the boundaries specified in the template: i.e. that it has
    #the correct dimension and (if specified) unit.
    if len(resource_scenarios) > 0:
        for ra_id in resource_attr_ids:
            rs = resource_scenarios.get(ra_id)
            if rs is None:
                continue
            attr_name = rs.resourceattr.attr.name
            rs_unit_id = rs.dataset.unit_id

            rs_dimension_id = units.get_dimension_by_unit_id(rs_unit_id, do_accept_unit_id_none=True).id

            type_dimension_id = ta_dict[rs.resourceattr.attr_id].attr.dimension_id
            type_unit_id = ta_dict[rs.resourceattr.attr_id].unit_id

            if rs_dimension_id != type_dimension_id:
                errors.append("Dimension mismatch on %s %s, attribute %s: "
                              "%s on attribute, %s on type"%
                             (resource.ref_key, resource.get_name(), attr_name,
                              rs_dimension_id, type_dimension_id))

            if type_unit_id is not None:
                if rs_unit_id != type_unit_id:
                    errors.append("Unit mismatch on attribute %s. "
                                  "%s on attribute, %s on type"%
                                 (attr_name, rs_unit_id, type_unit_id))
    if len(errors) > 0:
        log.warning(errors)

    return errors
