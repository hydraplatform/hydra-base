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

from __future__ import division

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import func
from sqlalchemy.orm import load_only

from .. import db

from ..util.dataset_util import array_dim
from ..util.dataset_util import arr_to_vector
from ..util.dataset_util import vector_to_arr
from ..db.model import Dataset, Unit, Dimension
from .objects import JSONObject
from ..exceptions import HydraError, ResourceNotFoundError, ValidationError

from ..util.permissions import required_perms

import numpy
import logging
log = logging.getLogger(__name__)



"""
+-------------------------------+
| DIMENSION FUNCTIONS - VARIOUS |
+-------------------------------+
"""

def exists_dimension(dimension_name,**kwargs):
    """
        Given a dimension returns True if it exists, False otherwise
    """
    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()
        # At this point the dimension exists
        return True
    except NoResultFound:
        # The dimension does not exist
        raise False

def is_global_dimension(dimension_id,**kwargs):
    """
        Returns True if the dimension is Global, False is it is assigned to a project
    """
    dimension = get_dimension(dimension_id)
    return (dimension.project_id is None)


"""
+--------------------------+
| UNIT FUNCTIONS - VARIOUS |
+--------------------------+
"""

def _parse_unit(measure_or_unit_abbreviation):
    """
        Helper function that extracts constant factors from unit specifications.
        This allows to specify units similar to this: 10^6 m^3.
        Return a couple (unit, factor)
    """
    try:
        float(measure_or_unit_abbreviation[0])
        # The measure contains the values and the unit_abbreviation
        factor, unit_abbreviation = measure_or_unit_abbreviation.split(' ', 1)
        return unit_abbreviation, float(factor)
    except ValueError:
        # The measure just contains the unit_abbreviation
        return measure_or_unit_abbreviation, 1.0

def is_global_unit(unit_id,**kwargs):
    """
        Returns True if the Unit is Global, False is it is assigned to a project
        'unit' is a Unit object
    """
    unit_data = get_unit(unit_id)
    return (unit_data.project_id is None)

def convert_units(values, source_measure_or_unit_abbreviation, target_measure_or_unit_abbreviation,**kwargs):
    """
        Convert a value from one unit to another one.

        Example::

            >>> cli = PluginLib.connect()
            >>> cli.service.convert_units(20.0, 'm', 'km')
            0.02
        Parameters:
            values: single measure or an array of measures
            source_measure_or_unit_abbreviation: A measure in the source unit, or just the abbreviation of the source unit, from which convert the provided measure value/values
            target_measure_or_unit_abbreviation: A measure in the target unit, or just the abbreviation of the target unit, into which convert the provided measure value/values

        Returns:
            Always a list
    """
    if numpy.isscalar(values):
        # If it is a scalar, converts to an array
        values = [values]
    float_values = [float(value) for value in values]
    values_to_return = convert(float_values, source_measure_or_unit_abbreviation, target_measure_or_unit_abbreviation)

    return values_to_return

def convert(values, source_measure_or_unit_abbreviation, target_measure_or_unit_abbreviation):
    """
        Convert a value or a list of values from an unit to another one.
        The two units must represent the same physical dimension.
    """

    source_dimension = get_dimension_by_unit_measure_or_abbreviation(source_measure_or_unit_abbreviation)
    target_dimension = get_dimension_by_unit_measure_or_abbreviation(target_measure_or_unit_abbreviation)

    if source_dimension == target_dimension:
        source=JSONObject({})
        target=JSONObject({})
        source.unit_abbreviation, source.factor = _parse_unit(source_measure_or_unit_abbreviation)
        target.unit_abbreviation, target.factor = _parse_unit(target_measure_or_unit_abbreviation)

        source.unit_data = get_unit_by_abbreviation(source.unit_abbreviation)
        target.unit_data = get_unit_by_abbreviation(target.unit_abbreviation)

        source.conv_factor = JSONObject({'lf': source.unit_data.lf, 'cf': source.unit_data.cf})
        target.conv_factor = JSONObject({'lf': target.unit_data.lf, 'cf': target.unit_data.cf})

        if isinstance(values, float):
            # If values is a float => returns a float
            return (source.conv_factor.lf / target.conv_factor.lf * (source.factor * values)
                    + (source.conv_factor.cf - target.conv_factor.cf)
                    / target.conv_factor.lf) / target.factor
        elif isinstance(values, list):
            # If values is a list of floats => returns a list of floats
            return [(source.conv_factor.lf / target.conv_factor.lf * (source.factor * value)
                    + (source.conv_factor.cf - target.conv_factor.cf)
                    / target.conv_factor.lf) / target.factor for value in values]
    else:
        raise HydraError("Unit conversion: dimensions are not consistent.")


"""
+---------------------------+
| DIMENSION FUNCTIONS - GET |
+---------------------------+
"""
def get_empty_dimension(**kwargs):
    """
        Returns a dimension object initialized with empty values
    """
    dimension = JSONObject(Dimension())
    dimension.id = None
    dimension.name = ''
    dimension.description = ''
    dimension.project_id = None
    dimension.units = []
    return dimension



def get_dimension(dimension_id, do_accept_dimension_id_none=False,**kwargs):
    """
        Given a dimension id returns all its data
    """
    if do_accept_dimension_id_none == True and dimension_id is None:
        # In this special case, the method returns a dimension with id None
        return get_empty_dimension()

    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.id==dimension_id).one()

        #lazy load units
        dimension.units

        return JSONObject(dimension)
    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Dimension %s not found"%(dimension_id))


def get_dimensions(**kwargs):
    """
        Returns a list of objects describing all the dimensions with all the units.
    """
    dimensions_list = db.DBSession.query(Dimension).options(load_only(Dimension.id)).all()
    return_list = []
    for dimension in dimensions_list:
        return_list.append(get_dimension(dimension.id))


    return return_list


def get_dimension_by_name(dimension_name,**kwargs):
    """
        Given a dimension name returns all its data. Used in convert functions
    """
    try:
        if dimension_name is None:
            dimension_name = ''
        dimension = db.DBSession.query(Dimension).filter(func.lower(Dimension.name)==func.lower(dimension_name.strip())).one()

        return get_dimension(dimension.id)

    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Dimension %s not found"%(dimension_name))


"""
+----------------------+
| UNIT FUNCTIONS - GET |
+----------------------+
"""


def get_unit(unit_id, **kwargs):
    """
        Returns a single unit
    """
    try:
        unit = db.DBSession.query(Unit).filter(Unit.id==unit_id).one()
        return JSONObject(unit)
    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Unit %s not found"%(unit_id))

def get_units(**kwargs):
    """
        Returns all the units
    """
    units_list = db.DBSession.query(Unit).all()
    units = []
    for unit in units_list:
        new_unit = JSONObject(unit)
        units.append(new_unit)

    return units

def get_dimension_by_unit_measure_or_abbreviation(measure_or_unit_abbreviation,**kwargs):
    """
        Return the physical dimension a given unit abbreviation of a measure, or the measure itself, refers to.
        The search key is the abbreviation or the full measure
    """

    unit_abbreviation, factor = _parse_unit(measure_or_unit_abbreviation)

    units = db.DBSession.query(Unit).filter(Unit.abbreviation==unit_abbreviation).all()

    if len(units) == 0:
        raise HydraError('Unit %s not found.'%(unit_abbreviation))
    elif len(units) > 1:
        raise HydraError('Unit %s has multiple dimensions not found.'%(unit_abbreviation))
    else:
        dimension = db.DBSession.query(Dimension).filter(Dimension.id==units[0].dimension_id).one()
        return dimension

def get_dimension_by_unit_id(unit_id, do_accept_unit_id_none=False, **kwargs):
    """
        Return the physical dimension a given unit id refers to.
        if do_accept_unit_id_none is False, it raises an exception if unit_id is not valid or None
        if do_accept_unit_id_none is True, and unit_id is None, the function returns a Dimension with id None (unit_id can be none in some cases)
    """
    if do_accept_unit_id_none == True and unit_id is None:
        # In this special case, the method returns a dimension with id None
        return get_empty_dimension()

    try:
        dimension = db.DBSession.query(Dimension).join(Unit).filter(Unit.id==unit_id).filter().one()
        return get_dimension(dimension.id)
    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Unit %s not found"%(unit_id))


def get_unit_by_abbreviation(unit_abbreviation, **kwargs):
    """
        Returns a single unit by abbreviation. Used as utility function to resolve string to id
    """
    try:
        if unit_abbreviation is None:
            unit_abbreviation = ''
        unit_i = db.DBSession.query(Unit).filter(Unit.abbreviation==unit_abbreviation.strip()).one()
        return JSONObject(unit_i)
    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Unit '%s' not found"%(unit_abbreviation))

"""
+---------------------------------------+
| DIMENSION FUNCTIONS - ADD - DEL - UPD |
+---------------------------------------+
"""

@required_perms("add_dimension")
def add_dimension(dimension,**kwargs):
    """
        Add the dimension defined into the object "dimension" to the DB
        If dimension["project_id"] is None it means that the dimension is global, otherwise is property of a project
        If the dimension exists emits an exception
    """

    db_dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension['name']).first()

    if db_dimension is not None:
        raise HydraError(f"A dimension with name {dimension['name']} "
                         f"already exists with ID {db_dimension.id}")

    new_dimension = Dimension()
    new_dimension.name = dimension["name"]

    if "description" in dimension and dimension["description"] is not None:
        new_dimension.description = dimension["description"]
    if "project_id" in dimension and dimension["project_id"] is not None:
        new_dimension.project_id = dimension["project_id"]

    # Save on DB
    db.DBSession.add(new_dimension)
    db.DBSession.flush()

    # Load all the record
    db_dimension = db.DBSession.query(Dimension).filter(Dimension.id==new_dimension.id).one()
    #lazy load units
    db_dimension.units

    return JSONObject(db_dimension)

@required_perms("update_dimension")
def update_dimension(dimension,**kwargs):
    """
        Update a dimension in the DB.
        Raises and exception if the dimension does not exist.
        The key is ALWAYS the name and the name itself is not modificable
    """
    db_dimension = None
    dimension = JSONObject(dimension)
    try:
        db_dimension = db.DBSession.query(Dimension).filter(Dimension.id==dimension.id).filter().one()
        #laxy load units
        db_dimension.units
        if "description" in dimension and dimension["description"] is not None:
            db_dimension.description = dimension["description"]
        if "project_id" in dimension and dimension["project_id"] is not None and dimension["project_id"] != "" and dimension["project_id"].isdigit():
            db_dimension.project_id = dimension["project_id"]
    except NoResultFound:
        raise ResourceNotFoundError("Dimension (ID=%s) does not exist"%(dimension.id))


    db.DBSession.flush()
    return JSONObject(db_dimension)

@required_perms("delete_dimension")
def delete_dimension(dimension_id,**kwargs):
    """
        Delete a dimension from the DB. Raises and exception if the dimension does not exist
    """
    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.id==dimension_id).one()

        db.DBSession.query(Unit).filter(Unit.dimension_id==dimension.id).delete()

        db.DBSession.delete(dimension)
        db.DBSession.flush()
        return True
    except NoResultFound:
        raise ResourceNotFoundError("Dimension (dimension_id=%s) does not exist"%(dimension_id))


@required_perms("add_dimension")
def bulk_add_dimensions(dimension_list, **kwargs):
    """
        Save all the dimensions contained in the passed list.
    """
    added_dimensions = []
    for dimension in dimension_list:
        added_dimensions.append(add_dimension(dimension, **kwargs))

    return JSONObject({"dimensions": added_dimensions})

"""
+----------------------------------+
| UNIT FUNCTIONS - ADD - DEL - UPD |
+----------------------------------+
"""

@required_perms("add_unit")
def add_unit(unit,**kwargs):
    """
        Add the unit defined into the object "unit" to the DB
        If unit["project_id"] is None it means that the unit is global, otherwise is property of a project
        If the unit exists emits an exception


        A minimal example:

        .. code-block:: python

            new_unit = dict(

                name      = 'Teaspoons per second',
                abbreviation = 'tsp s^-1',
                cf        = 0,               # Constant conversion factor
                lf        = 1.47867648e-05,  # Linear conversion factor
                dimension_id = 2,
                description  = 'A flow of one teaspoon per second.',
            )
            add_unit(new_unit)


    """

    new_unit = Unit()
    new_unit.dimension_id   = unit["dimension_id"]
    new_unit.name           = unit['name']

    # Needed to uniform abbr to abbreviation
    new_unit.abbreviation = unit['abbreviation']

    # Needed to uniform into to description
    new_unit.description = unit.get('description')

    new_unit.lf             = unit['lf']
    new_unit.cf             = unit['cf']

    if ('project_id' in unit) and (unit['project_id'] is not None):
        # Adding dimension to the "user" dimensions list
        new_unit.project_id = unit['project_id']

    # Save on DB
    db.DBSession.add(new_unit)
    db.DBSession.flush()

    return JSONObject(new_unit)

@required_perms("add_unit")
def bulk_add_units(unit_list, **kwargs):
    """
        Save all the units contained in the passed list, with the name of their dimension.
    """
    # for unit in unit_list:
    #     add_unit(unit, **kwargs)

    added_units = []
    for unit in unit_list:
        added_units.append(add_unit(unit, **kwargs))

    return JSONObject({"units": added_units})




@required_perms("delete_unit")
def delete_unit(unit_id, **kwargs):
    """
        Delete a unit from the DB.
        Raises and exception if the unit does not exist
    """

    try:
        db_unit = db.DBSession.query(Unit).filter(Unit.id==unit_id).one()

        db.DBSession.delete(db_unit)
        db.DBSession.flush()
        return True
    except NoResultFound:
        raise ResourceNotFoundError("Unit (ID=%s) does not exist"%(unit_id))


@required_perms("update_unit")
def update_unit(unit, **kwargs):
    """
        Update a unit in the DB.
        Raises and exception if the unit does not exist
    """
    try:

        db_unit = db.DBSession.query(Unit).join(Dimension).filter(Unit.id==unit["id"]).filter().one()

        db_unit.name = unit["name"]

        # Needed to uniform into to description
        db_unit.abbreviation = unit.abbreviation
        db_unit.description = unit.description

        db_unit.lf = unit["lf"]
        db_unit.cf = unit["cf"]
        if "project_id" in unit and unit['project_id'] is not None and unit['project_id'] != "":
            db_unit.project_id = unit["project_id"]
    except NoResultFound:
        raise ResourceNotFoundError("Unit (ID=%s) does not exist"%(unit["id"]))


    db.DBSession.flush()
    return JSONObject(db_unit)



"""
+-----------------+
| OTHER FUNCTIONS |
+-----------------+
"""
def check_consistency(measure_or_unit_abbreviation, dimension,**kwargs):
    """
        Check whether a specified unit is consistent with the physical
        dimension asked for by the attribute or the dataset.
    """
    dim = get_dimension_by_unit_measure_or_abbreviation(measure_or_unit_abbreviation)
    return dim.name == dimension

def check_unit_matches_dimension(unit_id, dimension_id,**kwargs):
    """
        Check whether a specified unit is part of the specified dimension.
        args:
            unit_id (int): The ID of the unit to compare with the dimension
            dimension_id (int): The ID of the dimension to check
        throws
            hydra_base.ValidationError when the unit_id does not match the supplied dimension
        returns:
            None
    """
    #ensure the unit exists
    unit_i = db.DBSession.query(Unit).filter(Unit.id==unit_id).one()

    if dimension_id is not None:
        #ensure the dimension exists
        dimension_i = db.DBSession.query(Dimension).filter(Dimension.id==dimension_id).one()
        dimension_name = dimension_i.name
    else:
        dimension_name = None

    if unit_i.dimension.id != dimension_id:
        raise ValidationError(f"Unit {unit_i.name} has a dimension of {unit_i.dimension.name}, not {dimension_name}")


"""
+-------------------+
| DATASET functions |
+-------------------+
"""


def convert_dataset(dataset_id, target_unit_abbreviation,**kwargs):
    """
        Convert a whole dataset (specified by 'dataset_id') to new unit ('target_unit_abbreviation').
        Conversion ALWAYS creates a NEW dataset, so function returns the dataset ID of new dataset.
    """

    ds_i = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()

    dataset_type = ds_i.type

    dsval = ds_i.get_val()
    source_unit_abbreviation = get_unit(ds_i.unit_id).abbreviation

    if source_unit_abbreviation is not None:
        if dataset_type == 'scalar':
            new_val = convert(float(dsval), source_unit_abbreviation, target_unit_abbreviation)
        elif dataset_type == 'array':
            dim = array_dim(dsval)
            vecdata = arr_to_vector(dsval)
            newvec = convert(vecdata, source_unit_abbreviation, target_unit_abbreviation)
            new_val = vector_to_arr(newvec, dim)
        elif dataset_type == 'timeseries':
            new_val = []
            for ts_time, ts_val in dsval.items():
                dim = array_dim(ts_val)
                vecdata = arr_to_vector(ts_val)
                newvec = convert(vecdata, source_unit_abbreviation, target_unit_abbreviation)
                newarr = vector_to_arr(newvec, dim)
                new_val.append(ts_time, newarr)
        elif dataset_type == 'descriptor':
            raise HydraError('Cannot convert descriptor.')

        new_dataset = Dataset()
        new_dataset.type   = ds_i.type
        new_dataset.value  = str(new_val)
        new_dataset.name   = ds_i.name

        new_dataset.unit_id   = get_unit_by_abbreviation(target_unit_abbreviation).id
        new_dataset.hidden = 'N'
        new_dataset.set_metadata(ds_i.get_metadata_as_dict())
        new_dataset.set_hash()

        existing_ds = db.DBSession.query(Dataset).filter(Dataset.hash==new_dataset.hash).first()

        if existing_ds is not None:
            db.DBSession.expunge_all()
            return existing_ds.id

        db.DBSession.add(new_dataset)
        db.DBSession.flush()

        return new_dataset.id

    else:
        raise HydraError('Dataset has no units.')


"""
+--------------------+
| RESOURCE functions |
+--------------------+
"""

def validate_resource_attributes(resource, attributes, template, check_unit=True, exact_match=False,**kwargs):
    """
        Validate that the resource provided matches the template.
        Only passes if the resource contains ONLY the attributes specified
        in the template.

        The template should take the form of a dictionary, as should the
        resources.

        *check_unit*: Makes sure that if a unit is specified in the template, it
                      is the same in the data
        *exact_match*: Ensures that all the attributes in the template are in
                       the data also. By default this is false, meaning a subset
                       of the template attributes may be specified in the data.
                       An attribute specified in the data *must* be defined in
                       the template.

        @returns a list of error messages. An empty list indicates no
        errors were found.
    """
    errors = []
    #is it a node or link?
    res_type = 'GROUP'
    if resource.get('x') is not None:
        res_type = 'NODE'
    elif resource.get('node_1_id') is not None:
        res_type = 'LINK'
    elif resource.get('nodes') is not None:
        res_type = 'NETWORK'

    #Find all the node/link/network definitions in the template
    tmpl_res = template['resources'][res_type]

    #the user specified type of the resource
    res_user_type = resource.get('type')

    #Check the user specified type is in the template
    if res_user_type is None:
        errors.append("No type specified on resource %s"%(resource['name']))

    elif tmpl_res.get(res_user_type) is None:
        errors.append("Resource %s is defined as having type %s but "
                      "this type is not specified in the template."%
                      (resource['name'], res_user_type))

    #It is in the template. Now check all the attributes are correct.
    tmpl_attrs = tmpl_res.get(res_user_type)['attributes']

    attrs = {}
    for a in attributes.values():
        attrs[a['id']] = a

    for a in tmpl_attrs.values():
        if a.get('id') is not None:
            attrs[a['id']] = {'name':a['name'], 'unit':a.get('unit'), 'dimen':a.get('dimension')}

    if exact_match is True:
        #Check that all the attributes in the template are in the data.
        #get all the attribute names from the template
        tmpl_attr_names = set(tmpl_attrs.keys())
        #get all the attribute names from the data for this resource
        resource_attr_names = []
        for ra in resource['attributes']:
            attr_name = attrs[ra['attr_id']]['name']
            resource_attr_names.append(attr_name)
        resource_attr_names = set(resource_attr_names)

        #Compare the two lists to ensure they are the same (using sets is easier)
        in_tmpl_not_in_resource = tmpl_attr_names - resource_attr_names
        in_resource_not_in_tmpl = resource_attr_names - tmpl_attr_names

        if len(in_tmpl_not_in_resource) > 0:
            errors.append("Template has defined attributes %s for type %s but they are not"
                            " specified in the Data."%(','.join(in_tmpl_not_in_resource),
                                                    res_user_type ))

        if len(in_resource_not_in_tmpl) > 0:
            errors.append("Resource %s (type %s) has defined attributes %s but this is not"
                            " specified in the Template."%(resource['name'],
                                                        res_user_type,
                                                        ','.join(in_resource_not_in_tmpl)))

    #Check that each of the attributes specified on the resource are valid.
    for res_attr in resource['attributes']:

        attr = attrs.get(res_attr['attr_id'])

        if attr is None:
            errors.append("An attribute mismatch has occurred. Attr %s is not "
                          "defined in the data but is present on resource %s"
                          %(res_attr['attr_id'], resource['name']))
            continue

        #If an attribute is not specified in the template, then throw an error
        if tmpl_attrs.get(attr['name']) is None:
            errors.append("Resource %s has defined attribute %s but this is not"
                          " specified in the Template."%(resource['name'], attr['name']))
        else:
            #If the dimensions or units don't match, throw an error

            tmpl_attr = tmpl_attrs[attr['name']]

            if tmpl_attr.get('data_type') is not None:
                if res_attr.get('type') is not None:
                    if tmpl_attr.get('data_type') != res_attr.get('type'):
                        errors.append("Error in data. Template says that %s on %s is a %s, but data suggests it is a %s"%
                            (attr['name'], resource['name'], tmpl_attr.get('data_type'), res_attr.get('type')))

            attr_dimension = 'dimensionless' if attr.get('dimension') is None else attr.get('dimension')
            tmpl_attr_dimension = 'dimensionless' if tmpl_attr.get('dimension') is None else tmpl_attr.get('dimension')

            if attr_dimension.lower() != tmpl_attr_dimension.lower():
                errors.append("Dimension mismatch on resource %s for attribute %s"
                              " (template says %s on type %s, data says %s)"%
                              (resource['name'], attr.get('name'),
                               tmpl_attr.get('dimension'), res_user_type, attr_dimension))

            if check_unit is True:
                if tmpl_attr.get('unit') is not None:
                    if attr.get('unit') != tmpl_attr.get('unit'):
                        errors.append("Unit mismatch for resource %s with unit %s "
                                      "(template says %s for type %s)"
                                      %(resource['name'], attr.get('unit'),
                                        tmpl_attr.get('unit'), res_user_type))

    return errors

"""
+-------------------+
| Utility functions |
+-------------------+
"""
