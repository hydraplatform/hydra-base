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

import os
from copy import deepcopy
from lxml import etree

from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm.exc import NoResultFound
from .. import db

from .. import config
from ..util.dataset_util import array_dim
from ..util.dataset_util import arr_to_vector
from ..util.dataset_util import vector_to_arr
from ..db.model import Dataset, Unit, Dimension
from .objects import JSONObject
from ..exceptions import HydraError, ResourceNotFoundError

import json
from ..util.permissions import check_perm, required_perms
from sqlalchemy.orm import load_only

import numpy
import logging
log = logging.getLogger(__name__)


# NEW



"""
-------------------------------
 DIMENSION FUNCTIONS - VARIOUS
-------------------------------
"""

def exists_dimension(dimension_name,**kwargs):
    """
        Given a dimension returns its units as a list
    """
    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()
        # At this point the dimension exists
        return True
    except NoResultFound:
        # The dimension does not exist
        raise False

def is_global_dimension(dimension_name):
    """
        Returns True if the dimension is Global, False is it is assigned to a project
    """
    dimension = get_dimension_data(dimension_name)
    return (dimension.project_id is None)


"""
-------------------------------
UNIT FUNCTIONS - VARIOUS
-------------------------------
"""

def _parse_unit(unit):
    """
        Helper function that extracts constant factors from unit
        specifications. This allows to specify units similar to this: 10^6 m^3.
        Return a couple (unit, factor)
    """
    try:
        float(unit[0])
        factor, unit = unit.split(' ', 1)
        return unit, float(factor)
    except ValueError:
        return unit, 1.0

def is_global_unit(unit):
    """
        Returns True if the Unit is Global, False is it is assigned to a project
        'unit' is a Unit object
    """
    unit_data = get_unit_data(unit['abbr'])
    return (unit_data.project_id is None)

def extract_unit_abbreviation(unit):
    """
        Returns the abbreviation of a unit object, either if it is defined as "abbreviation" or "abbr"
        'unit' is a Unit object
    """
    unit = JSONObject(unit)
    if ('abbreviation' in unit) and (unit['abbreviation'] is not None):
        return unit['abbreviation']
    elif ('abbr' in unit) and (unit['abbr'] is not None):
        return unit['abbr']
    return None

def extract_unit_description(unit):
    """
        Returns the description of a unit object, either if it is defined as "description" or "info"
        'unit' is a Unit object
    """
    unit = JSONObject(unit)
    if ('description' in unit) and (unit['description'] is not None):
        return  unit['description']
    elif ('info' in unit) and (unit['info'] is not None):
        return  unit['info']
    return None


def convert_units(values, unit1, unit2,**kwargs):
    """Convert a value from one unit to another one.

    Example::

        >>> cli = PluginLib.connect()
        >>> cli.service.convert_units(20.0, 'm', 'km')
        0.02
    """
    if numpy.isscalar(values):
        # If it is a scalar, converts to an array
        values = [values]
    float_values = [float(value) for value in values]
    return _convert(float_values, unit1, unit2)

def _convert(values, unit1, unit2):
    """
        Convert a value from one unit to another one. The two units must
        represent the same physical dimension.
    """
    if get_unit_dimension(unit1) == get_unit_dimension(unit2):
        unit1, factor1 = _parse_unit(unit1)
        unit2, factor2 = _parse_unit(unit2)

        unit_data_1 = get_unit_data(unit1)
        unit_data_2 = get_unit_data(unit2)

        conv_factor1 = JSONObject({'lf': unit_data_1.lf, 'cf': unit_data_1.cf})
        conv_factor2 = JSONObject({'lf': unit_data_2.lf, 'cf': unit_data_2.cf})

        if isinstance(values, float):
            return (conv_factor1.lf / conv_factor2.lf * (factor1 * values)
                    + (conv_factor1.cf - conv_factor2.cf)
                    / conv_factor2.lf) / factor2
        elif isinstance(values, list):
            return [(conv_factor1.lf / conv_factor2.lf * (factor1 * value)
                    + (conv_factor1.cf - conv_factor2.cf)
                    / conv_factor2.lf) / factor2 for value in values]
    else:
        raise HydraError("Unit conversion: dimensions are not consistent.")


"""
---------------------------
 DIMENSION FUNCTIONS - GET
---------------------------
"""

def get_dimension(dimension_name,**kwargs):
    """
        Given a dimension returns its units as a list
    """
    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()
        # At this point the dimension exists
        units_list = db.DBSession.query(Unit).filter(Unit.dimension_id==dimension.id).all()
        u_list = []
        for u in units_list:
            u_list.append(str(u.abbreviation))

        return u_list
    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Dimension %s not found"%(dimension_name))

def get_dimensions():
    """
        Get the list of all dimensions keys.
    """
    dimensions_list = db.DBSession.query(Dimension).options(load_only("name")).all()
    dims_list = []
    for dim in dimensions_list:
        dims_list.append(str(dim.name))
    return dims_list

def get_all_dimensions():
    """
        Get an object having dimension name as key and le value is a list containing the units abbreviation of the dimension.
    """
    dimensions_list = db.DBSession.query(Dimension).options(load_only("name","id")).all()
    dimensions_obj = dict()
    for dimension in dimensions_list:
        dimensions_obj[str(dimension.name)] = []
        units_list = get_units_from_db(dimension_id = dimension.id)
        for unit in units_list:
            dimensions_obj[str(dimension.name)].append(unit.abbreviation)


    return dimensions_obj


def get_dimension_data(dimension_name):
    """
        Given a dimension returns all its data
    """
    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()

        return dimension
    except NoResultFound:
        # The dimension does not exist
        raise ResourceNotFoundError("Dimension %s not found"%(dimension_name))


"""
----------------------
 UNIT FUNCTIONS - GET
----------------------
"""

def get_units_from_db(**kwargs):
    """
        Gets all units from the DB table.
        If one of the arguments is defined it is used as filter
    """
    options = {
            'dimension_id' : None,
            'dimension_name' : None,
            'unit_abbreviation': None
            }
    options.update(kwargs)

    rs = None
    if options["dimension_id"] is not None:
        log.info("Looking for dimension_id '{}'".format(options["dimension_id"]))
        rs = db.DBSession.query(Unit).filter(Unit.dimension_id==options["dimension_id"]).all()
    elif options["dimension_name"] is not None:
        log.info("Looking for dimension_name '{}'".format(options["dimension_name"]))
        rs = db.DBSession.query(Unit).join(Dimension).filter(Dimension.name==options["dimension_name"]).all()
        log.info(rs)
    elif options["unit_abbreviation"] is not None:
        log.info("Looking for unit_abbreviation '{}'".format(options["unit_abbreviation"]))
        rs = db.DBSession.query(Unit).filter(Unit.abbreviation==options["unit_abbreviation"]).all()
        log.info(rs)
    else:
        rs = db.DBSession.query(Unit).all()

    return rs



def get_units(dim_name):
    """
        Gets a list of all units corresponding to a physical dimension.
    """
    unit_list = get_units_from_db(dimension_name = dim_name)
    log.info(unit_list)
    unit_dict_list = []
    for unit in unit_list:
        new_unit = dict(
            name = unit.name,
            abbr = unit.abbreviation,
            abbreviation = unit.abbreviation,
            lf = unit.lf,
            cf = unit.cf,
            dimension = dim_name,
            info = unit.description,
            description = unit.description
        )
        unit_dict_list.append(new_unit)
    log.info(unit_dict_list)
    return unit_dict_list


def get_unit_dimension(unit):
    """
        Return the physical dimension a given unit refers to.
    """

    unit, factor = _parse_unit(unit)


    units = get_units_from_db(unit_abbreviation = unit)

    if len(units) == 0:
        raise HydraError('Unit %s not found.'%(unit))
    elif len(units) > 1:
        raise HydraError('Unit %s has multiple dimensions not found.'%(unit))
    else:
        dimension = db.DBSession.query(Dimension).filter(Dimension.id==units[0].dimension_id).one()
        return str(dimension.name)

def get_unit_data(unit):
    """
        Return the full data of a given unit.
    """

    unit, factor = _parse_unit(unit)

    units = get_units_from_db(unit_abbreviation = unit)

    if len(units) == 0:
        raise HydraError('Unit %s not found.'%(unit))
    elif len(units) > 1:
        raise HydraError('Unit %s has multiple dimensions not found.'%(unit))
    else:
        log.info(JSONObject(units[0]).lf)
        return JSONObject(units[0])




"""
-----------------------------------------
 DIMENSION FUNCTIONS - ADD - DEL - UPD
-----------------------------------------
"""




@required_perms("add_dimension")
def add_dimension(dimension,**kwargs):
    """
        Add the dimension defined into the object "dimension" to the DB
        If dimension["project_id"] is None it means that the dimension is global, otherwise is property of a project
        If the dimension exists emits an exception
    """
    #user_id = kwargs.get('user_id')
    #log.info("user_id: {}".format(user_id))
    new_dimension = Dimension()
    new_dimension.name = dimension["name"]

    if "description" in dimension and dimension["description"] is not None:
        new_dimension.description = dimension["description"]
    if "project_id" in dimension and dimension["project_id"] is not None:
        new_dimension.project_id = dimension["project_id"]

    # Save on DB
    db.DBSession.add(new_dimension)
    db.DBSession.flush()



@required_perms("delete_dimension")
def delete_dimension(dimension_name,**kwargs):
    """
        Delete a dimension from the DB. Raises and exception if the dimension does not exist
    """

    try:
        #dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).filter(Dimension.project_id.isnot(None)).one()
        dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()
        log.info("delete_dimension 1")

        db.DBSession.query(Unit).filter(Unit.dimension_id==dimension.id).delete()

        db.DBSession.delete(dimension)
        db.DBSession.flush()
    except NoResultFound:
        raise ResourceNotFoundError("Dimension (dimension_name=%s) does not exist"%(dimension_name))

@required_perms("update_dimension")
def update_dimension(dimension,**kwargs):
    """
        Update a dimension in the DB.
        Raises and exception if the dimension does not exist.
        The key is ALWAYS the name and the name itself is not modificable
    """
    try:
        db_dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension['name']).filter().one()

        if "description" in dimension and dimension["description"] is not None:
            db_dimension.description = dimension["description"]
        if "project_id" in dimension and dimension["project_id"] is not None:
            db_dimension.project_id = dimension["project_id"]
    except NoResultFound:
        raise ResourceNotFoundError("Dimension (name=%s) does not exist"%(unit["abbreviation"]))


    db.DBSession.flush()
    return db_dimension

"""
-----------------------------------------
 UNIT FUNCTIONS - ADD - DEL - UPD
-----------------------------------------
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
                abbr      = 'tsp s^-1',
                cf        = 0,               # Constant conversion factor
                lf        = 1.47867648e-05,  # Linear conversion factor
                dimension = 'Volumetric flow rate',
                info      = 'A flow of one teaspoon per second.',
            )
            add_unit(new_unit)


    """
    # 'info' is the only field that is allowed to be empty
    if 'description' not in unit.keys() or unit['description'] is None:
        unit['description'] = ''

    dimension_data = get_dimension_data(unit["dimension"])

    new_unit = Unit()
    new_unit.dimension_id   = dimension_data.id
    new_unit.name           = unit['name']

    # Needed to uniform abbr to abbreviation
    if ('abbreviation' in unit) and (unit['abbreviation'] is not None):
        new_unit.abbreviation   = unit['abbreviation']
    elif ('abbr' in unit) and (unit['abbr'] is not None):
        new_unit.abbreviation   = unit['abbr']

    # Needed to uniform into to description
    if ('description' in unit) and (unit['description'] is not None):
        new_unit.description   = unit['description']
    elif ('info' in unit) and (unit['info'] is not None):
        new_unit.description   = unit['info']

    new_unit.lf             = unit['lf']
    new_unit.cf             = unit['cf']

    if ('project_id' in unit) and (unit['project_id'] is not None):
        # Adding dimension to the "user" dimensions list
        new_unit.project_id = unit['project_id']

    # Save on DB
    db.DBSession.add(new_unit)
    db.DBSession.flush()



@required_perms("delete_unit")
def delete_unit(unit, **kwargs):
    """
        Delete a unit from the DB.
        Raises and exception if the unit does not exist
    """

    try:
        db_unit = db.DBSession.query(Unit).join(Dimension).filter(Unit.abbreviation==unit['abbr']).filter(Dimension.name==unit['dimension']).filter().one()
        db.DBSession.delete(db_unit)
        db.DBSession.flush()
    except NoResultFound:
        raise ResourceNotFoundError("Unit (abbreviation=%s) does not exist"%(unit['abbr']))


@required_perms("update_unit")
def update_unit(unit, **kwargs):
    """
        Update a unit in the DB.
        Raises and exception if the unit does not existself.
        The key is ALWAYS the abbreviation
    """
    try:
        db_unit = db.DBSession.query(Unit).join(Dimension).filter(Unit.abbreviation==unit['abbr']).filter(Dimension.name==unit['dimension']).filter().one()
        db_unit.name = unit["name"]
        db_unit.description = unit["description"]
        db_unit.lf = unit["lf"]
        db_unit.cf = unit["cf"]
        if "project_id" in unit:
            db_unit.project_id = unit["project_id"]
    except NoResultFound:
        raise ResourceNotFoundError("Unit (abbreviation=%s) does not exist"%(unit["abbreviation"]))


    db.DBSession.flush()
    return db_unit


"""
-----------------
 OTHER FUNCTIONS
-----------------
"""






def check_consistency(unit, dimension):
    """
        Check whether a specified unit is consistent with the physical
        dimension asked for by the attribute or the dataset.
    """
    unit_abbr, factor = _parse_unit(unit)
    dim = get_unit_dimension(unit_abbr)
    log.info(dim)
    return dim == dimension




# OLD

class Units(object):
    """
    This class provides functionality for unit conversion and checking of
    consistency between units and dimensions. Unit conversion factors are
    defined in a static built-in XML file and in a custom file defined by
    the user. The location of the unit conversion file provided by the user
    is specified in the config file in section ``[unit conversion]``. This
    section and a file specifying custom unit conversion factors are optional.
    """

    unittree = None
    usertree = None

    dimensions = dict()
    units = dict()
    userunits = []
    userdimensions = []
    static_dimensions = []
    unit_description = dict()
    unit_info = dict()

    dimensions_2 = dict()
    units_2 = dict()
    unit_description_2 = dict()
    unit_info_2 = dict()
    static_dimensions_2 = []
    userunits_2 = []
    userdimensions_2 = []

    full_data = dict() # This will contain all the data from the DB


    def __init__(self):
        db.connect() # Connection to the DB
        return

        #self.get_units_from_db()
        dimensions_list = self.get_dimensions_from_db()

        user_dimensions_list = self.get_dimensions_from_db(True)

        default_user_file_location = os.path.realpath(\
            os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         '../',
                         'static',
                         'user_units.xml'))

        user_unitfile = config.get("unit_conversion",
                                       "user_file",
                                       default_user_file_location)

        #If the user unit file doesn't exist, create it.
        if not os.path.exists(user_unitfile):
            open(user_unitfile, 'a').close()

        default_builtin_unitfile_location = \
                os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '../'
                             'static',
                             'unit_definitions.xml')

        builtin_unitfile = config.get("unit_conversion",
                                       "default_file",
                                       default_builtin_unitfile_location)

        log.debug("Default unitfile: %s", builtin_unitfile)
        log.debug("User unitfile: %s", user_unitfile)

        with open(builtin_unitfile) as f:
            self.unittree = etree.parse(f).getroot()

        # for element in self.unittree:
        #     self.static_dimensions_2.append(element.get('name'))

        with open(user_unitfile) as f:
            self.usertree = etree.parse(f).getroot()

        with open(builtin_unitfile) as f:
            self.unittree = etree.parse(f).getroot()

        # for element in self.usertree:
        #     self.unittree.append(deepcopy(element))
        #     self.userdimensions_2.append(element.get('name'))
        #     for subelement in element:
        #         self.userunits_2.append(subelement.get('abbr'))

        # DB Based
        for dimension in dimensions_list:
            # log.info(dimension)
            # log.info(dimension.name)
            #self.userdimensions.append(dim.name)
            new_dimension = JSONObject({})
            new_dimension.id = dimension.id
            new_dimension.name= dimension.name
            new_dimension.description= dimension.description
            new_dimension.units= []

            self.full_data[dimension.name] = new_dimension

            if dimension not in self.dimensions.keys():
                self.dimensions.update({str(dimension.name): []})

            if dimension not in self.dimensions.keys():
                self.static_dimensions.append(dimension.name)

            units_list = self.get_units_from_db(dimension.id)

            for unit in units_list:
                new_unit = JSONObject({})
                new_unit.id = unit.id
                new_unit.dimension_id = unit.dimension_id
                new_unit.name = unit.name
                new_unit.abbreviation = unit.abbreviation
                new_unit.description = unit.description
                new_unit.lf = unit.lf
                new_unit.cf = unit.cf
                new_unit.project_id = unit.project_id

                self.full_data[dimension.name].units.append(new_unit)



                self.dimensions[dimension.name].append(unit.abbreviation)
                self.units.update({unit.abbreviation:
                                   (float(unit.lf),
                                    float(unit.cf))})
                self.unit_description.update({unit.abbreviation:
                                              unit.name})
                self.unit_info.update({unit.abbreviation:
                                       unit.description})

        for dimension in user_dimensions_list:
            self.userdimensions.append(dimension.name)
            units_list = self.get_units_from_db(dimension.name)
            for unit in units_list:
                self.userunits.append(unit.abbreviation)

        log.info(self.full_data)

        log.info(self.get_dimension_from_db_by_name("Length"))

        log.info(self.get_unit_from_db_by_abbreviation("s"))

    def do_deep_compare(self, left, right, message_to_show):
        """
            Utility function. Does a deep compare of two objects and show an error in case of failure
        """
        if self.deep_compare(left, right):
             log.info("{} are OK".format(message_to_show))
        else:
             log.critical("{} are WRONG".format(message_to_show))
             log.info(left)
             log.info(right)



    def deep_compare(self,left, right, level=0):
        """
            Utility function. Does a deep compare of two objects and returns the comparison success status
        """
        if type(left) != type(right):
            log.info("Exit 1 - Different types")
            return False

        elif type(left) is dict:
            # Dict comparison
            for key in left:
                if key not in right:
                    log.info("Exit 2 - missing {} in right".format(key))
                    return False
                else:
                    if not self.deep_compare(left[str(key)], right[str(key)], level +1 ):
                        log.info("Exit 3 - different children")
                        return False
            return True
        elif type(left) is list:
            # List comparison
            for key in left:
                if key not in right:
                    log.info("Exit 4 - missing {} in right".format(key))
                    return False
                else:
                    if not self.deep_compare(left[left.index(key)], right[right.index(key)], level +1 ):
                        log.info("Exit 5 - different children")
                        return False
            return True
        else:
            # Other comparison
            return left == right

        return False


    def get_units_from_db(self, dimension_id=None):
        """
            Gets all units from the DB table. If dimension_id is specified it is used as filter
        """
        rs = None
        if dimension_id==None:
            rs = db.DBSession.query(Unit).all()
        else:
            rs = db.DBSession.query(Unit).filter(Unit.dimension_id==dimension_id).all()
        #log.info(rs)
        return rs

    def get_dimensions_from_db(self, are_user_dimensions=False):
        """
            Gets all dimension from the DB table.
        """
        if not are_user_dimensions:
            rs = db.DBSession.query(Dimension).all()
        else:
            rs = db.DBSession.query(Dimension).filter(Dimension.project_id.isnot(None)).all()

        #log.info(rs)
        return rs

    def get_dimension_from_db_by_name(self, dimension_name):
        """
            Gets a dimension from the DB table.
        """
        try:
            dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()
            return JSONObject(dimension)
        except NoResultFound:
            raise ResourceNotFoundError("Dimension %s not found"%(dimension_name))

    def get_unit_from_db_by_abbreviation(self, unit_abbr):
        """
            Gets a Unit from the DB table.
        """
        try:
            unit = db.DBSession.query(Unit).filter(Unit.abbreviation==unit_abbr).one()
            return JSONObject(unit)
        except NoResultFound:
            raise ResourceNotFoundError("Unit %s not found"%(unit_abbr))

    # def check_consistency(self, unit, dimension):
    #     """
    #         Check whether a specified unit is consistent with the physical
    #         dimension asked for by the attribute or the dataset.
    #     """
    #     unit, factor = self.parse_unit(unit)
    #     return unit in self.dimensions[dimension]

    def get_dimension(self, unit):
        """
            Return the physical dimension a given unit refers to.
        """

        unit, factor = self.parse_unit(unit)
        for dim in self.dimensions.keys():
            if unit in self.dimensions[dim]:
                return dim
        raise HydraError('Unit %s not found.'%(unit))

    def convert(self, values, unit1, unit2):
        """
            Convert a value from one unit to another one. The two units must
            represent the same physical dimension.
        """
        if self.get_dimension(unit1) == self.get_dimension(unit2):
            unit1, factor1 = self.parse_unit(unit1)
            unit2, factor2 = self.parse_unit(unit2)
            conv_factor1 = self.units[unit1]
            conv_factor2 = self.units[unit2]

            if isinstance(values, float):
                return (conv_factor1[0] / conv_factor2[0] * (factor1 * values)
                        + (conv_factor1[1] - conv_factor2[1])
                        / conv_factor2[0]) / factor2
            elif isinstance(values, list):
                return [(conv_factor1[0] / conv_factor2[0] * (factor1 * value)
                        + (conv_factor1[1] - conv_factor2[1])
                        / conv_factor2[0]) / factor2 for value in values]
        else:
            raise HydraError("Unit conversion: dimensions are not consistent.")

    def parse_unit(self, unit):
        """
            Helper function that extracts constant factors from unit
            specifications. This allows to specify units similar to this: 10^6 m^3.
        """
        try:
            float(unit[0])
            factor, unit = unit.split(' ', 1)
            return unit, float(factor)
        except ValueError:
            return unit, 1.0

    def get_dimensions(self):
        """
            Get a list of all dimensions keys listed in one of the xml files.
        """
        return self.dimensions.keys()

    def get_all_dimensions(self):
        """
            Get the list of all dimensions objects listed in one of the xml files.
        """
        return self.dimensions

    def get_units(self, dimension):
        """
            Get a list of all units describing one specific dimension.
        """
        unitlist = []
        for unit in self.dimensions[dimension]:
            unitdict = dict()
            unitdict.update({'abbr': unit})
            unitdict.update({'name': self.unit_description[unit]})
            unitdict.update({'lf': self.units[unit][0]})
            unitdict.update({'cf': self.units[unit][1]})
            unitdict.update({'dimension': dimension})
            unitdict.update({'info': self.unit_info[unit]})
            unitlist.append(unitdict)
        return unitlist







"""
-------------------
 DATASET functions
-------------------
"""


def convert_dataset(dataset_id, to_unit,**kwargs):
    """
        Convert a whole dataset (specified by 'dataset_id' to new unit
        ('to_unit'). Conversion ALWAYS creates a NEW dataset, so function
        returns the dataset ID of new dataset.
    """

    ds_i = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()

    dataset_type = ds_i.type

    dsval = ds_i.get_val()
    old_unit = ds_i.unit

    if old_unit is not None:
        if dataset_type == 'scalar':
            new_val = _convert(float(dsval), old_unit, to_unit)
        elif dataset_type == 'array':
            dim = array_dim(dsval)
            vecdata = arr_to_vector(dsval)
            newvec = _convert(vecdata, old_unit, to_unit)
            new_val = vector_to_arr(newvec, dim)
        elif dataset_type == 'timeseries':
            new_val = []
            for ts_time, ts_val in dsval.items():
                dim = array_dim(ts_val)
                vecdata = arr_to_vector(ts_val)
                newvec = _convert(vecdata, old_unit, to_unit)
                newarr = vector_to_arr(newvec, dim)
                new_val.append(ts_time, newarr)
        elif dataset_type == 'descriptor':
            raise HydraError('Cannot convert descriptor.')

        new_dataset = Dataset()
        new_dataset.type   = ds_i.type
        new_dataset.value  = str(new_val) # The data type is TEXT!!!
        new_dataset.name   = ds_i.name
        new_dataset.unit   = to_unit
        new_dataset.hidden = 'N'
        new_dataset.set_metadata(ds_i.get_metadata_as_dict())
        new_dataset.set_hash()

        existing_ds = db.DBSession.query(Dataset).filter(Dataset.hash==new_dataset.hash).first()

        log.info(new_dataset.value)

        if existing_ds is not None:
            db.DBSession.expunge_all()
            return existing_ds.id

        db.DBSession.add(new_dataset)
        db.DBSession.flush()

        return new_dataset.id

    else:
        raise HydraError('Dataset has no units.')


global hydra_units
hydra_units = Units()


# def get_unit_dimension(unit1,**kwargs):
#     """Get the corresponding physical dimension for a given unit.
#
#     Example::
#
#         >>> cli = PluginLib.connect()
#         >>> cli.service.get_dimension('m')
#         Length
#     """
#     return hydra_units.get_dimension(unit1)

# def get_dimensions(**kwargs):
#     """Get a list of all physical dimensions available on the server.
#     """
#     dim_list = hydra_units.get_dimensions()
#     return dim_list

# def get_all_dimensions(**kwargs):
#     db_units = db.DBSession.query(Unit).all()
#     log.info(db_units)
#     return hydra_units.get_all_dimensions()

# def get_units(dimension,**kwargs):
#     """Get a list of all units corresponding to a physical dimension.
#     """
#     unit_list = hydra_units.get_units(dimension)
#     unit_dict_list = []
#     for unit in unit_list:
#         cm_unit = dict(
#             name = unit['name'],
#             abbr = unit['abbr'],
#             lf = unit['lf'],
#             cf = unit['cf'],
#             dimension = unit['dimension'],
#             info = unit['info'],
#         )
#         unit_dict_list.append(cm_unit)
#     return unit_dict_list

# def check_consistency(unit, dimension,**kwargs):
#     """Check if a given units corresponds to a physical dimension.
#     """
#     return hydra_units.check_consistency(unit, dimension)

"""
--------------------
 RESOURCE functions
--------------------
"""

def validate_resource_attributes(resource, attributes, template, check_unit=True, exact_match=False):
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

# if __name__ == '__main__':
#     units = Units()
#     for dim in units.unittree:
#         print('**' + dim.get('name') + '**')
#         for unit in dim:
#             print(unit.get('name'), unit.get('abbr'), unit.get('lf'),
#                   unit.get('cf'), unit.get('info'))
#
#     print(units.convert(200, 'm^3', 'ac-ft'))
