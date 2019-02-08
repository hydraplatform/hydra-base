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

import hydra_base as hb
from hydra_base.exceptions import HydraError, ResourceNotFoundError
from hydra_base.lib.objects import JSONObject
from fixtures import *
import pytest
import util
import logging
log = logging.getLogger(__name__)
import util
import sys
import json
# from ..hydra_base.util.dataset_util import arr_to_vector

"""
+----------------------------+
| NEEDED for testing dataset |
+----------------------------+
"""

def arr_to_vector(arr):
    """Reshape a multidimensional array to a vector.
    """
    dim = array_dim(arr)
    tmp_arr = []
    for n in range(len(dim) - 1):
        for inner in arr:
            for i in inner:
                tmp_arr.append(i)
        arr = tmp_arr
        tmp_arr = []
    return arr

def array_dim(arr):
    """Return the size of a multidimansional array.
    """
    dim = []
    while True:
        try:
            dim.append(len(arr))
            arr = arr[0]
        except TypeError:
            return dim



class TestUnits():
    """
        Test for working with units.
    """
    def test_get_dimensions(self, session):

        dimension_list = hb.get_dimensions()
        log.info(dimension_list)
        assert dimension_list is not None and len(dimension_list) != 0, \
            "Could not get a list of dimensions names."

    def test_get_all_dimensions(self, session):

        dimension_list = hb.get_all_dimensions()
        log.info(dimension_list)
        assert dimension_list is not None and len(dimension_list) != 0, \
            "Could not get the list of all dimensions."

    def test_get_all_dimensions_data(self, session):

        dimension_list = hb.get_all_dimensions_data()
        log.info(dimension_list)
        assert dimension_list is not None and len(dimension_list) != 0, \
            "Could not get the list of all dimensions."

    def test_get_units(self, session):

        dimension_list = hb.get_dimensions() # Dimensions names list
        units_found = 0
        log.info(dimension_list)
        for dimension in dimension_list:
            units_list = hb.get_units(dimension)
            assert units_list is not None, \
                "Could not get a list of units from hydra_base.get_units"
            units_found+=len(units_list)
        assert units_found != 0, \
            "hydra_base.get_units Could not get any units from the source"

    def test_get_dimension(self, session):

        testdim = 'Length'
        resultdim = hb.get_dimension(testdim)
        assert len(resultdim) > 0, \
            "Getting dimension for 'kilometers' didn't work."

        with pytest.raises(ResourceNotFoundError):
            #dimension = hb.get_dimension('not-existing-dimension')
            dimension = hb.get_dimension('not-existing-dimension')

    def test_get_dimension_by_id(self, session):

        testdim = 1
        resultdim = hb.get_dimension_by_id(testdim)
        assert len(resultdim) > 0, \
            "Getting dimension for 'kilometers' didn't work."

        with pytest.raises(ResourceNotFoundError):
            dimension = hb.get_dimension_by_id(2000)

    def test_get_dimension_data(self, session):

        testdim = 'Length'
        resultdim = hb.get_dimension_data(testdim)
        assert resultdim["name"] == testdim, \
            "Getting dimension for 'kilometers' didn't work."

        with pytest.raises(ResourceNotFoundError):
            dimension = hb.get_dimension_data('not-existing-dimension')

    def test_get_unit_dimension(self, session):

        testdim = 'Length'
        testunit = 'km'
        resultdim = hb.get_unit_dimension(testunit)

        assert testdim == resultdim, \
            "Getting dimension for 'kilometers' didn't work."

        with pytest.raises(HydraError):
            dimension = hb.get_unit_dimension('not-existing-unit')

    def test_add_dimension(self, session):

        # Try to add an existing dimension
        testdim = {'name': 'Length'}
        with pytest.raises(Exception) as excinfo:
            hb.add_dimension(testdim, user_id=pytest.root_user_id)

        # IN this way we can test that the exception is specifically what we want and not other exceptions type
        assert "IntegrityError" in JSONObject(excinfo.type)["__doc__"], \
            "Adding existing dimension didn't work as expected: {}".format(JSONObject(excinfo)["_excinfo"])

        # Needed to reset the exception
        hb.db.DBSession.rollback()
        # Add a new dimension
        testdim = {'name':'Electric current'}
        hb.add_dimension(testdim, user_id = pytest.root_user_id)

        dimension_list = list(hb.get_dimensions())
        assert testdim["name"] in dimension_list, \
            "Adding new dimension didn't work as expected."
        hb.db.DBSession.rollback()


        # Add a new dimension as scalar
        testdim = 'Electric current'
        hb.add_dimension(testdim, user_id = pytest.root_user_id)

        dimension_list = list(hb.get_dimensions())
        assert testdim in dimension_list, \
            "Adding new dimension didn't work as expected."
        hb.db.DBSession.rollback()


    def test_update_dimension(self, session):
        # Updating existing dimension
        testdim = {
                    'name':'Length',
                    'description': 'New description'
                    }
        hb.update_dimension(testdim, user_id=pytest.root_user_id)

        modified_dim = hb.get_dimension_data(testdim["name"])
        assert modified_dim.description == testdim["description"], \
                "Updating a dimension didn't work"


    def test_delete_dimension(self, session):
        # Add a new dimension and delete it

        # Test adding the object and deleting the name
        testdim = {'name':'Electric current'}
        hb.add_dimension(testdim, user_id=pytest.root_user_id)
        old_dimension_list = list(hb.get_dimensions())

        hb.delete_dimension(testdim["name"], user_id=pytest.root_user_id)

        new_dimension_list = list(hb.get_dimensions())

        log.info(new_dimension_list)

        assert testdim["name"] in old_dimension_list and \
            testdim["name"] not in new_dimension_list, \
            "Deleting dimension didn't work."

        # Test adding the name and deleting by object
        testdim = {'name':'Electric current'}
        hb.add_dimension(testdim["name"], user_id=pytest.root_user_id)
        old_dimension_list = list(hb.get_dimensions())

        hb.delete_dimension(testdim, user_id=pytest.root_user_id)

        new_dimension_list = list(hb.get_dimensions())

        log.info(new_dimension_list)

        assert testdim["name"] in old_dimension_list and \
            testdim["name"] not in new_dimension_list, \
            "Deleting dimension didn't work."

    def test_add_unit(self, session):
        # Add a new unit to an existing static dimension
        new_unit = JSONObject({})
        new_unit.name = 'Teaspoons per second'
        new_unit.abbr = 'tsp s^-1'
        new_unit.cf = 0               # Constant conversion factor
        new_unit.lf = 1.47867648e-05  # Linear conversion factor
        new_unit.dimension = 'Volumetric flow rate'
        new_unit.info = 'A flow of one tablespoon per second.'
        hb.add_unit(new_unit, user_id=pytest.root_user_id)

        unitlist = list(hb.get_units(new_unit.dimension))

        #log.info(unitlist)

        unitabbr = []
        for unit in unitlist:
            unitabbr.append(unit["abbr"])

        assert new_unit.abbr in unitabbr, \
            "Adding new unit didn't work."


        # Add a new unit to a custom dimension
        testdim = {'name':'Test dimension'}
        hb.add_dimension(testdim, user_id=pytest.root_user_id)

        testunit = JSONObject({})
        testunit.name = 'Test'
        testunit.abbr = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension = testdim["name"]

        result = hb.add_unit(testunit, user_id=pytest.root_user_id)


        unitlist = list(hb.get_units(testdim["name"]))
        #log.info(unitlist)
        assert len(unitlist) == 1, \
            "Adding a new unit didn't work as expected"

        assert unitlist[0]["name"] == 'Test', \
            "Adding a new unit didn't work as expected"

        hb.delete_dimension(testdim["name"], user_id=pytest.root_user_id)



    def test_update_unit(self, session):
        # Add a new unit to a new dimension

        testdim = {'name':'Test dimension'}
        hb.add_dimension(testdim, user_id=pytest.root_user_id)

        testunit = JSONObject({})
        testunit.name = 'Test'
        testunit.abbr = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension = testdim["name"]
        hb.add_unit(testunit, user_id=pytest.root_user_id)

        # Update it
        testunit.cf = 0
        hb.update_unit(testunit, user_id=pytest.root_user_id)

        unitlist = list(hb.get_units(testdim["name"]))

        assert len(unitlist) > 0 and int(unitlist[0]['cf']) == 0, \
            "Updating unit didn't work correctly."

        hb.delete_dimension(testdim["name"], user_id=pytest.root_user_id)

    def test_delete_unit(self, session):
        # Add a new unit to a new dimension

        testdim = {'name':'Test dimension'}
        hb.add_dimension(testdim, user_id=pytest.root_user_id)

        testunit = JSONObject({})
        testunit.name = 'Test'
        testunit.abbr = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension = testdim["name"]
        hb.add_unit(testunit, user_id=pytest.root_user_id)

        # Check if the unit has been added
        unitlist = hb.get_units(testunit.dimension)


        assert len(unitlist) > 0 and unitlist[0]['abbr'] == testunit.abbr, \
            "The adding has not worked properly"

        result = hb.delete_unit(testunit, user_id=pytest.root_user_id)

        unitlist = hb.get_units(testunit.dimension)

        assert len(unitlist) == 0, \
            "Deleting unit didn't work correctly."

        hb.delete_dimension(testunit.dimension, user_id=pytest.root_user_id)

    def test_convert_units(self, session):

        result = hb.convert_units(20, 'm', 'km')
        assert result == [0.02], \
            "Converting metres to kilometres didn't work."

        result = hb.convert_units([20., 30., 40.], 'm', 'km')
        assert result == [0.02, 0.03, 0.04],  \
            "Unit conversion of array didn't work."

        result = hb.convert_units(20, '2e6 m^3', 'hm^3')
        assert result == [40], "Conversion with factor didn't work correctly."



    def test_check_consistency(self, session):
        result1 = hb.check_consistency('m^3', 'Volume')
        result2 = hb.check_consistency('m', 'Volume')
        assert result1 is True, \
            "Unit consistency check didn't work."
        assert result2 is False, \
            "Unit consistency check didn't work."



    def test_is_global_dimension(self, session):
        result = hb.is_global_dimension('Length')
        assert result is True, \
            "Is global dimension check didn't work."

    def test_is_global_unit(self, session):
        result = hb.is_global_unit({'abbr':'m'})
        assert result is True, \
            "Is global unit check didn't work."


    def test_extract_unit_abbreviation(self, session):
        assert hb.extract_unit_abbreviation({'abbr': 'test'}) == 'test', \
            "extract_unit_abbreviation didn't work."

        assert hb.extract_unit_abbreviation({'abbreviation': 'test'}) == 'test', \
            "extract_unit_abbreviation didn't work."

        assert hb.extract_unit_abbreviation({}) is None, \
            "extract_unit_abbreviation didn't work."

    def test_extract_unit_description(self, session):
        assert hb.extract_unit_description({'info': 'test'}) == 'test', \
            "extract_unit_description didn't work."

        assert hb.extract_unit_description({'description': 'test'}) == 'test', \
            "extract_unit_description didn't work."

        assert hb.extract_unit_description({}) is None, \
            "extract_unit_description didn't work."


    def test_convert_dataset(self, session):
        project = util.create_project()

        network = util.create_network_with_data(num_nodes=2, project_id=project.id)

        scenario = \
            network.scenarios[0].resourcescenarios

        # Select the first array (should have untis 'bar') and convert it
        for res_scen in scenario:
            if res_scen.value.type == 'array':
                dataset_id = res_scen.value.id
                old_val = res_scen.value.value
                break
        newid = hb.convert_dataset(dataset_id, 'mmHg')

        assert newid is not None
        assert newid != dataset_id, "Converting dataset not completed."
        log.info(newid)

        new_dataset = hb.get_dataset(newid, user_id = pytest.root_user_id)
        new_val = new_dataset.value

        new_val = arr_to_vector(json.loads(new_val))
        old_val = arr_to_vector(json.loads(old_val))

        old_val_conv = [i * 100000 / 133.322 for i in old_val]

        # Rounding is not exactly the same on the server, that's why we
        # calculate the sum.
        assert sum(new_val) - sum(old_val_conv) < 0.00001, \
            "Unit conversion did not work"


if __name__ == '__main__':
    server.run()
