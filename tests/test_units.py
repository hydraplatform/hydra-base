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
import sys
import logging
import json
import pytest
import datetime

from hydra_base.exceptions import HydraError
from hydra_base.lib.objects import JSONObject
import hydra_base as hb

log = logging.getLogger(__name__)

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
    def test_get_empty_dimension(self, client):
        dimension = client.get_empty_dimension()
        assert dimension.id is None, "get_empty_dimension didn't work as expected"


    def test_get_dimension(self, client):
        # Returns a dimension searching by ID. The result contains all the
        # dimension data plus all the units of the dimension
        # 1 is the id of length
        dimension = client.get_dimension(1)

        assert dimension is not None, \
            "Could not get a dimensions by id."
        assert dimension.name == "Length", \
            "Could not get the dimension 'Length' by ID 1."
        assert len(dimension.units) > 0, \
            "Could not get the dimension units for Dimension ID 1."

        with pytest.raises(HydraError):
            # It must raise an exception
            dimension = client.get_dimension(None, do_accept_dimension_id_none=False)

        dimension = client.get_dimension(None, do_accept_dimension_id_none=True)
        assert dimension is not None, \
            "Could not get a dimension by None."
        assert dimension.id is None, \
            "Could not get a dimension by None."


    def test_get_dimensions(self, client):
        # Returns an array of dimensions. Every item contains the
        # dimension data plus the units list
        dimension_list = client.get_dimensions()

        assert dimension_list is not None and len(dimension_list) != 0, \
            "Could not get the full list of dimensions."
        assert dimension_list[0].id is not None, \
            "The first element of the list has no ID.  An ID is expected on a dimension"

    def test_get_dimension_by_name(self, client):
        # Returns a dimension searching by ID. The result contains all the
        # dimension data plus all the units of the dimension
        # 1 is the id of length
        dimension = client.get_dimension_by_name("Length")

        assert dimension is not None, \
            "Could not get a dimensions by name."
        assert dimension.id == 1, \
            "Could not get the dimension 1 by aname 'Length'."
        assert len(dimension.units) > 0, \
            "Could not get the dimension units for Dimension ID 1."

    def test_get_unit(self, client):
        # Returns a dimension searching by ID. The result contains all the
        # dimension data plus all the units of the dimension
        # 1 is the id of length
        unit = client.get_unit(2)

        assert unit is not None, \
            "Could not get a unit by id."
        assert unit.abbreviation == "AU", \
            "Could not get the unit 'AU (astronomical unit)' by ID 2."

    def test_get_unit_by_abbreviation(self, client):
        # Returns a dimension searching by ID. The result contains all the
        # dimension data plus all the units of the dimension
        # 1 is the id of length
        unit = client.get_unit_by_abbreviation("AU")

        assert unit is not None, \
            "Could not get a unit by abbreviation."
        assert unit.id == 2, \
            "Could not get the unit '(astronomical unit)' with ID 2 by abbreviation 'AU'."

    def test_get_units(self, client):
        # Returns an array of dimensions. Every item contains the dimension data plus the units list
        units_list = client.get_units()

        assert units_list is not None and len(units_list) != 0, \
            "Could not get the full list of units."


    def test_get_dimension_by_unit_measure_or_abbreviation(self, client):

        testdim = 'Length'
        testunit = 'km'
        resultdim = client.get_dimension_by_unit_measure_or_abbreviation(testunit)

        assert testdim == resultdim.name, \
            "Getting dimension for 'km' didn't work."

        with pytest.raises(HydraError):
            dimension = client.get_dimension_by_unit_measure_or_abbreviation('not-existing-unit')

    def test_get_dimension_by_unit_id(self, client):
        # Returns a dimension searching by ID. The result contains all the
        # dimension data plus all the units of the dimension
        # 1 is the id of length

        # Referring dimension
        testdim = JSONObject({'name':'Length', 'id': 1})
        # Units list of the dimension
        units = client.get_dimension(testdim.id).units
        # The first unit id
        test_unit_id = units[0].id
        # The dimension relative to the first unit id
        dimension = client.get_dimension_by_unit_id(test_unit_id)

        assert dimension is not None, \
            "Could not get a dimension by unit id."
        assert dimension.id == testdim.id, \
            "Could not get the dimension '{}' by unit id {}.".format(testdim.name, test_unit_id)

        with pytest.raises(HydraError):
            # It must raise an exception
            dimension = client.get_dimension_by_unit_id(None, do_accept_unit_id_none=False)

        dimension = client.get_dimension_by_unit_id(None, do_accept_unit_id_none=True)
        assert dimension is not None, \
            "Could not get a dimension by None."
        assert dimension.id is None, \
            "Could not get a dimension by None."


    """
        Manipulate DIMENSIONS
    """

    def test_add_dimension(self, client):

        # Try to add an existing dimension
        testdim = {'name': 'Length'}
        with pytest.raises(Exception) as excinfo:
            client.add_dimension(testdim)


        # IN this way we can test that the exception is specifically what we
        # want and not other exceptions type
        assert "already exists" in str(excinfo.value)

        # Add a new dimension
        testdim = {'name':'Electric current'}
        client.add_dimension(testdim)

        dimension_list = list(client.get_dimensions())

        assert testdim['name'] in [d.name for d in dimension_list]


    def test_update_dimension(self, client):
        # Updating existing dimension
        testdim = {
            'id': 1,
            'name':'Length',
            'description': 'New description'
        }
        modified_dim = client.update_dimension(testdim)

        assert modified_dim.description == testdim["description"], \
                "Updating a dimension didn't work"


    def test_delete_dimension(self, client):
        # Add a new dimension and delete it

        # Test adding the object and deleting it
        testdim = {'name':f'Dimension {datetime.datetime.now()}'}
        new_dimension = client.add_dimension(testdim)

        old_dimension_list = list(client.get_dimensions())

        client.delete_dimension(new_dimension.id)

        new_dimension_list = list(client.get_dimensions())

        assert len(list(filter(lambda x: x["name"] == testdim["name"], old_dimension_list))) > 0 and \
               len(list(filter(lambda x: x["name"] == testdim["name"], new_dimension_list))) == 0,\
            "Deleting a dimension didn't work as expected."

    """
        Manipulate UNITS
    """
    def test_add_unit(self, client):
        # Add a new unit to an existing static dimension
        new_unit = JSONObject({})
        new_unit.name = 'Teaspoons per second'
        new_unit.abbreviation = 'tsp s^-1'
        new_unit.cf = 0               # Constant conversion factor
        new_unit.lf = 1.47867648e-05  # Linear conversion factor

        new_unit.dimension_id = client.get_dimension_by_name('Volumetric flow rate').id
        new_unit.info = 'A flow of one tablespoon per second.'
        client.add_unit(new_unit)

        dimension_loaded = client.get_dimension(new_unit.dimension_id)

        assert len(list(filter(lambda x: x["name"] == new_unit["name"], dimension_loaded.units))) > 0 , \
            "Adding new unit didn't work."


        # Add a new unit to a custom dimension
        testdim = {'name':'Test dimension'}
        new_dimension = client.add_dimension(testdim)

        testunit = JSONObject({})
        testunit.name = 'Test'
        testunit.abbreviation = 'ttt'
        testunit.cf = 21
        testunit.lf = 42

        testunit.dimension_id = new_dimension.id

        new_unit = client.add_unit(testunit)

        new_dimension_loaded = client.get_dimension(new_dimension.id)

        unitlist = list(new_dimension_loaded.units)

        assert len(unitlist) == 1, \
            "Adding a new unit didn't work as expected"

        assert unitlist[0]["name"] == 'Test', \
            "Adding a new unit didn't work as expected"

        client.delete_dimension(new_dimension.id)



    def test_update_unit(self, client):
        # Add a new unit to a new dimension

        testdim = {'name':'Test dimension'}
        new_dimension = client.add_dimension(testdim)

        testunit = JSONObject({})
        testunit.name = 'Test'
        testunit.abbreviation = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension_id = new_dimension.id
        new_unit = client.add_unit(testunit)

        # Update it
        new_unit.cf = 0
        new_unit_modified = client.update_unit(new_unit)

        unitlist = list(client.get_dimension(new_dimension.id).units)

        assert len(unitlist) > 0 and int(unitlist[0]['cf']) == 0, \
            "Updating unit didn't work correctly."

        client.delete_dimension(new_dimension.id)

    def test_delete_unit(self, client):
        # Add a new unit to a new dimension

        testdim = {'name':'Test dimension'}
        new_dimension = client.add_dimension(testdim)

        testunit = JSONObject({})
        testunit.name = 'Test'
        testunit.abbreviation = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension_id = new_dimension.id
        new_unit = client.add_unit(testunit)

        # Check if the unit has been added
        unitlist = list(client.get_dimension(new_dimension.id).units)


        assert len(unitlist) > 0 and unitlist[0]['abbreviation'] == testunit.abbreviation, \
            "The adding has not worked properly"

        result = client.delete_unit(new_unit.id)

        unitlist = list(client.get_dimension(new_dimension.id).units)

        assert len(unitlist) == 0, \
            "Deleting unit didn't work correctly."

        client.delete_dimension(new_dimension.id)

    def test_convert_units(self, client):

        result = client.convert_units(20, 'm', 'km')
        assert result == [0.02], \
            "Converting metres to kilometres didn't work."

        result = client.convert_units([20., 30., 40.], 'm', 'km')
        assert result == [0.02, 0.03, 0.04],  \
            "Unit conversion of array didn't work."

        result = client.convert_units(20, '2e6 m^3', 'hm^3')
        assert result == [40], "Conversion with factor didn't work correctly."



    def test_check_consistency(self, client):
        result1 = client.check_consistency('m^3', 'Volume')
        result2 = client.check_consistency('m', 'Volume')
        assert result1 is True, \
            "Unit consistency check didn't work."
        assert result2 is False, \
            "Unit consistency check didn't work."



    def test_is_global_dimension(self, client):
        result = client.is_global_dimension(1)
        assert result is True, \
            "Is global dimension check didn't work."

    def test_is_global_unit(self, client):
        result = client.is_global_unit(1)
        assert result is True, \
            "Is global unit check didn't work."

    def test_convert_dataset(self, client):
        project = client.testutils.create_project()

        network = client.testutils.create_network_with_data(num_nodes=2, project_id=project.id)

        resourcescenarios = \
            network.scenarios[0].resourcescenarios

        # Select the first array (should have untis 'bar') and convert it
        for res_scen in resourcescenarios:
            if res_scen.dataset.type == 'array':
                dataset_id = res_scen.dataset.id
                old_val = res_scen.dataset.value
                break

        newid = client.convert_dataset(dataset_id, 'mmHg')

        assert newid is not None
        assert newid != dataset_id, "Converting dataset not completed."

        new_dataset = client.get_dataset(newid)
        new_val = new_dataset.value

        new_val = arr_to_vector(json.loads(new_val))
        old_val = arr_to_vector(json.loads(old_val))

        old_val_conv = [i * 100000 / 133.322 for i in old_val]

        # Rounding is not exactly the same on the server, that's why we
        # calculate the sum.
        assert sum(new_val) - sum(old_val_conv) < 0.00001, \
            "Unit conversion did not work"

    def test_apply_unit_to_network_rs(self, client, network_with_data):

        network_id = network_with_data.id

        dimension_list = client.get_dimensions()

        rs_to_change = network_with_data.scenarios[0].resourcescenarios[0]

        #The ID of the attribute for which these changes will be made
        attr_id = rs_to_change.resourceattr.attr_id

        #identify a unit to change
        unit_to_change = rs_to_change.dataset.unit_id

        #find another valid unit to replace it with
        unit = client.get_unit(unit_to_change)
        dimension = client.get_dimension(unit.dimension_id)

        new_unit = dimension.units[0].id
        client.apply_unit_to_network_rs(network_id, new_unit, attr_id)

        #now try to apply an incompatible unit. Just go grab another dimension, ensuring it's not accidenally
        #the one we're using
        bad_dimension = dimension_list[0]
        if bad_dimension.id == dimension.id:
            bad_dimension == dimension_list[1]

        bad_unit = bad_dimension.units[0].id

        with pytest.raises(HydraError):
            client.apply_unit_to_network_rs(network_id, bad_unit, attr_id)

        updated_scenario = client.get_scenario(rs_to_change.scenario_id)

        #it's possible that the same unit spans multiple attributes in the original scenario
        #so the updated scenario can only be tested for datsets which are attached to the attribute
        old_unit_rs = [rs for rs in network_with_data.scenarios[0].resourcescenarios if rs.resourceattr.attr_id==attr_id]
        new_unit_rs = [rs for rs in updated_scenario.resourcescenarios if rs.resourceattr.attr_id==attr_id]

        #check that the attributes haven't changed
        assert len(old_unit_rs) == len(new_unit_rs)
        #check that all units have been updated
        assert set([rs.dataset.unit_id for rs in new_unit_rs]) == {new_unit}

        for rs in updated_scenario.resourcescenarios:
            if rs.resourceattr.attr_id == attr_id:
                assert rs.dataset.unit_id == new_unit
