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

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import server

from HydraLib.util import arr_to_vector
from HydraLib import PluginLib


class UnitsTest(server.SoapServerTest):

    def test_get_dimensions(self):

        dimension_list = self.client.service.get_dimensions()
        assert dimension_list is not None and len(dimension_list) != 0, \
            "Could not get a list of dimensions."

    def test_get_units(self):

        dimension_list = self.client.service.get_dimensions()[0]
        unit_list = self.client.service.get_units(dimension_list[0])
        assert unit_list is not None and len(unit_list) != 0, \
            "Could not get a list of units"

    def test_get_dimension(self):

        testdim = 'Length'
        testunit = 'km'
        resultdim = self.client.service.get_dimension(testunit)
        assert testdim == resultdim, \
            "Getting dimension for 'kilometers' didn't work."

    def test_add_dimension(self):

        # Try to add an existing dimension
        testdim = 'Length'
        result = self.client.service.add_dimension(testdim)
        assert result is False, \
            "Adding existing dimension didn't work as expected."

        dimension_list = self.client.service.get_dimensions()[0]
        assert testdim in dimension_list, \
            "Adding existing dimension didn't work as expected."

        # Add a new dimension
        testdim = 'Electric current'
        result = self.client.service.add_dimension(testdim)
        assert result is True, \
            "Adding new dimension didn't work as expected."
        dimension_list = self.client.service.get_dimensions()[0]
        assert testdim in dimension_list, \
            "Adding new dimension didn't work as expected."

    def test_delete_dimension(self):
        # Add a new dimension and delete it
        testdim = 'Electric current'
        result = self.client.service.add_dimension(testdim)
        old_dimension_list = self.client.service.get_dimensions()[0]
        result = self.client.service.delete_dimension(testdim)
        assert result is True, \
            "Deleting dimension didn't work as expected."
        new_dimension_list = self.client.service.get_dimensions()[0]
        assert testdim in old_dimension_list and \
            testdim not in new_dimension_list, \
            "Deleting dimension didn't work."
        # Add a unit to an existing dimension and delete the dimension. This
        # should delete the dimension from the user file only but not from the
        # static file.
        new_unit = self.client.factory.create('hyd:Unit')
        new_unit.name = 'Teaspoons per second'
        new_unit.abbr = 'tsp s^-1'
        new_unit.cf = 0               # Constant conversion factor
        new_unit.lf = 1.47867648e-05  # Linear conversion factor
        new_unit.dimension = 'Volumetric flow rate'
        new_unit.info = 'A flow of one tablespoon per second.'
        self.client.service.add_unit(new_unit)
        result = self.client.service.delete_dimension(new_unit.dimension)
        assert result is True, \
            "Deleting a static dimension didn't work as expected."
        new_dimension_list = self.client.service.get_dimensions()[0]
        assert new_unit.dimension in new_dimension_list, \
            "Deleting static dimension didn't work."

    def test_add_unit(self):
        # Add a new unit to an existing static dimension
        new_unit = self.client.factory.create('hyd:Unit')
        new_unit.name = 'Teaspoons per second'
        new_unit.abbr = 'tsp s^-1'
        new_unit.cf = 0               # Constant conversion factor
        new_unit.lf = 1.47867648e-05  # Linear conversion factor
        new_unit.dimension = 'Volumetric flow rate'
        new_unit.info = 'A flow of one tablespoon per second.'
        result = self.client.service.add_unit(new_unit)
        assert result is True, \
            "Adding new unit didn't return 'True'"
        unitlist = self.client.service.get_units(new_unit.dimension)
        unitabbr = []
        for unit in unitlist[0]:
            unitabbr.append(unit.abbr)

        assert new_unit.abbr in unitabbr, \
            "Adding new unit didn't work."

        self.client.service.delete_dimension(new_unit.dimension)
        # Add a new unit to an existing custom dimension
        testdim = 'Test dimension'
        self.client.service.add_dimension(testdim)
        testunit = self.client.factory.create('hyd:Unit')
        testunit.name = 'Test'
        testunit.abbr = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension = testdim

        result = self.client.service.add_unit(testunit)
        assert result is True, \
            "Adding a new unit didn't work as expected"
        unitlist = self.client.service.get_units(testdim)
        assert len(unitlist) == 1, \
            "Adding a new unit didn't work as expected"
        assert unitlist[0][0].name == 'Test', \
            "Adding a new unit didn't work as expected"

        self.client.service.delete_dimension(testdim)

    def test_update_unit(self):
        # Add a new unit to a new dimension
        testdim = 'Test dimension'
        self.client.service.add_dimension(testdim)
        testunit = self.client.factory.create('hyd:Unit')
        testunit.name = 'Test'
        testunit.abbr = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension = testdim
        self.client.service.add_unit(testunit)

        # Update it
        testunit.cf = 0
        result = self.client.service.update_unit(testunit)

        assert result is True, \
            "Updating unit didn't work."

        unitlist = self.client.service.get_units(testdim)

        assert unitlist[0][0].cf == 0, \
            "Updating unit didn't work correctly."

        self.client.service.delete_dimension(testdim)

    def test_delete_unit(self):
        # Add a new unit to a new dimension
        testdim = 'Test dimension'
        self.client.service.add_dimension(testdim)
        testunit = self.client.factory.create('hyd:Unit')
        testunit.name = 'Test'
        testunit.abbr = 'ttt'
        testunit.cf = 21
        testunit.lf = 42
        testunit.dimension = testdim
        self.client.service.add_unit(testunit)

        result = self.client.service.delete_unit(testunit)

        assert result is True, \
            "Deleting unit didn't work."

        unitlist = self.client.service.get_units(testdim)

        assert len(unitlist) == 0, \
            "Deleting unit didn't work correctly."

        self.client.service.delete_dimension(testdim)

    def test_convert_units(self):
        result = self.client.service.convert_units(20, 'm', 'km')
        assert result == [0.02], \
            "Converting metres to kilometres didn't work."
        result = self.client.service.convert_units([20., 30., 40.], 'm', 'km')
        assert result == [0.02, 0.03, 0.04],  \
            "Unit conversion of array didn't work."
        result = self.client.service.convert_units(20, '2e6 m^3', 'hm^3')
        assert result == [40], "Conversion with factor didn't work correctly."

    def test_convert_dataset(self):
        network = self.create_network_with_data(num_nodes=2)
        scenario = \
            network.scenarios.Scenario[0].resourcescenarios.ResourceScenario
        # Select the first array (should have untis 'bar') and convert it
        for res_scen in scenario:
            if res_scen.value.type == 'array':
                dataset_id = res_scen.value.id
                old_val = res_scen.value.value
                break
        newid = self.client.service.convert_dataset(dataset_id, 'mmHg')
        
        assert newid is not None
        assert newid != dataset_id, "Converting dataset not completed."

        new_dataset = self.client.service.get_dataset(newid)
        new_val = new_dataset.value

        new_val = arr_to_vector(PluginLib.parse_suds_array(new_val.arr_data))
        old_val = arr_to_vector(PluginLib.parse_suds_array(old_val.arr_data))

        old_val_conv = [i * 100000 / 133.322 for i in old_val]

        # Rounding is not exactly the same on the server, that's why we
        # calculate the sum.
        assert sum(new_val) - sum(old_val_conv) < 0.00001, \
            "Unit conversion did not work"

    def test_check_consistency(self):
        result1 = self.client.service.check_consistency('m^3', 'Volume')
        result2 = self.client.service.check_consistency('m', 'Volume')
        assert result1 is True, \
            "Unit consistency check didn't work."
        assert result2 is False, \
            "Unit consistency check didn't work."

if __name__ == '__main__':
    server.run()
