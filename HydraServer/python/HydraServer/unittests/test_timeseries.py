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
import datetime
import logging
from HydraLib.PluginLib import parse_suds_array
from decimal import Decimal
from HydraLib.util import parse_array
from suds import WebFault
from HydraLib.PluginLib import create_dict
log = logging.getLogger(__name__)

class TimeSeriesTest(server.SoapServerTest):
    def test_subtract_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10, 2)
        dataset_2 = self.create_timeseries(timesteps, 10, 1)

        sub_result = self.client.service.subtract_datasets([dataset_1.id, dataset_2.id])

        assert parse_suds_array(sub_result.ts_values[0].ts_value) == 0
        assert parse_suds_array(sub_result.ts_values[1].ts_value) == 1
        assert parse_suds_array(sub_result.ts_values[2].ts_value) == 2

    def test_add_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10, 2)
        dataset_2 = self.create_timeseries(timesteps, 10, 1)

        sub_result = self.client.service.add_datasets([dataset_1.id, dataset_2.id])

        assert parse_suds_array(sub_result.ts_values[0].ts_value) == 20
        assert parse_suds_array(sub_result.ts_values[1].ts_value) == 23
        assert parse_suds_array(sub_result.ts_values[2].ts_value) == 26

    def test_multiply_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10, 2)
        dataset_2 = self.create_timeseries(timesteps, 10, 1)

        sub_result = self.client.service.multiply_datasets([dataset_1.id, dataset_2.id])

        assert parse_suds_array(sub_result.ts_values[0].ts_value) == 100
        assert parse_suds_array(sub_result.ts_values[1].ts_value) == 132
        assert parse_suds_array(sub_result.ts_values[2].ts_value) == 168

    def test_divide_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10.00, 2)
        dataset_2 = self.create_timeseries(timesteps, 10.00, 1)

        sub_result = self.client.service.divide_datasets([dataset_1.id, dataset_2.id])
        
        res1 = parse_suds_array(sub_result.ts_values[0].ts_value)
        res2 = parse_suds_array(sub_result.ts_values[1].ts_value)
        res3 = parse_suds_array(sub_result.ts_values[2].ts_value)
        
        threeplaces = Decimal('0.001')
        assert Decimal(res1).quantize(threeplaces) == Decimal(1).quantize(threeplaces)
        assert Decimal(res2).quantize(threeplaces) == Decimal('1.090909091').quantize(threeplaces)
        assert Decimal(res3).quantize(threeplaces) == Decimal('1.166666667').quantize(threeplaces)



    def create_timeseries(self, timesteps, start_val, step=1):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        metadata_array = self.client.factory.create("hyd:MetadataArray")
        metadata = self.client.factory.create("hyd:Metadata")
        metadata.name = 'created_by'
        metadata.value = 'Test user'
        metadata_array.Metadata.append(metadata)


        value = []
        curr_val = start_val
        for timestep in timesteps:
            time_val = {'ts_time': timestep, 'ts_value': curr_val}
            curr_val = curr_val + step
            value.append(time_val)

        dataset = dict(
            id=None,
            type = 'timeseries',
            name = 'my time series',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = {'ts_values' : value
        },
            metadata = metadata_array, 
        )

        new_dataset = self.client.service.add_dataset(dataset)
        return new_dataset

class ArrayTest(server.SoapServerTest):
    def test_subtract_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = self.client.service.subtract_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        assert parse_suds_array(sub_result.arr_data)[0] == 9
        assert parse_suds_array(sub_result.arr_data)[1] == 8
        assert parse_suds_array(sub_result.arr_data)[2] == 7

    def test_add_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = self.client.service.add_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        assert parse_suds_array(sub_result.arr_data)[0] == 14
        assert parse_suds_array(sub_result.arr_data)[1] == 17
        assert parse_suds_array(sub_result.arr_data)[2] == 20
 
    def test_multiply_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = self.client.service.multiply_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        assert parse_suds_array(sub_result.arr_data)[0] == 17.25
        assert parse_suds_array(sub_result.arr_data)[1] == 62.5
        assert parse_suds_array(sub_result.arr_data)[2] == 141.75

    def test_divide_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = self.client.service.divide_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        res1 = parse_suds_array(sub_result.arr_data)[0]
        res2 = parse_suds_array(sub_result.arr_data)[1]
        res3 = parse_suds_array(sub_result.arr_data)[2]
        threeplaces = Decimal('0.001')
        assert Decimal(res1).quantize(threeplaces) == Decimal(7.666666667).quantize(threeplaces)
        assert Decimal(res2).quantize(threeplaces) == Decimal('2.5').quantize(threeplaces)
        assert Decimal(res3).quantize(threeplaces) == Decimal('1.285714286').quantize(threeplaces)

    def create_array(self, arr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        metadata_array = self.client.factory.create("hyd:MetadataArray")
        metadata = self.client.factory.create("hyd:Metadata")
        metadata.name = 'created_by'
        metadata.value = 'Test user'
        metadata_array.Metadata.append(metadata)

        dataset = dict(
            id=None,
            type = 'array',
            name = 'my array',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = {'arr_data' : create_dict(arr)
        },
            metadata = metadata_array, 
        )

        new_dataset = self.client.service.add_dataset(dataset)
        return new_dataset

class ScalarTest(server.SoapServerTest):
    def test_subtract_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = self.client.service.subtract_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        assert float(sub_result.param_value) == 9

    def test_add_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = self.client.service.add_datasets([dataset_1.id, dataset_2.id, dataset_3.id])
        assert float(sub_result.param_value) == 14
 
    def test_multiply_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = self.client.service.multiply_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        assert float(sub_result.param_value) == 17.25

    def test_divide_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = self.client.service.divide_datasets([dataset_1.id, dataset_2.id, dataset_3.id])

        res1 = sub_result.param_value 
        threeplaces = Decimal('0.001')
        assert Decimal(res1).quantize(threeplaces) == Decimal(7.666666667).quantize(threeplaces)
    def create_scalar(self, value):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        metadata_array = self.client.factory.create("hyd:MetadataArray")
        metadata = self.client.factory.create("hyd:Metadata")
        metadata.name = 'created_by'
        metadata.value = 'Test user'
        metadata_array.Metadata.append(metadata)

        dataset = dict(
            id=None,
            type = 'scalar',
            name = 'my scalar',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = {'param_value':value},
            metadata = metadata_array, 
        )

        new_dataset = self.client.service.add_dataset(dataset)
        return new_dataset

if __name__ == '__main__':
    server.run()
