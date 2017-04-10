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
from decimal import Decimal
import json
log = logging.getLogger(__name__)

class TimeSeriesTest(server.SoapServerTest):
    """
        Test for working with timeseries
    """
    def test_subtract_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10, 2)
        dataset_2 = self.create_timeseries(timesteps, 10, 1)

        sub_result = self.client.service.subtract_datasets([dataset_1.id, dataset_2.id])
        expected = {
            start.strftime(self.fmt) : 0.0,
            timedelta_1.strftime(self.fmt) : 1.0,
            timedelta_2.strftime(self.fmt) : 2.0,
        }
       
        result_dict = json.loads(sub_result)
        for k, v in expected.items():
            assert result_dict.values()[0][k] == v

    def test_add_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10, 2)
        dataset_2 = self.create_timeseries(timesteps, 10, 1)

        sub_result = self.client.service.add_datasets([dataset_1.id, dataset_2.id])
       
        expected = {
            start.strftime(self.fmt) : 20,
            timedelta_1.strftime(self.fmt) : 23,
            timedelta_2.strftime(self.fmt) : 26,
        }

        result_dict = json.loads(sub_result)
        for k, v in expected.items():
            assert result_dict.values()[0][k] == v

    def test_multiply_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10, 2)
        dataset_2 = self.create_timeseries(timesteps, 10, 1)

        sub_result = self.client.service.multiply_datasets([dataset_1.id, dataset_2.id])

        expected = {
            start.strftime(self.fmt) : 100,
            timedelta_1.strftime(self.fmt) : 132,
            timedelta_2.strftime(self.fmt) : 168,
        }

        result_dict = json.loads(sub_result)
        for k, v in expected.items():
            assert result_dict.values()[0][k] == v

    def test_divide_timeseries(self):
        
        start = datetime.datetime.now()
        timedelta_1 = start + datetime.timedelta(hours=1)
        timedelta_2 = start + datetime.timedelta(hours=2)
        timesteps = [start, timedelta_1, timedelta_2]
        
        dataset_1 = self.create_timeseries(timesteps, 10.00, 2)
        dataset_2 = self.create_timeseries(timesteps, 10.00, 1)

        sub_result = self.client.service.divide_datasets([dataset_1.id, dataset_2.id])
        
        threeplaces = Decimal('0.001')
        expected = {
            start.strftime(self.fmt) : Decimal(1).quantize(threeplaces),
            timedelta_1.strftime(self.fmt) : Decimal('1.090909091').quantize(threeplaces),
            timedelta_2.strftime(self.fmt) : Decimal('1.166666667').quantize(threeplaces),
        }

        result_dict = json.loads(sub_result)
        for k, v in expected.items():
            assert Decimal(result_dict.values()[0][k]).quantize(threeplaces) == v



    def create_timeseries(self, timesteps, start_val, step=1):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        

        metadata = {'created_by': 'Test user'}

        value = {}
        curr_val = start_val
        for timestep in timesteps:
            value[timestep.strftime(self.fmt)] = curr_val
            curr_val = curr_val + step

        dataset = dict(
            id=None,
            type = 'timeseries',
            name = 'my time series',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = json.dumps({"0.0":value}),
            metadata = json.dumps(metadata), 
        )

        new_dataset = self.client.service.add_dataset(dataset)
        return new_dataset

class ArrayTest(server.SoapServerTest):
    def test_subtract_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = json.loads(self.client.service.subtract_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        assert sub_result[0] == 9
        assert sub_result[1] == 8
        assert sub_result[2] == 7

    def test_add_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = json.loads(self.client.service.add_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        assert sub_result[0] == 14
        assert sub_result[1] == 17
        assert sub_result[2] == 20
 
    def test_multiply_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = json.loads(self.client.service.multiply_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        assert sub_result[0] == 17.25
        assert sub_result[1] == 62.5
        assert sub_result[2] == 141.75

    def test_divide_arrays(self):
        
        dataset_1 = self.create_array([11.5, 12.5, 13.5])
        dataset_2 = self.create_array([1.5, 2.5, 3.5])
        dataset_3 = self.create_array([1, 2, 3])

        sub_result = json.loads(self.client.service.divide_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        res1 = sub_result[0]
        res2 = sub_result[1]
        res3 = sub_result[2]
        threeplaces = Decimal('0.001')
        assert Decimal(res1).quantize(threeplaces) == Decimal(7.666666667).quantize(threeplaces)
        assert Decimal(res2).quantize(threeplaces) == Decimal('2.5').quantize(threeplaces)
        assert Decimal(res3).quantize(threeplaces) == Decimal('1.285714286').quantize(threeplaces)

    def create_array(self, arr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        metadata = {'created_by': 'Test user'}

        dataset = dict(
            id=None,
            type = 'array',
            name = 'my array',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = json.dumps(arr),
            metadata = json.dumps(metadata), 
        )

        new_dataset = self.client.service.add_dataset(dataset)
        return new_dataset

class ScalarTest(server.SoapServerTest):
    def test_subtract_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = json.loads(self.client.service.subtract_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        assert float(sub_result) == 9

    def test_add_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = json.loads(self.client.service.add_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))
        assert float(sub_result) == 14
 
    def test_multiply_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = json.loads(self.client.service.multiply_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        assert float(sub_result) == 17.25

    def test_divide_scalars(self):
        
        dataset_1 = self.create_scalar(11.5)
        dataset_2 = self.create_scalar(1.5)
        dataset_3 = self.create_scalar(1)

        sub_result = json.loads(self.client.service.divide_datasets([dataset_1.id, dataset_2.id, dataset_3.id]))

        res1 = sub_result
        threeplaces = Decimal('0.001')
        assert Decimal(res1).quantize(threeplaces) == Decimal(7.666666667).quantize(threeplaces)
    def create_scalar(self, value):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        metadata = {'created_by': 'Test user'}

        dataset = dict(
            id=None,
            type = 'scalar',
            name = 'my scalar',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = value,
            metadata = json.dumps(metadata), 
        )

        new_dataset = self.client.service.add_dataset(dataset)
        return new_dataset

if __name__ == '__main__':
    server.run()
