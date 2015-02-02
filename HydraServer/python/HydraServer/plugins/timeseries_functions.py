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
from spyne.decorator import rpc
from spyne.model.primitive import Integer, AnyDict 
from HydraServer.soap_server.hydra_base import HydraService
from HydraServer.lib.data import get_dataset
from HydraLib.HydraException import HydraError
from HydraServer.util import get_val
import logging
import numpy
import json
log = logging.getLogger(__name__)

op_map = {
    'add'      : lambda x, y: numpy.add(x, y),
    'subtract' : lambda x, y: numpy.subtract(x, y),
    'multiply' : lambda x, y: numpy.multiply(x, y),
    'divide'   : lambda x, y: numpy.divide(x, y),
    'avg'      : lambda x : numpy.mean(x),
    'stddev'   : lambda x : numpy.std(x),
}

class Service(HydraService):
    __service_name__ = "TimeseriesService"

    @rpc(Integer(min_occurs=2, max_occurs='unbounded'), _returns=AnyDict)
    def subtract_datasets(ctx, dataset_ids):
        """
            Subtract the value of dataset[1] from the value of dataset[0].
            subtract dataset[2] from result etc.
            Rules: 1: The datasets must be of the same type
                   2: The datasets must be numerical
                   3: If timeseries, the timesteps must match.
            The result is a new value, NOT a new dataset. It is up
            to the client to create a new datasets with the resulting value
            if they wish to do so.
        """
        return _perform_op_on_datasets('subtract', dataset_ids, **ctx.in_header.__dict__)

    @rpc(Integer(min_occurs=2, max_occurs='unbounded'), _returns=AnyDict)
    def add_datasets(ctx, dataset_ids):
        """
            Add the value of dataset[0] to the value of dataset[1] etc.
            Rules: 1: The datasets must be of the same type
                   2: The datasets must be numerical
                   3: If timeseries, the timesteps must match.
            The result is a new value, NOT a new dataset. It is up
            to the client to create a new datasets with the resulting value
            if they wish to do so.
        """
        return _perform_op_on_datasets('add', dataset_ids, **ctx.in_header.__dict__)

    @rpc(Integer(min_occurs=2, max_occurs='unbounded'), _returns=AnyDict)
    def multiply_datasets(ctx, dataset_ids):
        """
            Multiply the value of dataset[0] by the value of dataset[1] and the result
            by the value of dataset[2] etc.
            Rules: 1: The datasets must be of the same type
                   2: The datasets must be numerical
                   3: If timeseries, the timesteps must match.
            The result is a new value, NOT a new dataset. It is up
            to the client to create a new datasets with the resulting value
            if they wish to do so.
        """
        return _perform_op_on_datasets('multiply', dataset_ids, **ctx.in_header.__dict__)

    @rpc(Integer(min_occurs=2, max_occurs='unbounded'), _returns=AnyDict)
    def divide_datasets(ctx, dataset_ids):
        """
            Divide the value of dataset[0] by the value of dataset[1], the
            result of which is divided by the value of dataset[2] etc.
            Rules: 1: The datasets must be of the same type
                   2: The datasets must be numerical
                   3: If timeseries, the timesteps must match.
            The result is a new value, NOT a new dataset. It is up
            to the client to create a new datasets with the resulting value
            if they wish to do so.
        """
        return _perform_op_on_datasets('divide', dataset_ids, **ctx.in_header.__dict__)


def _perform_op_on_datasets(op, dataset_ids, **kwargs):
    datasets = []
    for dataset_id in dataset_ids:
        datasets.append(get_dataset(dataset_id, **kwargs))
    
    data_type = None
    vals = []
    for d in datasets:
        if data_type is None:
            data_type = d.data_type
        if data_type == 'descriptor':
            raise HydraError("Data must be numerical")
        else:
            if d.data_type != d.data_type:
                raise HydraError("Data types do not match.")
        dataset_val = get_val(d)
        if data_type == 'timeseries':
            dataset_val = dataset_val.astype('float')
        vals.append(dataset_val)

    _op = op_map[op]
    op_result = vals[0]
    for v in vals[1:]:
        try:
            op_result = _op(op_result, v)
        except:
            raise HydraError("Unable to perform operation %s on values %s and %s"
                                %(op, op_result, v))
    
    result = json.dumps(op_result)

    return result
