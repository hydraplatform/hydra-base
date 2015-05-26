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
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import Integer32

from HydraServer.soap_server.hydra_complexmodels import Dataset, HydraComplexModel
from HydraServer.soap_server.hydra_base import HydraService

from HydraServer.db import DBSession
from HydraServer.db.model import ResourceAttr, ResourceScenario, Scenario

from sqlalchemy.orm import joinedload

from HydraLib.HydraException import HydraError

import logging
log = logging.getLogger(__name__)

def _get_data(ref_key, resource_ids, attribute_ids, scenario_ids):

    qry = DBSession.query(ResourceScenario).filter(
                    ResourceAttr.attr_id.in_(attribute_ids),
                    ResourceScenario.resource_attr_id==ResourceAttr.resource_attr_id,
                    Scenario.scenario_id.in_(scenario_ids),
                    ResourceScenario.scenario_id==Scenario.scenario_id).options(joinedload('dataset'))

    if ref_key == 'NODE':
        qry = qry.filter(ResourceAttr.node_id.in_(resource_ids))
    elif ref_key == 'LINK':
        qry = qry.filter(ResourceAttr.link_id.in_(resource_ids))

    return qry.all()

def _get_resource_attributes(ref_key, resource_ids, attribute_ids):

    qry = DBSession.query(ResourceAttr).filter(
                        ResourceAttr.attr_id.in_(attribute_ids),
                    )

    if ref_key == 'NODE':
        qry = qry.filter(ResourceAttr.node_id.in_(resource_ids))
    elif ref_key == 'LINK':
        qry = qry.filter(ResourceAttr.link_id.in_(resource_ids))

    return qry.all()

class MatrixResourceAttribute(HydraComplexModel):
    _type_info = [
        ('attr_id', Integer32(min_occurs=1)),
        ('dataset', Dataset),
    ]

    def __init__(self, attr_id=None, dataset=None):
        super(MatrixResourceAttribute, self).__init__()
        if attr_id is None:
            return
        self.attr_id = attr_id
        if dataset is None:
            self.dataset  = None
        else:
            self.dataset = Dataset(dataset, metadata=False)

class MatrixResourceData(HydraComplexModel):
    _type_info = [
        ('resource_id', Integer32(min_occurs=1)),
        ('attributes'  , SpyneArray(MatrixResourceAttribute)),
    ]

    def __init__(self, resource_id=None, attributes=None):
        super(MatrixResourceData, self).__init__()
        if resource_id is None:
            return
        self.resource_id = resource_id
        self.attributes = [MatrixResourceAttribute(attr_id, dataset) for attr_id, dataset in attributes.items()]

class NodeDatasetMatrix(HydraComplexModel):
    _type_info = [
        ('scenario_id', Integer32(min_occurs=1)),
        ('nodes'  , SpyneArray(MatrixResourceData)),
    ]

    def __init__(self, scenario_id=None, nodes=None):
        super(NodeDatasetMatrix, self).__init__()
        if scenario_id is None:
            return
        self.scenario_id = scenario_id
        node_data = []
        for node_id, attributes in nodes.items():
           node = MatrixResourceData(node_id, attributes)
           node_data.append(node)
        self.nodes = node_data


class Service(HydraService):
    __service_name__ = "AdvancedDatasetRetrievalService"

    @rpc(Integer32(min_occurs=1, max_occurs='unbounded'),
         Integer32(min_occurs=1, max_occurs='unbounded'),
         Integer32(min_occurs=1, max_occurs='unbounded'),
         _returns=SpyneArray(NodeDatasetMatrix))
    def get_node_dataset_matrix(ctx, node_ids, attribute_ids, scenario_ids):
        """
            Given a list of resources, attributes and scenarios return a matrix
            of datasets. If a resource doesn't have an attribute, return None.
            If a resource has an attribute but no dataset, return None.

        """

        if len(scenario_ids) == 0:
            raise HydraError("No scenarios specified!")
        if len(attribute_ids) == 0:
            raise HydraError("No attributes specified!")
        if len(node_ids) == 0:
            raise HydraError("No resources specified")

        resource_attrs = _get_resource_attributes('NODE', node_ids, attribute_ids)

        data = _get_data('NODE', node_ids, attribute_ids, scenario_ids) 
       
        #group the data by scenario
        scenario_data = {}
        for rs in data:
            data_in_scenario = scenario_data.get(rs.scenario_id, {})
            data_in_scenario[rs.resource_attr_id] = rs.dataset
            scenario_data[rs.scenario_id] = data_in_scenario

        #For each node, make a list of its attr_ids.
        all_node_attrs = {}
        for ra in resource_attrs:
            node_attrs = all_node_attrs.get(ra.node_id, {})
            node_attrs[ra.attr_id] = ra.resource_attr_id
            all_node_attrs[ra.node_id] = node_attrs

        node_attr_dict = {}
        for s_id in scenario_ids:
            node_attr_dict[s_id] = {}
            for node_id in node_ids:
                node_attr_dict[s_id][node_id] = {}
                for attr_id in attribute_ids:
                    if attr_id in all_node_attrs[node_id]:
                        #This should be searching in ra_data_map by resource_attr_id,
                        #which is accessed at existing_node_attrs[node_id][attr_id]
                        node_attr_dict[s_id][node_id][attr_id] = \
                                scenario_data[s_id].get(
                                                    all_node_attrs[node_id][attr_id]
                                                       )
                    else:
                        node_attr_dict[s_id][node_id][attr_id] = None
        
        returned_matrix = [NodeDatasetMatrix(scenario_id, data) for scenario_id, data in node_attr_dict.items()]

        return returned_matrix 



