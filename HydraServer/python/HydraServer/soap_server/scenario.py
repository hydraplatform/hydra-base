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
from spyne.model.primitive import Integer, Integer32, Unicode
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Scenario,\
        ResourceScenario,\
        Dataset,\
        ResourceAttr,\
        AttributeData,\
        ScenarioDiff

import logging
log = logging.getLogger(__name__)
from HydraServer.lib import scenario
from hydra_base import HydraService

class ScenarioService(HydraService):
    """
        The scenario SOAP service
        as all resources already exist, there is no need to worry
        about negative IDS
    """

    @rpc(Integer, Unicode(pattern="['YN']", default='N'), _returns=Scenario)
    def get_scenario(ctx, scenario_id, return_summary):
        """
            Get the specified scenario
        """
        scen = scenario.get_scenario(scenario_id, **ctx.in_header.__dict__)

        if return_summary=='Y':
            return Scenario(scen, summary=True)
        else:
            return Scenario(scen, summary=False)

    @rpc(Integer, Scenario, Unicode(pattern="['YN']", default='N'), _returns=Scenario)
    def add_scenario(ctx, network_id, scen, return_summary):
        """
            Add a scenario to a specified network.
        """
        new_scen = scenario.add_scenario(network_id, scen, **ctx.in_header.__dict__)
        if return_summary=='Y':
            return Scenario(new_scen, summary=True)
        else:
            return Scenario(new_scen, summary=False)

    @rpc(Scenario, Unicode(pattern="['YN']", default='N'), _returns=Scenario)
    def update_scenario(ctx, scen, return_summary):
        """
            Update a single scenario
            as all resources already exist, there is no need to worry
            about negative IDS
        """
        updated_scen = scenario.update_scenario(scen, **ctx.in_header.__dict__)
        if return_summary=='Y':
            return Scenario(updated_scen, summary=True)
        else:
            return Scenario(updated_scen, summary=False)

    @rpc(Integer, _returns=Unicode)
    def purge_scenario(ctx, scenario_id):
        """
            Set the status of a scenario to 'X'.
        """

        return scenario.purge_scenario(scenario_id, **ctx.in_header.__dict__)

    @rpc(Integer, _returns=Unicode)
    def delete_scenario(ctx, scenario_id):
        """
            Set the status of a scenario to 'X'.
        """

        return scenario.set_scenario_status(scenario_id, 'X', **ctx.in_header.__dict__)

    @rpc(Integer, _returns=Unicode)
    def activate_scenario(ctx, scenario_id):
        """
            Set the status of a scenario to 'X'.
        """

        return scenario.set_scenario_status(scenario_id, 'A', **ctx.in_header.__dict__)


    @rpc(Integer, _returns=Scenario)
    def clone_scenario(ctx, scenario_id):

        cloned_scen = scenario.clone_scenario(scenario_id, **ctx.in_header.__dict__)

        return Scenario(cloned_scen)

    @rpc(Integer, Integer, _returns=ScenarioDiff)
    def compare_scenarios(ctx, scenario_id_1, scenario_id_2):
        scenariodiff = scenario.compare_scenarios(scenario_id_1,
                                                  scenario_id_2,
                                                  **ctx.in_header.__dict__)

        return ScenarioDiff(scenariodiff)


    @rpc(Integer, _returns=Unicode)
    def lock_scenario(ctx, scenario_id):
        result = scenario.lock_scenario(scenario_id, **ctx.in_header.__dict__)
        return result

    @rpc(Integer, _returns=Unicode)
    def unlock_scenario(ctx, scenario_id):
        result = scenario.unlock_scenario(scenario_id, **ctx.in_header.__dict__)
        return result

    @rpc(Integer, _returns=SpyneArray(Scenario))
    def get_dataset_scenarios(ctx, dataset_id):
        """
            Get all the scenarios attached to a dataset
            @returns a list of scenario_ids
        """
        
        scenarios = scenario.get_dataset_scenarios(dataset_id, **ctx.in_header.__dict__)

        return [Scenario(s, summary=True) for s in scenarios]

    @rpc(Integer, SpyneArray(ResourceScenario), _returns=SpyneArray(ResourceScenario))
    def update_resourcedata(ctx,scenario_id, resource_scenarios):
        """
            Update the data associated with a scenario.
            Data missing from the resource scenario will not be removed
            from the scenario. Use the remove_resourcedata for this task.
        """
        res = scenario.update_resourcedata(scenario_id,
                                           resource_scenarios,
                                           **ctx.in_header.__dict__)
        ret = [ResourceScenario(r) for r in res]
        return ret

    @rpc(SpyneArray(Integer32), SpyneArray(ResourceScenario), _returns=Unicode)
    def bulk_update_resourcedata(ctx, scenario_ids, resource_scenarios):
        """
            Update the data associated with a scenario.
            Data missing from the resource scenario will not be removed
            from the scenario. Use the remove_resourcedata for this task.
        """

        scenario.bulk_update_resourcedata(scenario_ids,
                                          resource_scenarios,
                                         **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer, ResourceScenario, _returns=Unicode)
    def delete_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Remove the data associated with a resource in a scenario.
        """
        success = 'OK'
        scenario.delete_resourcescenario(scenario_id,
                                         resource_scenario,
                                         **ctx.in_header.__dict__)
        return success

    @rpc(Integer, Integer, Dataset, _returns=ResourceScenario)
    def add_data_to_attribute(ctx, scenario_id, resource_attr_id, dataset):
        """
                Add data to a resource scenario outside of a network update
        """
        new_rs = scenario.add_data_to_attribute(scenario_id,
                                                  resource_attr_id,
                                                  dataset,
                                                  **ctx.in_header.__dict__)
        x = ResourceScenario(new_rs)
        return x

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_scenario_data(ctx, scenario_id):
        scenario_data = scenario.get_scenario_data(scenario_id,
                                                   **ctx.in_header.__dict__)
        data_cm = [Dataset(d) for d in scenario_data]
        return data_cm

    @rpc(Integer, Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceScenario))
    def get_node_data(ctx, node_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given node 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        node_data = scenario.get_resource_data('NODE',
                                               node_id,
                                               scenario_id,
                                               type_id,
                                               **ctx.in_header.__dict__
                                              )
        
        ret_data = [ResourceScenario(rs) for rs in node_data]
        return ret_data 

    @rpc(Integer, Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceScenario))
    def get_link_data(ctx, link_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given link 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        link_data = scenario.get_resource_data('LINK',
                                               link_id,
                                               scenario_id,
                                               type_id,
                                               **ctx.in_header.__dict__
        )
        
        ret_data = [ResourceScenario(rs) for rs in link_data]
        return ret_data

    @rpc(Integer, Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceScenario))
    def get_network_data(ctx, network_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given network 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        network_data = scenario.get_resource_data('NETWORK',
                                               network_id,
                                               scenario_id,
                                               type_id,
                                                **ctx.in_header.__dict__)
        
        ret_data = [ResourceScenario(rs) for rs in network_data]
        return ret_data

    @rpc(Integer, Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceScenario))
    def get_resourcegroup_data(ctx, resourcegroup_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given resourcegroup 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        group_data = scenario.get_resource_data('GROUP',
                                               resourcegroup_id,
                                               scenario_id,
                                               type_id,
                                               **ctx.in_header.__dict__)
        
        ret_data = [ResourceScenario(rs) for rs in group_data]
        return ret_data

    @rpc(SpyneArray(Integer), SpyneArray(Integer), _returns=AttributeData)
    def get_node_attribute_data(ctx, node_ids, attr_ids):
        """
            Get the data for multiple attributes on multiple nodes
            across multiple scenarios.
            @returns a list of AttributeData objects, which consist of a list
            of ResourceAttribute objects and a list of corresponding
            ResourceScenario objects.
        """

        node_attrs, resource_scenarios = scenario.get_attribute_data(attr_ids,
                                                                     node_ids,
                                                                    **ctx.in_header.__dict__)

        node_ras = [ResourceAttr(na) for na in node_attrs]
        node_rs  = [ResourceScenario(rs) for rs in resource_scenarios]

        ret_obj = AttributeData()
        ret_obj.resourceattrs = node_ras
        ret_obj.resourcescenarios = node_rs

        return ret_obj

    @rpc(Integer, Integer, _returns=SpyneArray(ResourceAttr))
    def get_attribute_datasets(ctx, attr_id, scenario_id):
        """
            Get all the datasets from resource attributes with the given attribute
            ID in the given scenario.

            Return a list of resource attributes with their associated
            resource scenarios (and values).
        """
        resource_attrs = scenario.get_attribute_datasests(attr_id, scenario_id, **ctx.in_header.__dict__)

        ra_cms = []
        for ra in resource_attrs:
            res_attr_cm = ResourceAttr(ra)
            res_attr_cm.resourcescenario = ResourceScenario(ra.resourcescenario)
            ra_cms.append(res_attr_cm)

        return ra_cms

    @rpc(Integer(min_occurs=1, max_occurs='unbounded'), Integer, Integer, _returns=SpyneArray(ResourceScenario))
    def copy_data_from_scenario(ctx, resource_attr_ids, source_scenario_id, target_scenario_id):
        """
            Copy the datasets from a source scenario into the equivalent resource scenarios
            in the target scenario. Parameters are a list of resource attribute IDS, the
            ID of the source scenario and the ID of the target scenario.
        """
        updated_resourcescenarios = scenario.copy_data_from_scenario(resource_attr_ids,
                                                                    source_scenario_id,
                                                                    target_scenario_id,
                                                                    **ctx.in_header.__dict__)

        ret_resourcescenarios=[ResourceScenario(rs) for rs in updated_resourcescenarios]

        return ret_resourcescenarios 

    @rpc(Integer, Integer, Integer, _returns=ResourceScenario)
    def set_resourcescenario_dataset(ctx, resource_attr_id, scenario_id, dataset_id):
        
        rs = scenario.set_rs_dataset(resource_attr_id,
                                     scenario_id,
                                     dataset_id,
                                     **ctx.in_header.__dict__)
        
        return ResourceScenario(rs)
        
