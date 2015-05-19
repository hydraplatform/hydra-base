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
import copy
import suds
import logging
log = logging.getLogger(__name__)

class ScenarioTest(server.SoapServerTest):

    def test_update(self):

        network =  self.create_network_with_data()
        
        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
       
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        new_resource_scenario = self.client.service.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        assert new_resource_scenario.value.value.desc_val == 'I am an updated test!', "Value was not updated correctly!!"

    def test_add_scenario(self):
        """
            Test adding a new scenario to a network.
        """
        network = self.create_network_with_data()

        new_scenario = copy.deepcopy(network.scenarios.Scenario[0])
        new_scenario.id = -1
        new_scenario.name = 'Scenario 2'
        new_scenario.description = 'Scenario 2 Description'
        new_scenario.start_time = datetime.datetime.now()
        new_scenario.end_time = new_scenario.start_time + datetime.timedelta(hours=10)
        new_scenario.time_step = "1 day"

        node_attrs = network.nodes.Node[0].attributes

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(node_attrs.ResourceAttr[0], "new_descriptor")
        timeseries = self.create_timeseries(node_attrs.ResourceAttr[1])

        for r in new_scenario.resourcescenarios.ResourceScenario:
            if r.resource_attr_id == node_attrs.ResourceAttr[0].id:
                r.value = descriptor['value']
            elif r.resource_attr_id == node_attrs.ResourceAttr[1].id:
                r.value = timeseries['value']

        scenario = self.client.service.add_scenario(network.id, new_scenario)

        assert scenario is not None
        assert len(scenario.resourcegroupitems.ResourceGroupItem) > 0
        assert len(scenario.resourcescenarios) > 0


    def test_update_scenario(self):
        """
            Test updating an existing scenario.
        """
        network = self.create_network_with_data()

        #Create the new scenario
        scenario = network.scenarios.Scenario[0] 
        scenario.name = 'Updated Scenario'
        scenario.description = 'Updated Scenario Description'
        scenario.start_time = datetime.datetime.now()
        scenario.end_time = scenario.start_time + datetime.timedelta(hours=10)
        scenario.time_step = "1 day"
      
        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes.Node[0]
        node2 = network.nodes.Node[-1]

        #Identify 1 resource group item to edit (the last one in the list).
        item_to_edit = scenario.resourcegroupitems.ResourceGroupItem[-1]
        #Just checking that we're not changing an item that is already
        #assigned to this node..
        assert item_to_edit.ref_id != node2.id
        item_to_edit.ref_id   = node2.id

        descriptor = self.create_descriptor(node1.attributes.ResourceAttr[0], 
                                                "updated_descriptor")

        for resourcescenario in scenario.resourcescenarios.ResourceScenario:
            if resourcescenario.attr_id == descriptor['attr_id']:
                resourcescenario.value = descriptor['value']
        
        updated_scenario = self.client.service.update_scenario(scenario)

        assert updated_scenario is not None
        assert updated_scenario.id == scenario.id
        assert updated_scenario.name == scenario.name 
        assert updated_scenario.description == scenario.description
        assert updated_scenario.start_time == str(scenario.start_time)
        assert updated_scenario.end_time   == str(scenario.end_time)
        assert updated_scenario.time_step  == scenario.time_step
        assert len(updated_scenario.resourcegroupitems.ResourceGroupItem) > 0
        for i in updated_scenario.resourcegroupitems.ResourceGroupItem:
            if i.id == item_to_edit.id:
                assert i.ref_id == node2.id
        assert len(updated_scenario.resourcescenarios) > 0

        for data in updated_scenario.resourcescenarios.ResourceScenario: 
            if data.attr_id == descriptor['attr_id']:
                assert data.value.value.desc_val == descriptor['value']['value']['desc_val']

    def test_get_dataset_scenarios(self):
        """
            Test to get the scenarios attached to a dataset
        """

        network = self.create_network_with_data()

        #Create the new scenario
        scenario = network.scenarios.Scenario[0] 
        rs = scenario.resourcescenarios.ResourceScenario
  
        dataset_id_to_check = rs[0].value.id

        dataset_scenarios = self.client.service.get_dataset_scenarios(dataset_id_to_check)

        assert len(dataset_scenarios.Scenario) == 1

        assert dataset_scenarios.Scenario[0].id == scenario.id
        
        clone = self.client.service.clone_scenario(scenario.id)
        new_scenario = self.client.service.get_scenario(clone.id)

        dataset_scenarios = self.client.service.get_dataset_scenarios(dataset_id_to_check)

        assert len(dataset_scenarios.Scenario) == 2

        assert dataset_scenarios.Scenario[0].id == scenario.id
        assert dataset_scenarios.Scenario[1].id == new_scenario.id

    def test_update_resourcedata(self):
        """
            Test updating an existing scenario data.
            2 main points to test: 1: setting a value to null should remove
            the resource scenario
            2: changing the value should create a new dataset
        """
        network = self.create_network_with_data()

        #Create the new scenario
        scenario = network.scenarios.Scenario[0] 
        num_old_rs = len(scenario.resourcescenarios.ResourceScenario)
      
        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes.Node[0]
        node2 = network.nodes.Node[-1]

        descriptor = self.create_descriptor(node1.attributes.ResourceAttr[0], 
                                                "updated_descriptor")

        val_to_delete = node2.attributes.ResourceAttr[0]
        
        rs_to_update = self.client.factory.create('ns1:ResourceScenarioArray')
        updated_dataset_id = None
        for resourcescenario in scenario.resourcescenarios.ResourceScenario:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                updated_dataset_id = resourcescenario.value['id']
                resourcescenario.value = descriptor['value']
                rs_to_update.ResourceScenario.append(resourcescenario)
            elif ra_id == val_to_delete['id']:
                resourcescenario.value = None
                rs_to_update.ResourceScenario.append(resourcescenario)
       
        assert updated_dataset_id is not None

        new_resourcescenarios = self.client.service.update_resourcedata(scenario.id, rs_to_update)

        assert len(new_resourcescenarios.ResourceScenario) == 1

        for rs in new_resourcescenarios.ResourceScenario: 
            if rs.resource_attr_id == descriptor['resource_attr_id']:
                assert rs.value.value.desc_val == descriptor['value']['value']['desc_val']

        updated_scenario = self.client.service.get_scenario(scenario.id)

        num_new_rs = len(updated_scenario.resourcescenarios.ResourceScenario)
        assert num_new_rs == num_old_rs - 1


        for u_rs in updated_scenario.resourcescenarios.ResourceScenario:
            for rs in new_resourcescenarios.ResourceScenario:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    assert str(u_rs.value) == str(rs.value)
                    break

    def test_update_resourcedata2(self):
        """
            Test to check leng's questions about this not working correctly.
        """
        network = self.create_network_with_data()

        #Create the new scenario
        scenario = network.scenarios.Scenario[0] 
        node1 = network.nodes.Node[0]

        ra_to_update = node1.attributes.ResourceAttr[0].id
        
        updated_val = None

        rs_to_update = self.client.factory.create('ns1:ResourceScenarioArray')
        for resourcescenario in scenario.resourcescenarios.ResourceScenario:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == ra_to_update:
                updated_val = resourcescenario.value.value
                resourcescenario.value.name = 'I am an updated dataset name'
                rs_to_update.ResourceScenario.append(resourcescenario)
       
        self.client.service.get_all_node_data(network.id, scenario.id, [node1.id])

        self.client.service.update_resourcedata(scenario.id, rs_to_update)

        new_node_data = self.client.service.get_all_node_data(network.id, scenario.id, [node1.id])

        for new_val in new_node_data.ResourceAttr:
            if new_val.resourcescenario.value.value == updated_val:
                assert new_val.name == 'I am an updated dataset name'

    def test_bulk_update_resourcedata(self):
        """
            Test updating scenario data in a number of scenarios at once.
            2 main points to test: 1: setting a value to null should remove
            the resource scenario
            2: changing the value should create a new dataset
        """
        network1 = self.create_network_with_data()
        scenario1_to_update = network1.scenarios.Scenario[0] 
        clone = self.client.service.clone_scenario(network1.scenarios.Scenario[0].id)
        scenario2_to_update = self.client.service.get_scenario(clone.id)

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network1.nodes.Node[0]
        node2 = network1.nodes.Node[-1]

        descriptor = self.create_descriptor(node1.attributes.ResourceAttr[0], 
                                                "updated_descriptor")

        val_to_delete = node2.attributes.ResourceAttr[0]
        
        rs_to_update = self.client.factory.create('ns1:ResourceScenarioArray')
        updated_dataset_id = None
        for resourcescenario in scenario1_to_update.resourcescenarios.ResourceScenario:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                updated_dataset_id = resourcescenario.value['id']
                resourcescenario.value = descriptor['value']
                rs_to_update.ResourceScenario.append(resourcescenario)
            elif ra_id == val_to_delete['id']:
                resourcescenario.value = None
                rs_to_update.ResourceScenario.append(resourcescenario)
       
        assert updated_dataset_id is not None
        
        scenario_ids = self.client.factory.create('intArray')
        scenario_ids.int.append(scenario1_to_update.id)
        scenario_ids.int.append(scenario2_to_update.id)

        result = self.client.service.bulk_update_resourcedata(scenario_ids, rs_to_update)

        assert result == "OK" 

        updated_scenario1_data = self.client.service.get_scenario(scenario1_to_update.id)
        updated_scenario2_data = self.client.service.get_scenario(scenario2_to_update.id)
        
        for rs in updated_scenario1_data.resourcescenarios.ResourceScenario:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                assert rs.value == descriptor['value']
        for rs in updated_scenario2_data.resourcescenarios.ResourceScenario:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                assert rs.value == descriptor['value']



    def test_bulk_add_data(self):

        data = self.client.factory.create('ns1:DatasetArray')

        dataset1 = self.client.factory.create('ns1:Dataset')
        
        dataset1.type = 'timeseries'
        dataset1.name = 'my time series'
        dataset1.unit = 'feet cubed'
        dataset1.dimension = 'cubic capacity'

        dataset1.value = {'ts_values':
            [
                {
                    'ts_time' : datetime.datetime.now(),
                    'ts_value' : str([1, 2, 3, 4, 5]),
                },
                {
                    'ts_time' : datetime.datetime.now() + datetime.timedelta(hours=1),
                    'ts_value' : str([2, 3, 4, 5, 6]),
                }
            ],
        }
        data.Dataset.append(dataset1)

        dataset2 = self.client.factory.create('ns1:Dataset')
        dataset2.type = 'descriptor'
        dataset2.name = 'Max Capacity'
        dataset2.unit = 'metres / second'
        dataset2.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset2.value = descriptor

        data.Dataset.append(dataset2)

        new_datasets = self.client.service.bulk_insert_data(data)

        assert len(new_datasets.integer) == 2, "Data was not added correctly!"


    def test_clone(self):

        network =  self.create_network_with_data()
       
        assert len(network.scenarios.Scenario) == 1, "The network should have only one scenario!"

        #self.create_constraint(network)
        
        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        clone = self.client.service.clone_scenario(scenario_id)
        new_scenario = self.client.service.get_scenario(clone.id)


        updated_network = self.client.service.get_network(new_scenario.network_id)


        assert len(updated_network.scenarios.Scenario) == 2, "The network should have two scenarios!"

        assert updated_network.scenarios.Scenario[1].resourcescenarios is not None, "Data was not cloned!"

        scen_2_val = updated_network.scenarios.Scenario[1].resourcescenarios.ResourceScenario[0].value.id
        scen_1_val = network.scenarios.Scenario[0].resourcescenarios.ResourceScenario[0].value.id
        
        assert scen_2_val == scen_1_val, "Data was not cloned correctly"


  #      scen_1_constraint  = network.scenarios.Scenario[0].constraints.Constraint[0].value
        #scen_2_constraint  = updated_network.scenarios.Scenario[1].constraints.Constraint[0].value
#
 #       assert scen_1_constraint == scen_2_constraint, "Constraints did not clone correctly!"
        
        scen_1_resourcegroupitems = network.scenarios.Scenario[0].resourcegroupitems.ResourceGroupItem
        scen_2_resourcegroupitems = updated_network.scenarios.Scenario[1].resourcegroupitems.ResourceGroupItem
        
        assert len(scen_1_resourcegroupitems) == len(scen_2_resourcegroupitems)

        return updated_network

    def test_compare(self):

        network =  self.create_network_with_data()
       

        assert len(network.scenarios.Scenario) == 1, "The network should have only one scenario!"

    #    self.create_constraint(network)
        
        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        clone = self.client.service.clone_scenario(scenario_id)
        new_scenario = self.client.service.get_scenario(clone.id)

    #    self.create_constraint(network, constant=4)

        resource_scenario = new_scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
       
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
 
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        self.client.service.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        item_to_remove = new_scenario.resourcegroupitems.ResourceGroupItem[0].id
        self.client.service.delete_resourcegroupitem(item_to_remove)

        updated_network = self.client.service.get_network(new_scenario.network_id)

        scenarios = updated_network.scenarios.Scenario
        
        scenario_1 = None
        scenario_2 = None
        for s in scenarios:
            if s.id == new_scenario.id:
                scenario_1 = s 
            else:
                scenario_2 = s

        scenario_diff = self.client.service.compare_scenarios(scenario_1.id, scenario_2.id)
        
        #print "Comparison result: %s"%(scenario_diff)

        assert len(scenario_diff.resourcescenarios.ResourceScenarioDiff) == 1, "Data comparison was not successful!"

     #   assert len(scenario_diff.constraints.common_constraints) == 1, "Constraint comparison was not successful!"
        
     #   assert len(scenario_diff.constraints.scenario_2_constraints) == 1, "Constraint comparison was not successful!"

        assert len(scenario_diff.groups.scenario_2_items) == 1, "Group comparison was not successful!"
        assert scenario_diff.groups.scenario_1_items is None, "Group comparison was not successful!"

        return updated_network

    def test_purge_scenario(self):
        net = self.test_clone()

        scenarios_before = net.scenarios.Scenario

        assert len(scenarios_before) == 2

        self.client.service.purge_scenario(scenarios_before[1].id)

        updated_net = self.client.service.get_network(net.id)

        scenarios_after = updated_net.scenarios.Scenario

        assert len(scenarios_after) == 1

        self.assertRaises(suds.WebFault, self.client.service.get_scenario, scenarios_before[1].id)

        assert str(scenarios_after[0]) == str(scenarios_before[0])

    def test_delete_scenario(self):
        net = self.test_clone()

        scenarios_before = net.scenarios.Scenario

        assert len(scenarios_before) == 2

        self.client.service.delete_scenario(scenarios_before[1].id)

        updated_net = self.client.service.get_network(net.id)

        scenarios_after_delete = updated_net.scenarios.Scenario

        assert len(scenarios_after_delete) == 1

        self.client.service.activate_scenario(scenarios_before[1].id)

        updated_net2 = self.client.service.get_network(net.id)

        scenarios_after_reactivate = updated_net2.scenarios.Scenario

        assert len(scenarios_after_reactivate) == 2
        
        self.client.service.delete_scenario(scenarios_before[1].id)
        self.client.service.clean_up_network(net.id)
        updated_net3 = self.client.service.get_network(net.id)
        scenarios_after_cleanup = updated_net3.scenarios.Scenario
        assert len(scenarios_after_cleanup) == 1
        self.assertRaises(suds.WebFault, self.client.service.get_scenario, scenarios_before[1].id)
        
    def test_lock_scenario(self):

        network =  self.create_network_with_data()
       
        network = self.client.service.get_network(network.id)

        scenario_to_lock = network.scenarios.Scenario[0]
        scenario_id = scenario_to_lock.id
        
        log.info('Cloning scenario %s'%scenario_id)
        clone = self.client.service.clone_scenario(scenario_id)
        unlocked_scenario = self.client.service.get_scenario(clone.id)
        
        log.info("Locking scenario")
        self.client.service.lock_scenario(scenario_id)

        locked_scenario = self.client.service.get_scenario(scenario_id)

        assert locked_scenario.locked == 'Y'

        dataset = self.client.factory.create('ns1:Dataset')
       
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
 
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        
        locked_resource_scenarios = []
        for rs in locked_scenario.resourcescenarios.ResourceScenario:
            if rs.value.type == 'descriptor':
                locked_resource_scenarios.append(rs)

        unlocked_resource_scenarios = []
        for rs in unlocked_scenario.resourcescenarios.ResourceScenario:
            if rs.value.type == 'descriptor':
                unlocked_resource_scenarios.append(rs)

        resource_attr_id = unlocked_resource_scenarios[0].resource_attr_id
       
        locked_resource_scenarios_value = None
        for rs in locked_scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.value

        unlocked_resource_scenarios_value = None
        for rs in unlocked_scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.value
        log.info("Updating a shared dataset")
        ds = unlocked_resource_scenarios_value
        ds.dimension = 'updated_dimension'
        updated_ds = self.client.service.update_dataset(ds)

        updated_unlocked_scenario = self.client.service.get_scenario(unlocked_scenario.id)
        #This should not have changed
        updated_locked_scenario = self.client.service.get_scenario(locked_scenario.id)

        locked_resource_scenarios_value = None
        for rs in updated_locked_scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.value

        unlocked_resource_scenarios_value = None
        for rs in updated_unlocked_scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.value

        self.assertRaises(suds.WebFault, self.client.service.add_data_to_attribute, scenario_id, resource_attr_id, dataset)
    
        #THe most complicated situation is this:
        #Change a dataset in an unlocked scenario, which is shared by a locked scenario.
        #The original dataset should stay connected to the locked scenario and a new
        #dataset should be created for the edited scenario.
        self.client.service.add_data_to_attribute(unlocked_scenario.id, resource_attr_id, dataset)

        updated_unlocked_scenario = self.client.service.get_scenario(unlocked_scenario.id)
        #This should not have changed
        updated_locked_scenario = self.client.service.get_scenario(locked_scenario.id)

        locked_resource_scenarios_value = None
        for rs in updated_locked_scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.value

        unlocked_resource_scenarios_value = None
        for rs in updated_unlocked_scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.value


        assert locked_resource_scenarios_value.value != unlocked_resource_scenarios_value.value

        item_to_remove = locked_scenario.resourcegroupitems.ResourceGroupItem[0].id
        self.assertRaises(suds.WebFault, self.client.service.delete_resourcegroupitem, item_to_remove)
        log.info("Locking scenario")
        self.client.service.unlock_scenario(scenario_id)

        locked_scenario = self.client.service.get_scenario(scenario_id)

        assert locked_scenario.locked == 'N'


    def test_get_attribute_data(self):
        """
            Test for retrieval of data for an attribute in a scenario.
        """

        new_net = self.create_network_with_data()

        s = new_net.scenarios.Scenario[0]

        nodes = new_net.nodes.Node

        resource_attr = nodes[0].attributes.ResourceAttr[0]

        attr_id = resource_attr.attr_id

        all_matching_ras = []
        for n in nodes:
            for ra in n.attributes.ResourceAttr:
                if ra.attr_id == attr_id:
                    all_matching_ras.append(ra)
                    continue

        retrieved_ras = self.client.service.get_attribute_datasets(attr_id, s.id)

        ra_dict  = {}
        for ra in retrieved_ras.ResourceAttr:
            ra_dict[ra.id] = ra
            
        assert len(retrieved_ras.ResourceAttr) == len(all_matching_ras)

        for rs in s.resourcescenarios.ResourceScenario:
            if ra_dict.get(rs.resource_attr_id):
                matching_rs = ra_dict[rs.resource_attr_id].resourcescenario
                assert str(rs) == str(matching_rs)
    
    def test_copy_data_from_scenario(self):

        """
            Test copy_data_from_scenario : test that one scenario
            can be updated to contain the data of another with the same
            resource attrs.
        """

        network =  self.create_network_with_data()
       

        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        source_scenario_id = scenario.id

        clone = self.client.service.clone_scenario(source_scenario_id)
        cloned_scenario = self.client.service.get_scenario(clone.id)

        resource_scenario = cloned_scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
 
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        self.client.service.add_data_to_attribute(source_scenario_id, resource_attr_id, dataset)

        scenario_diff = self.client.service.compare_scenarios(source_scenario_id, cloned_scenario.id)
        
        assert len(scenario_diff.resourcescenarios.ResourceScenarioDiff) == 1, "Data comparison was not successful!"

        self.client.service.copy_data_from_scenario(resource_attr_id, cloned_scenario.id, source_scenario_id)

        scenario_diff = self.client.service.compare_scenarios(source_scenario_id, cloned_scenario.id)
        
        assert scenario_diff.resourcescenarios == None, "Scenario update was not successful!"

    def test_set_resourcescenario_dataset(self):

        """
            Test the direct setting of a dataset id on a resource scenario    
        """

        network =  self.create_network_with_data()
       

        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        source_scenario_id = scenario.id

        clone = self.client.service.clone_scenario(source_scenario_id)
        cloned_scenario = self.client.service.get_scenario(clone.id)

        resource_scenario = cloned_scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
 
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        new_ds = self.client.service.add_dataset(dataset)

        self.client.service.set_resourcescenario_dataset(resource_attr_id, source_scenario_id, new_ds.id)

        updated_net = self.client.service.get_network(network.id)

        updated_scenario = updated_net.scenarios.Scenario[0]
        scenario_rs = updated_scenario.resourcescenarios.ResourceScenario
        for rs in scenario_rs:
            if rs.resource_attr_id == resource_attr_id:
                assert rs.value.value.desc_val == 'I am an updated test!'

    def test_add_data_to_attribute(self):

        network =  self.create_network_with_data()
       
        empty_ra = network.links.Link[0].attributes.ResourceAttr[-1]

        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
       
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        updated_resource_scenario = self.client.service.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        new_resource_scenario = self.client.service.add_data_to_attribute(scenario_id, empty_ra.id, dataset)

        assert updated_resource_scenario.value.value.desc_val == 'I am an updated test!', "Value was not updated correctly!!"
        assert new_resource_scenario.value.value.desc_val == 'I am an updated test!', "Value was not updated correctly!!"

if __name__ == '__main__':
    server.run()
