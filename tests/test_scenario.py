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
import json
import hydra_base
from fixtures import *
import util
import pytest
from hydra_base.lib.objects import JSONObject, Dataset
from hydra_base.util.hydra_dateutil import ordinal_to_timestamp, timestamp_to_ordinal

import logging
log = logging.getLogger(__name__)

class TestScenario:

    def get_scenario(self, scenario_id):
        """
            Utility function wrapper for a function that's called regularly
            Introduced as the JSONObject wrapper can be controlled more easily
        """
        return JSONObject(hydra_base.get_scenario(scenario_id, user_id=pytest.root_user_id))

    def clone_scenario(self, scenario_id):
        """
            Utility function wrapper for a function tat's called regularly.
            Introduced as the JSONObject wrapper can be controlled more easily
        """
        return JSONObject(hydra_base.clone_scenario(scenario_id, user_id=pytest.root_user_id))

    def test_update(self, session, network_with_data):

        network =  network_with_data

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset({
            'type'      : 'descriptor',
            'name'      : 'Max Capacity',
            'unit'      : 'metres / second',
            'value'     : 'I am an updated test!',
        })

        new_resource_scenario = hydra_base.add_data_to_attribute(scenario_id, resource_attr_id, dataset, user_id=pytest.root_user_id)

        assert new_resource_scenario.dataset.value == 'I am an updated test!', "Value was not updated correctly!!"

    def test_add_scenario(self, session, network_with_data):
        """
            Test adding a new scenario to a network.
        """
        network = network_with_data

        new_scenario = copy.deepcopy(network.scenarios[0])
        new_scenario.id = -1
        new_scenario.name = 'Scenario 2'
        new_scenario.description = 'Scenario 2 Description'
        new_scenario.start_time = datetime.datetime.now()
        new_scenario.end_time = new_scenario.start_time + datetime.timedelta(hours=10)
        new_scenario.time_step = "1 day"

        node_attrs = network.nodes[0].attributes

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = util.create_descriptor(util.get_by_name('node_attr_a', node_attrs), "new_descriptor")
        timeseries = util.create_timeseries(util.get_by_name('node_attr_b', node_attrs))

        for r in new_scenario.resourcescenarios:
            if r.resource_attr_id == util.get_by_name('node_attr_a', node_attrs).id:
                r.dataset = descriptor.dataset
            elif r.resource_attr_id == util.get_by_name('node_attr_b', node_attrs).id:
                r.dataset = timeseries.dataset

        scenario = JSONObject(hydra_base.add_scenario(network.id, new_scenario, user_id=pytest.root_user_id))

        assert scenario is not None
        assert len(scenario.resourcegroupitems) > 0
        assert len(scenario.resourcescenarios) > 0


    def test_update_scenario(self, session, network_with_data):
        """
            Test updating an existing scenario.
        """
        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        scenario.name = 'Updated Scenario'
        scenario.description = 'Updated Scenario Description'
        scenario.start_time = datetime.datetime.now()
        scenario.end_time = scenario.start_time + datetime.timedelta(hours=10)
        scenario.time_step = "1 day"

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes[0]
        node2 = network.nodes[-1]

        #Identify 1 resource group item to edit (the last one in the list).
        item_to_edit = scenario.resourcegroupitems[-1]
        #Just checking that we're not changing an item that is already
        #assigned to this node..
        assert scenario.resourcegroupitems[-1].node_id != node2.node_id
        scenario.resourcegroupitems[-1].node_id   = node2.node_id

        descriptor = util.create_descriptor(util.get_by_name('node_attr_a', node1.attributes),
                                                "updated_descriptor")

        for resourcescenario in scenario.resourcescenarios:
            if resourcescenario.attr_id == descriptor.attr_id:
                resourcescenario.dataset = descriptor.dataset

        updated_scenario = JSONObject(hydra_base.update_scenario(scenario, user_id=pytest.root_user_id))

        assert updated_scenario is not None
        assert updated_scenario.id == scenario.id
        assert updated_scenario.name == scenario.name
        assert updated_scenario.description == scenario.description
        assert "%.2f"%updated_scenario.start_time == "%.2f"%timestamp_to_ordinal(scenario.start_time)
        assert "%.2f"%updated_scenario.end_time == "%.2f"%timestamp_to_ordinal(scenario.end_time)
        assert updated_scenario.time_step  == scenario.time_step
        assert len(updated_scenario.resourcegroupitems) > 0
        for i in updated_scenario.resourcegroupitems:
            if i.id == item_to_edit.id:
                assert i.node_id == node2.node_id
        assert len(updated_scenario.resourcescenarios) > 0

        for data in updated_scenario.resourcescenarios:
            if data.attr_id == descriptor['attr_id']:
                assert data.dataset.value == descriptor.dataset.value

    def test_get_dataset_scenarios(self, session, network_with_data):
        """
            Test to get the scenarios attached to a dataset
        """

        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        rs = scenario.resourcescenarios

        dataset_id_to_check = rs[0].dataset.id

        dataset_scenarios = [JSONObject(s) for s in hydra_base.get_dataset_scenarios(dataset_id_to_check, user_id=pytest.root_user_id)]

        assert len(dataset_scenarios) == 1

        assert dataset_scenarios[0].id == scenario.id

        clone = self.clone_scenario(scenario.id)
        new_scenario = self.get_scenario(clone.id)

        dataset_scenarios = [JSONObject(s) for s in hydra_base.get_dataset_scenarios(dataset_id_to_check, user_id=pytest.root_user_id)]

        assert len(dataset_scenarios) == 2

        assert set([scenario.id, new_scenario.id]) == set([s.id for s in dataset_scenarios])

    def test_update_resourcedata(self, session, network_with_data):
        """
            Test updating an existing scenario data.
            2 main points to test: 1: setting a value to null should remove
            the resource scenario
            2: changing the value should create a new dataset
        """
        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        num_old_rs = len(scenario.resourcescenarios)

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes[0]
        node1attr = util.get_by_name('node_attr_a', node1.attributes)
        node2 = network.nodes[-1]
        val_to_delete = util.get_by_name('node_attr_a', node2.attributes)

        descriptor = util.create_descriptor(node1attr,
                                                "updated_descriptor")

        rs_to_update = []
        updated_dataset_id = None
        for resourcescenario in scenario.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor.resource_attr_id:
                updated_dataset_id = resourcescenario.dataset.id
                resourcescenario.dataset = descriptor.dataset
                rs_to_update.append(resourcescenario)
            elif ra_id == val_to_delete['id']:
                resourcescenario.dataset = None
                rs_to_update.append(resourcescenario)

        assert updated_dataset_id is not None

        new_resourcescenarios = [JSONObject(rs) for rs in hydra_base.update_resourcedata(scenario.id, rs_to_update, user_id=pytest.root_user_id)]

        assert len(new_resourcescenarios) == 1

        for rs in new_resourcescenarios:
            if rs.resource_attr_id == descriptor.resource_attr_id:
                assert rs.dataset.value == descriptor.dataset.value

        updated_scenario = self.get_scenario(scenario.id)

        num_new_rs = len(updated_scenario.resourcescenarios)

        assert num_new_rs == num_old_rs - 1


        for u_rs in updated_scenario.resourcescenarios:
            for rs in new_resourcescenarios:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    assert str(u_rs.dataset.value) == str(rs.dataset.value)
                    break

    def test_update_resourcedata_single_dataset_update_and_delete(self, session, network_with_data):
        """
            Test to ensure update_resourcedata does not update other
            datasets that it should not.
        """
        network = network_with_data

        scenario_1 = network.scenarios[0]
        scenario_2 = self.clone_scenario(scenario_1.id)
        scenario_2 = self.get_scenario(scenario_2.id)

        new_value = json.dumps({"index": {"2000-01-01":"test", "2000-02-01":"update"}})

        #Delete a timeseries from one scenario, so there's only 1 reference to that
        #dataset in tResourceSceanrio.
        ts_to_delete = []
        ra_id = None
        ts_id = None
        for rs in scenario_1.resourcescenarios:
            if rs.dataset.type == 'timeseries':
                ra_id = rs.resource_attr_id
                ts_id = rs.dataset.id
                rs.dataset = None
                ts_to_delete.append(rs)
                break

        ts_to_update = []
        for rs in scenario_2.resourcescenarios:
            if rs.resource_attr_id == ra_id:
                rs.dataset.value= new_value
                ts_to_update.append(rs)
                break

        hydra_base.update_resourcedata(scenario_1.id, ts_to_delete, user_id=pytest.root_user_id)

        hydra_base.update_resourcedata(scenario_2.id, ts_to_update, user_id=pytest.root_user_id)

        scenario_2_updated_1 = self.get_scenario(scenario_2.id)
        for rs in scenario_2_updated_1.resourcescenarios:
            if rs.resource_attr_id == ra_id:
                assert json.loads(rs.dataset.value) == json.loads(new_value)
                #Either the dataset is the same dataset, just updated or the dataset
                #has been removed and linked to a previous dataset, which must have a lower ID.
                assert rs.dataset.id <= ts_id
                break
        else:
            raise Exception("Couldn't find resource scenario. SOmething went wrong.")

    def test_update_resourcedata_consistency(self, session, network_with_data):
        """
            Test to ensure update_resourcedata does not update other
            datasets that it should not.
        """
        network = network_with_data

        scenario_1 = util.get_by_name("Scenario 1", network.scenarios)
        scenario_2 = self.clone_scenario(scenario_1.id)
        scenario_2 = self.get_scenario(scenario_2.id)

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes[0]
  

        descriptor = util.create_descriptor(util.get_by_name('node_attr_a', node1.attributes),
                                                "updated_descriptor")

        rs_to_update = self._get_rs_to_update(scenario_1, descriptor)

        #TODO: avoid haveing to explicitly tell the DB to forget about existing relationships,
        #instead making JSONObject more robust at dealing with recursion
        hydra_base.db.DBSession.expunge_all()

        #Update the value
        updated_data = hydra_base.update_resourcedata(scenario_1.id, rs_to_update, user_id=pytest.root_user_id)

        new_resourcescenarios = [JSONObject(rs) for rs in updated_data]

        rs_1_id = None
        updated_scenario_1 = self.get_scenario(scenario_1.id)
        for u_rs in updated_scenario_1.resourcescenarios:
            for rs in new_resourcescenarios:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    log.info(u_rs.dataset)
                    log.info(rs.dataset)
                    assert str(u_rs.dataset.value) == str(rs.dataset.value)
                    rs_1_id = u_rs.dataset
                    break

        scalar = util.create_descriptor(util.get_by_name('node_attr_a', node1.attributes), 200)

        rs_to_update = self._get_rs_to_update(scenario_2, scalar)

        new_resourcescenarios = hydra_base.update_resourcedata(scenario_2.id,
                                                                        rs_to_update, user_id=pytest.root_user_id)
        rs_2_id = None
        #Check that scenario 2 has been updated correctly.
        updated_scenario_2 = self.get_scenario(scenario_2.id)
        for u_rs in updated_scenario_2.resourcescenarios:
            for rs in new_resourcescenarios:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    rs_2_id = u_rs.dataset.value
                    assert str(u_rs.dataset.value) == str(rs.dataset.value)
                    break
        log.critical("%s vs %s", rs_1_id, rs_2_id)
        #Check that this change has not affected scenario 1
        for u_rs in updated_scenario_1.resourcescenarios:
            for rs in new_resourcescenarios:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    assert str(u_rs.dataset.value) != str(rs.dataset.value)
                    break

    def _get_rs_to_update(self, scenario, rs):
        """
            Given a scenario, fetch all the RS which match the attribute ID
            of the rs passed in. These will be updated in an update call.
        """
        rs_to_update = []
        updated_dataset_id = None
        for resourcescenario in scenario.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == rs['resource_attr_id']:
                updated_dataset_id = resourcescenario.dataset.id
                resourcescenario.dataset = rs.dataset
                rs_to_update.append(resourcescenario)

        assert updated_dataset_id is not None

        return rs_to_update

    def test_get_attributes_for_resource(self, session, network_with_data):
        """
            Test to check leng's questions about this not working correctly.
        """
        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        node1 = network.nodes[0]

        ra_to_update = util.get_by_name('node_attr_a', node1.attributes).id

        updated_val = None

        rs_to_update = []
        for resourcescenario in scenario.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == ra_to_update:
                updated_val = resourcescenario.dataset.value
                resourcescenario.dataset.name = 'I am an updated dataset name'
                rs_to_update.append(resourcescenario)

        hydra_base.get_attributes_for_resource(network.id, scenario.id, 'NODE', [node1.id], user_id=pytest.root_user_id)

        hydra_base.update_resourcedata(scenario.id, rs_to_update, user_id=pytest.root_user_id)

        hydra_base.db.DBSession.expunge_all()

        new_node_data = [JSONObject(d) for d in hydra_base.get_attributes_for_resource(network.id, scenario.id, 'NODE', [node1.id], user_id=pytest.root_user_id)]

        for new_val in new_node_data:
            if new_val.dataset.value == updated_val:
                assert new_val.dataset.name == 'I am an updated dataset name'

    def test_bulk_update_resourcedata(self, session, network_with_data):
        """
            Test updating scenario data in a number of scenarios at once.
            2 main points to test: 1: setting a value to null should remove
            the resource scenario
            2: changing the value should create a new dataset
        """
        network1 = network_with_data
        scenario1_to_update = network1.scenarios[0]
        clone = self.clone_scenario(network1.scenarios[0].id)
        scenario2_to_update = self.get_scenario(clone.id)

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network1.nodes[0]

        node1attr = util.get_by_name('node_attr_a', node1.attributes)

        node2 = network1.nodes[-1]

        descriptor = util.create_descriptor(util.get_by_name('node_attr_a', node1.attributes),
                                                "updated_descriptor")

        val_to_delete = util.get_by_name('node_attr_a', node2.attributes)

        rs_to_update = []
        updated_dataset_id = None
        for resourcescenario in scenario1_to_update.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                updated_dataset_id = resourcescenario.dataset.id
                resourcescenario.dataset = descriptor.dataset
                rs_to_update.append(resourcescenario)
            elif ra_id == val_to_delete['id']:
                resourcescenario.dataset = None
                rs_to_update.append(resourcescenario)

        assert updated_dataset_id is not None

        scenario_ids = []
        scenario_ids.append(scenario1_to_update.id)
        scenario_ids.append(scenario2_to_update.id)

        result = hydra_base.bulk_update_resourcedata(scenario_ids, rs_to_update, user_id=pytest.root_user_id)

        assert len(result) == 2

        hydra_base.db.DBSession.expunge_all()

        updated_scenario1_data = self.get_scenario(scenario1_to_update.id)
        updated_scenario2_data = self.get_scenario(scenario2_to_update.id)

        for rs in updated_scenario1_data.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                assert rs.dataset.value == descriptor.dataset
        for rs in updated_scenario2_data.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                assert rs.dataset.value == descriptor.dataset



    def test_bulk_add_data(self, session, dateformat):

        data = []

        dataset1 = Dataset()

        dataset1.type = 'timeseries'
        dataset1.name = 'my time series'
        dataset1.unit = 'feet cubed'

        t1 = datetime.datetime.now()
        t2 = t1+datetime.timedelta(hours=1)
        t3 = t1+datetime.timedelta(hours=2)

        t1 = t1.strftime(dateformat)
        t2 = t2.strftime(dateformat)
        t3 = t3.strftime(dateformat)

        val_1 = 1.234
        val_2 = 2.345
        val_3 = 3.456

        ts_val = json.dumps({0: {t1: val_1,
                      t2: val_2,
                      t3: val_3}})
        dataset1.value = ts_val
        data.append(dataset1)

        dataset2 = Dataset()
        dataset2.type = 'descriptor'
        dataset2.name = 'Max Capacity'
        dataset2.unit = 'metres / second'

        dataset2.value ='I am an updated test!'

        data.append(dataset2)

        new_datasets = [Dataset(d) for d in hydra_base.bulk_insert_data(data, user_id=pytest.root_user_id)]

        assert len(new_datasets) == 2, "Data was not added correctly!"


    def test_clone(self, session, network_with_data):

        network =  network_with_data

        assert len(network.scenarios) == 1, "The network should have only one scenario!"

        #self.create_constraint(network)

        network = JSONObject(hydra_base.get_network(network.id, include_data='Y', user_id=pytest.root_user_id))

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        clone = self.clone_scenario(scenario_id)
        new_scenario = self.get_scenario(clone.id)


        updated_network = JSONObject(hydra_base.get_network(new_scenario.network_id, include_data='Y', user_id=pytest.root_user_id))


        assert len(updated_network.scenarios) == 2, "The network should have two scenarios!"

        assert len(updated_network.scenarios[1].resourcescenarios) > 0, "Data was not cloned!"

        scen_2_val = updated_network.scenarios[1].resourcescenarios[0].dataset.id
        scen_1_val = network.scenarios[0].resourcescenarios[0].dataset.id

        assert scen_2_val == scen_1_val, "Data was not cloned correctly"


  #      scen_1_constraint  = network.scenarios[0].constraints.Constraint[0].value
        #scen_2_constraint  = updated_network.scenarios[1].constraints.Constraint[0].value
#
 #       assert scen_1_constraint == scen_2_constraint, "Constraints did not clone correctly!"

        scen_1_resourcegroupitems = network.scenarios[0].resourcegroupitems
        scen_2_resourcegroupitems = updated_network.scenarios[1].resourcegroupitems

        assert len(scen_1_resourcegroupitems) == len(scen_2_resourcegroupitems)

        return updated_network

    def test_compare(self, session, network_with_data):

        network =  network_with_data


        assert len(network.scenarios) == 1, "The network should have only one scenario!"

    #    self.create_constraint(network)

        network = JSONObject(hydra_base.get_network(network.id, user_id=pytest.root_user_id))

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        clone = self.clone_scenario(scenario_id)
        new_scenario = self.get_scenario(clone.id)

    #    self.create_constraint(network, constant=4)

        resource_scenario = new_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset({
            'type' : 'descriptor',
            'name' : 'Max Capacity',
            'unit' : 'metres / second',
            'value' : 'I am an updated test!',
        })

        hydra_base.add_data_to_attribute(scenario_id, resource_attr_id, dataset, user_id=pytest.root_user_id)

        item_to_remove = new_scenario.resourcegroupitems[0].id

        hydra_base.db.DBSession.expunge_all()

        hydra_base.delete_resourcegroupitem(item_to_remove, user_id=pytest.root_user_id)

        updated_network = JSONObject(hydra_base.get_network(new_scenario.network_id, user_id=pytest.root_user_id))

        scenarios = updated_network.scenarios

        scenario_1 = None
        scenario_2 = None
        for s in scenarios:
            if s.id == new_scenario.id:
                scenario_1 = s
            else:
                scenario_2 = s

        scenario_diff = JSONObject(hydra_base.compare_scenarios(scenario_1.id, scenario_2.id, user_id=pytest.root_user_id))

        #print "Comparison result: %s"%(scenario_diff)

        assert len(scenario_diff.resourcescenarios) == 1, "Data comparison was not successful!"

     #   assert len(scenario_diff.constraints.common_constraints) == 1, "Constraint comparison was not successful!"

     #   assert len(scenario_diff.constraints.scenario_2_constraints) == 1, "Constraint comparison was not successful!"

        assert len(scenario_diff.groups.scenario_2_items) == 1, "Group comparison was not successful!"
        assert scenario_diff.groups.scenario_1_items == [], "Group comparison was not successful!"

        return updated_network

    def test_purge_scenario(self, session, network_with_data):

        #Make a network with 2 scenarios
        hydra_base.clone_scenario(network_with_data.scenarios[0].id, user_id=pytest.root_user_id)
        net = hydra_base.get_network(network_with_data.id, user_id=pytest.root_user_id)

        scenarios_before = net.scenarios

        assert len(scenarios_before) == 2

        hydra_base.purge_scenario(scenarios_before[1].id, user_id=pytest.root_user_id)

        updated_net = JSONObject(hydra_base.get_network(net.id, user_id=pytest.root_user_id))

        scenarios_after = updated_net.scenarios

        assert len(scenarios_after) == 1

        with pytest.raises(hydra_base.HydraError):
            self.get_scenario(scenarios_before[1].id)

        assert scenarios_after[0].id == scenarios_before[0].id

    def test_delete_scenario(self, session, network_with_data):

        #Make a network with 2 scenarios
        hydra_base.clone_scenario(network_with_data.scenarios[0].id, user_id=pytest.root_user_id)
        net = hydra_base.get_network(network_with_data.id, user_id=pytest.root_user_id)

        scenarios_before = net.scenarios

        assert len(scenarios_before) == 2

        hydra_base.set_scenario_status(scenarios_before[1].id, 'X', user_id=pytest.root_user_id)

        updated_net = JSONObject(hydra_base.get_network(net.id, user_id=pytest.root_user_id))

        scenarios_after_delete = updated_net.scenarios

        assert len(scenarios_after_delete) == 1

        hydra_base.set_scenario_status(scenarios_before[1].id, 'A', user_id=pytest.root_user_id)

        updated_net2 = JSONObject(hydra_base.get_network(net.id, user_id=pytest.root_user_id))

        scenarios_after_reactivate = updated_net2.scenarios

        assert len(scenarios_after_reactivate) == 2

        hydra_base.set_scenario_status(scenarios_before[1].id, 'X', user_id=pytest.root_user_id)
        hydra_base.clean_up_network(net.id, user_id=pytest.root_user_id)
        updated_net3 = JSONObject(hydra_base.get_network(net.id, user_id=pytest.root_user_id))
        scenarios_after_cleanup = updated_net3.scenarios
        assert len(scenarios_after_cleanup) == 1
        with pytest.raises(hydra_base.HydraError):
            self.get_scenario(scenarios_before[1].id)

    def test_lock_scenario(self, session, network_with_data):

        network =  network_with_data

        network = hydra_base.get_network(network.id, user_id=pytest.root_user_id)

        scenario_to_lock = network.scenarios[0]
        scenario_id = scenario_to_lock.id

        log.info('Cloning scenario %s'%scenario_id)
        clone = self.clone_scenario(scenario_id)
        unlocked_scenario = self.get_scenario(clone.id)

        log.info("Locking scenario")
        hydra_base.lock_scenario(scenario_id, user_id=pytest.root_user_id)

        locked_scenario = self.get_scenario(scenario_id)

        assert locked_scenario.locked == 'Y'

        dataset = Dataset()

        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'

        dataset.value = 'I am an updated test!'


        locked_resource_scenarios = []
        for rs in locked_scenario.resourcescenarios:
            if rs.dataset.type == 'descriptor':
                locked_resource_scenarios.append(rs)

        unlocked_resource_scenarios = []
        for rs in unlocked_scenario.resourcescenarios:
            if rs.dataset.type == 'descriptor':
                unlocked_resource_scenarios.append(rs)

        resource_attr_id = unlocked_resource_scenarios[0].resource_attr_id

        locked_resource_scenarios_value = None
        for rs in locked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.dataset

        unlocked_resource_scenarios_value = None
        for rs in unlocked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.dataset
        log.info("Updating a shared dataset")
        ds = unlocked_resource_scenarios_value

        updated_ds = JSONObject(hydra_base.update_dataset(ds.id, ds.name, ds.type, ds.value, ds.unit, ds.metadata, user_id=pytest.root_user_id))

        updated_unlocked_scenario = self.get_scenario(unlocked_scenario.id)
        #This should not have changed
        hydra_base.db.DBSession.expunge_all()
        updated_locked_scenario = self.get_scenario(locked_scenario.id)

        locked_resource_scenarios_value = None
        for rs in updated_locked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.datset

        unlocked_resource_scenarios_value = None
        for rs in updated_unlocked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.dataset

        with pytest.raises(hydra_base.HydraError):
            hydra_base.add_data_to_attribute(scenario_id, resource_attr_id, dataset, user_id=pytest.root_user_id)

        #THe most complicated situation is this:
        #Change a dataset in an unlocked scenario, which is shared by a locked scenario.
        #The original dataset should stay connected to the locked scenario and a new
        #dataset should be created for the edited scenario.
        hydra_base.add_data_to_attribute(unlocked_scenario.id, resource_attr_id, dataset, user_id=pytest.root_user_id)

        updated_unlocked_scenario = self.get_scenario(unlocked_scenario.id)

        hydra_base.db.DBSession.expunge_all()

        #This should not have changed
        updated_locked_scenario = self.get_scenario(locked_scenario.id)

        locked_resource_scenarios_value = None
        for rs in updated_locked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.dataset

        unlocked_resource_scenarios_value = None
        for rs in updated_unlocked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.dataset


        assert locked_resource_scenarios_value.hash != unlocked_resource_scenarios_value.hash

        item_to_remove = locked_scenario.resourcegroupitems[0].id

        with pytest.raises(hydra_base.HydraError):
            hydra_base.delete_resourcegroupitem(item_to_remove, user_id=pytest.root_user_id)

        log.info("Locking scenario")
        hydra_base.unlock_scenario(scenario_id, user_id=pytest.root_user_id)

        locked_scenario = self.get_scenario(scenario_id)

        assert locked_scenario.locked == 'N'


    def test_get_attribute_data(self, session, network_with_data):
        """
            Test for retrieval of data for an attribute in a scenario.
        """

        new_net = network_with_data

        s = new_net.scenarios[0]

        nodes = new_net.nodes

        resource_attr = util.get_by_name('node_attr_a', nodes[0].attributes)

        attr_id = resource_attr.attr_id

        all_matching_ras = []
        for n in nodes:
            for ra in n.attributes:
                if ra.attr_id == attr_id:
                    all_matching_ras.append(ra)
                    continue

        retrieved_resource_scenarios = hydra_base.get_attribute_datasets(attr_id, s.id, user_id=pytest.root_user_id)

        rs_dict  = {}
        for rs in retrieved_resource_scenarios:
            rs_dict[rs.resource_attr_id] = rs

        assert len(retrieved_resource_scenarios) == len(all_matching_ras)

        for rs in s.resourcescenarios:
            if rs_dict.get(rs.resource_attr_id):
                matching_rs = rs_dict[rs.resource_attr_id]
                assert str(rs.dataset.hash) == str(matching_rs.dataset.hash)

    def test_copy_data_from_scenario(self, session, network_with_data):

        """
            Test copy_data_from_scenario : test that one scenario
            can be updated to contain the data of another with the same
            resource attrs.
        """

        network =  network_with_data


        network = hydra_base.get_network(network.id, user_id=pytest.root_user_id)

        scenario = network.scenarios[0]
        source_scenario_id = scenario.id

        clone = self.clone_scenario(source_scenario_id)
        cloned_scenario = self.get_scenario(clone.id)

        resource_scenario = cloned_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'

        dataset.value = 'I am an updated test!'

        hydra_base.db.DBSession.expunge_all()

        hydra_base.add_data_to_attribute(source_scenario_id, resource_attr_id, dataset, user_id=pytest.root_user_id)

        scenario_diff = JSONObject(hydra_base.compare_scenarios(source_scenario_id, cloned_scenario.id, user_id=pytest.root_user_id))

        assert len(scenario_diff.resourcescenarios) == 1, "Data comparison was not successful!"

        hydra_base.copy_data_from_scenario([resource_attr_id], cloned_scenario.id, source_scenario_id, user_id=pytest.root_user_id)

        scenario_diff = JSONObject(hydra_base.compare_scenarios(source_scenario_id, cloned_scenario.id, user_id=pytest.root_user_id))

        assert len(scenario_diff.resourcescenarios) == 0, "Scenario update was not successful!"

    def test_set_resourcescenario_dataset(self, session, network_with_data):

        """
            Test the direct setting of a dataset id on a resource scenario
        """

        network =  network_with_data


        network = hydra_base.get_network(network.id, user_id=pytest.root_user_id)

        scenario = network.scenarios[0]
        source_scenario_id = scenario.id

        clone = self.clone_scenario(source_scenario_id)
        cloned_scenario = self.get_scenario(clone.id)

        resource_scenario = cloned_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'

        dataset.value = 'I am an updated test!'

        new_ds = hydra_base.add_dataset(dataset.type, dataset.value, dataset.unit, {}, dataset.name, flush=True, user_id=pytest.root_user_id)

        hydra_base.set_rs_dataset(resource_attr_id, source_scenario_id, new_ds.id, user_id=pytest.root_user_id)

        updated_net = hydra_base.get_network(network.id, user_id=pytest.root_user_id)

        updated_scenario = updated_net.scenarios[0]
        scenario_rs = updated_scenario.resourcescenarios
        for rs in scenario_rs:
            if rs.resource_attr_id == resource_attr_id:
                assert rs.dataset.value == 'I am an updated test!'

    def test_add_data_to_attribute(self, session, network_with_data):

        network =  network_with_data

        empty_ra = network.links[0].attributes[-1]

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'

        dataset.value = 'I am an updated test!'

        updated_resource_scenario = hydra_base.add_data_to_attribute(scenario_id, resource_attr_id, dataset, user_id=pytest.root_user_id)

        new_resource_scenario = hydra_base.add_data_to_attribute(scenario_id, empty_ra.id, dataset, user_id=pytest.root_user_id)

        assert updated_resource_scenario.dataset.value == 'I am an updated test!', "Value was not updated correctly!!"
        assert new_resource_scenario.dataset.value == 'I am an updated test!', "Value was not updated correctly!!"

if __name__ == '__main__':
    server.run()
