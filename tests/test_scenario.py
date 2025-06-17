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

import datetime
import copy
import json
import hydra_base
import pytest
from hydra_base.exceptions import HydraError
from hydra_base.lib.objects import JSONObject, Dataset

import logging
log = logging.getLogger(__name__)

class TestScenario:

    def get_scenario(self, scenario_id, get_parent_data=False):
        """
            Utility function wrapper for a function that's called regularly
            Introduced as the JSONObject wrapper can be controlled more easily
        """
        return JSONObject(client.get_scenario(scenario_id, get_parent_data=get_parent_data))
    def get_scenario_data(self, scenario_id, get_parent_data=False):
        """
            Utility function wrapper for a function that's called regularly
            Introduced as the JSONObject wrapper can be controlled more easily
        """
        return client.get_scenario_data(scenario_id, get_parent_data=get_parent_data)

    def test_get_scenario(self, client, network_with_data):
        """
            Test to get the scenarios attached to a dataset
        """

        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]

        rs = scenario.resourcescenarios


        scenario_retrieved = client.get_scenario(scenario.id)
        assert len(scenario_retrieved.resourcescenarios) == len(rs)
        assert len(scenario_retrieved.resourcegroupitems) == len(scenario.resourcegroupitems)

        scenario_no_data = client.get_scenario(scenario.id, include_data=False)
        assert scenario_no_data.resourcescenarios == []
        assert len(scenario_no_data.resourcegroupitems) == len(scenario.resourcegroupitems)

        scenario_no_groupitems = client.get_scenario(scenario.id, include_group_items=False)
        assert len(scenario_no_groupitems.resourcescenarios) == len(scenario.resourcescenarios)
        assert scenario_no_groupitems.resourcegroupitems == []

    def test_get_scenario_by_name(self, client, network_with_data):
        """
            Test to get the scenarios attached to a dataset
        """

        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]

        rs = scenario.resourcescenarios


        scenario_retrieved = client.get_scenario_by_name(network.id, scenario.name)
        assert len(scenario_retrieved.resourcescenarios) == len(rs)
        assert len(scenario_retrieved.resourcegroupitems) == len(scenario.resourcegroupitems)

        scenario_no_data = client.get_scenario_by_name(network.id, scenario.name, include_data=False)
        assert scenario_no_data.resourcescenarios == []
        assert len(scenario_no_data.resourcegroupitems) == len(scenario.resourcegroupitems)

        scenario_no_groupitems = client.get_scenario_by_name(network.id, scenario.name, include_group_items=False)
        assert len(scenario_no_groupitems.resourcescenarios) == len(scenario.resourcescenarios)
        assert scenario_no_groupitems.resourcegroupitems == []

    def test_update(self, client, network_with_data):

        network =  network_with_data

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset({
            'type'      : 'descriptor',
            'name'      : 'Max Capacity',
            'unit_id'   : client.get_unit_by_abbreviation("m s^-1").id,
            'value'     : 'I am an updated test!',
        })

        new_resource_scenario = client.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        assert new_resource_scenario.dataset.value == 'I am an updated test!', "Value was not updated correctly!!"

    def test_add_scenario(self, client, network_with_data, dateformat):
        """
            Test adding a new scenario to a network.
        """
        network = network_with_data

        new_scenario = copy.deepcopy(network.scenarios[0])
        new_scenario.id = -1
        new_scenario.name = 'Scenario 2'
        new_scenario.description = 'Scenario 2 Description'
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(hours=10)
        new_scenario.start_time = start_time.strftime(dateformat)
        new_scenario.end_time = end_time.strftime(dateformat)
        new_scenario.time_step = "1 day"

        node_attrs = network.nodes[0].attributes

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = client.testutils.create_descriptor(client.testutils.get_by_name('node_attr_a', node_attrs), "new_descriptor")
        timeseries = client.testutils.create_timeseries(client.testutils.get_by_name('node_attr_b', node_attrs))

        for r in new_scenario.resourcescenarios:
            if r.resource_attr_id == client.testutils.get_by_name('node_attr_a', node_attrs).id:
                r.dataset = descriptor.dataset
            elif r.resource_attr_id == client.testutils.get_by_name('node_attr_b', node_attrs).id:
                r.dataset = timeseries.dataset

        scenario = JSONObject(client.add_scenario(network.id, new_scenario))

        assert scenario is not None
        assert len(scenario.resourcegroupitems) > 0
        assert len(scenario.resourcescenarios) > 0


    def test_update_scenario(self, client, network_with_data, dateformat):
        """
            Test updating an existing scenario.
        """
        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        scenario.name = 'Updated Scenario'
        scenario.description = 'Updated Scenario Description'
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(hours=10)
        scenario.start_time = start_time.strftime(dateformat)
        scenario.end_time = end_time.strftime(dateformat)
        scenario.time_step = "1 day"

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes[0]
        node2 = network.nodes[-1]

        #Identify 1 resource group item to edit (the last one in the list).
        item_to_edit = scenario.resourcegroupitems[-1]
        #Just checking that we're not changing an item that is already
        #assigned to this node..
        assert scenario.resourcegroupitems[-1].node_id != node2.id
        scenario.resourcegroupitems[-1].node_id   = node2.id

        descriptor = client.testutils.create_descriptor(
            client.testutils.get_by_name('node_attr_a', node1.attributes),
            "updated_descriptor")

        for resourcescenario in scenario.resourcescenarios:
            if resourcescenario.attr_id == descriptor.attr_id:
                resourcescenario.dataset = descriptor.dataset

        updated_scenario = JSONObject(client.update_scenario(scenario))

        assert updated_scenario is not None
        assert updated_scenario.id == scenario.id
        assert updated_scenario.name == scenario.name
        assert updated_scenario.description == scenario.description
        assert updated_scenario.start_time == scenario.start_time
        assert updated_scenario.end_time == scenario.end_time
        assert updated_scenario.time_step  == scenario.time_step
        assert len(updated_scenario.resourcegroupitems) > 0

        for i in updated_scenario.resourcegroupitems:
            if i.id == item_to_edit.id:
                assert i.node_id == node2.id
        assert len(updated_scenario.resourcescenarios) > 0

        for data in updated_scenario.resourcescenarios:
            if data.attr_id == descriptor['attr_id']:
                assert data.dataset.value == descriptor.dataset.value

    def test_get_dataset_scenarios(self, client, network_with_data):
        """
            Test to get the scenarios attached to a dataset
        """

        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        rs = scenario.resourcescenarios

        dataset_id_to_check = rs[0].dataset.id

        dataset_scenarios = [JSONObject(s) for s in client.get_dataset_scenarios(dataset_id_to_check)]

        assert len(dataset_scenarios) >= 1
        assert scenario.id in [s.id for s in dataset_scenarios]

        clone = client.clone_scenario(scenario.id)
        new_scenario = client.get_scenario(clone.id)

        dataset_scenarios = [JSONObject(s) for s in client.get_dataset_scenarios(dataset_id_to_check)]

        assert len(dataset_scenarios) >= 2

        assert set([scenario.id, new_scenario.id]).issubset(set([s.id for s in dataset_scenarios]))

    def test_update_resourcedata(self, client, network_with_data):
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
        node1attr = client.testutils.get_by_name('node_attr_a', node1.attributes)
        node2 = network.nodes[-1]
        val_to_delete = client.testutils.get_by_name('node_attr_a', node2.attributes)

        descriptor = client.testutils.create_descriptor(node1attr,
                                                "updated_descriptor")

        rs_to_update = []
        updated_dataset_id = None
        scenario = client.get_scenario(scenario.id, include_data=True)
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

        new_resourcescenarios = [JSONObject(rs) for rs in client.update_resourcedata(scenario.id, rs_to_update)]

        assert len(new_resourcescenarios) == 1

        for rs in new_resourcescenarios:
            if rs.resource_attr_id == descriptor.resource_attr_id:
                assert rs.dataset.value == descriptor.dataset.value

        updated_scenario = client.get_scenario(scenario.id)

        num_new_rs = len(updated_scenario.resourcescenarios)

        assert num_new_rs == num_old_rs - 1


        for u_rs in updated_scenario.resourcescenarios:
            for rs in new_resourcescenarios:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    assert str(u_rs.dataset.value) == str(rs.dataset.value)
                    break

    def test_update_resourcedata_single_dataset_update_and_delete(self, client, network_with_data):
        """
            Test to ensure update_resourcedata does not update other
            datasets that it should not.
        """
        network = network_with_data

        scenario_1 = network.scenarios[0]
        scenario_2 = client.clone_scenario(scenario_1.id)
        scenario_2 = client.get_scenario(scenario_2.id)

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

        client.update_resourcedata(scenario_1.id, ts_to_delete)

        client.update_resourcedata(scenario_2.id, ts_to_update)

        scenario_2_updated_1 = client.get_scenario(scenario_2.id)
        for rs in scenario_2_updated_1.resourcescenarios:
            if rs.resource_attr_id == ra_id:
                assert json.loads(rs.dataset.value) == json.loads(new_value)
                #Either the dataset is the same dataset, just updated or the dataset
                #has been removed and linked to a previous dataset, which must have a lower ID.
                assert rs.dataset.id <= ts_id
                break
        else:
            raise Exception("Couldn't find resource scenario. SOmething went wrong.")

    def test_update_resourcedata_consistency(self, client, network_with_data):
        """
            Test to ensure update_resourcedata does not update other
            datasets that it should not.
        """
        network = network_with_data

        scenario_1 = client.testutils.get_by_name("Scenario 1", network.scenarios)
        scenario_2 = client.clone_scenario(scenario_1.id)
        scenario_2 = client.get_scenario(scenario_2.id)

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes[0]


        descriptor = client.testutils.create_descriptor(client.testutils.get_by_name('node_attr_a', node1.attributes),
                                                "updated_descriptor")

        rs_to_update = self._get_rs_to_update(scenario_1, descriptor)


        #Update the value
        updated_data = client.update_resourcedata(scenario_1.id, rs_to_update)

        new_resourcescenarios = [JSONObject(rs) for rs in updated_data]

        rs_1_id = None
        updated_scenario_1 = client.get_scenario(scenario_1.id)
        for u_rs in updated_scenario_1.resourcescenarios:
            for rs in new_resourcescenarios:
                if u_rs.resource_attr_id == rs.resource_attr_id:
                    log.info(u_rs.dataset)
                    log.info(rs.dataset)
                    assert str(u_rs.dataset.value) == str(rs.dataset.value)
                    rs_1_id = u_rs.dataset
                    break

        scalar = client.testutils.create_descriptor(client.testutils.get_by_name('node_attr_a', node1.attributes), 200)

        rs_to_update = self._get_rs_to_update(scenario_2, scalar)

        new_resourcescenarios = client.update_resourcedata(scenario_2.id,
                                                           rs_to_update)
        rs_2_id = None
        #Check that scenario 2 has been updated correctly.
        updated_scenario_2 = client.get_scenario(scenario_2.id)
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

    def test_get_attributes_for_resource(self, client, network_with_data):
        """
            Test to check leng's questions about this not working correctly.
        """
        network = network_with_data

        #Create the new scenario
        scenario = network.scenarios[0]
        node1 = network.nodes[0]

        ra_to_update = client.testutils.get_by_name('node_attr_a', node1.attributes).id

        updated_val = None

        rs_to_update = []
        for resourcescenario in scenario.resourcescenarios:
            ra_id = resourcescenario.resource_attr_id
            if ra_id == ra_to_update:
                updated_val = resourcescenario.dataset.value
                resourcescenario.dataset.name = 'I am an updated dataset name'
                rs_to_update.append(resourcescenario)

        client.get_attributes_for_resource(network.id, scenario.id, 'NODE', [node1.id])

        client.update_resourcedata(scenario.id, rs_to_update)

        new_node_data = [JSONObject(d) for d in client.get_attributes_for_resource(network.id, scenario.id, 'NODE', [node1.id])]

        for new_val in new_node_data:
            if new_val.dataset.value == updated_val:
                assert new_val.dataset.name == 'I am an updated dataset name'

    def test_bulk_update_resourcedata(self, client, network_with_data):
        """
            Test updating scenario data in a number of scenarios at once.
            2 main points to test: 1: setting a value to null should remove
            the resource scenario
            2: changing the value should create a new dataset
        """
        network1 = network_with_data
        scenario1_to_update = network1.scenarios[0]
        clone = client.clone_scenario(network1.scenarios[0].id)
        scenario2_to_update = client.get_scenario(clone.id)

        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network1.nodes[0]

        node1attr = client.testutils.get_by_name('node_attr_a', node1.attributes)

        node2 = network1.nodes[-1]

        descriptor = client.testutils.create_descriptor(node1attr, "updated_descriptor")

        val_to_delete = client.testutils.get_by_name('node_attr_a', node2.attributes)

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

        result = client.bulk_update_resourcedata(scenario_ids, rs_to_update)

        assert len(result) == 2

        updated_scenario1_data = client.get_scenario(scenario1_to_update.id)
        updated_scenario2_data = client.get_scenario(scenario2_to_update.id)

        for rs in updated_scenario1_data.resourcescenarios:
            ra_id = rs.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                assert rs.dataset.value == descriptor.dataset.value
        for rs in updated_scenario2_data.resourcescenarios:
            ra_id = rs.resource_attr_id
            if ra_id == descriptor['resource_attr_id']:
                assert rs.dataset.value == descriptor.dataset.value



    def test_bulk_add_data(self, client, dateformat):

        data = []

        dataset1 = Dataset()

        dataset1.type = 'timeseries'
        dataset1.name = 'my time series'
        dataset1.unit_id = client.get_unit_by_abbreviation("ft^3").id


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
        dataset2.unit_id = client.get_unit_by_abbreviation("m s^-1").id

        dataset2.value ='I am an updated test!'

        data.append(dataset2)

        new_datasets = [Dataset(d) for d in client.bulk_insert_data(data)]

        assert len(new_datasets) == 2, "Data was not added correctly!"


    def test_bulk_insert_data(self, client, dateformat):
        import random
        from hydra_base.lib.data import bulk_insert_data

        num_datasets = 10
        datasets = []
        unit_id = client.get_unit_by_abbreviation("m s^-1").id
        for idx in range(num_datasets):
            ds = Dataset()
            ds.name = f"Bulk dataset {idx}"
            ds.type = "ARRAY"
            ds.unit_id = unit_id
            # Every third dataset is large
            data_sz = 10 if idx % 3 else 8192
            ds.value = [random.uniform(1, 100) for _ in range(data_sz)]

            datasets.append(ds)

        inserted = [Dataset(ds) for ds in client.bulk_insert_data(datasets)]
        assert len(inserted) == num_datasets, "Datasets insertion count mismatch"

        for ds in inserted:
            retrieved = client.get_dataset(ds.id)
            assert ds.value == retrieved.value


    def test_clone_scenario(self, client, network_with_data):

        network =  network_with_data

        assert len(network.scenarios) == 1, "The network should have only one scenario!"

        #self.create_constraint(network)

        network = JSONObject(client.get_network(network.id, include_data=True))

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        clone = client.clone_scenario(scenario_id)
        clone1 = client.clone_scenario(scenario_id, scenario_name="My Cloned Scenario")
        clone2 = client.clone_scenario(scenario_id, retain_results=True)

        scenario_diff = JSONObject(client.compare_scenarios(scenario.id, clone.id))
        assert len(scenario_diff.resourcescenarios) == 0, "Scenarios should be the same but are not"

        #This should fail because there's already another scenario with this name
        with pytest.raises(HydraError):
            client.clone_scenario(scenario_id, scenario_name="My Cloned Scenario")
        #to deal with the broken flush caused by this exception


        new_scenario = client.get_scenario(clone.id)

        new_scenario_new_name = client.get_scenario(clone1.id)
        new_scenario_with_results = client.get_scenario(clone2.id)

        assert new_scenario_new_name.name == "My Cloned Scenario"

        assert len(new_scenario_with_results.resourcescenarios) > len(new_scenario.resourcescenarios)


        updated_network = JSONObject(client.get_network(new_scenario.network_id, include_data=True))


        assert len(updated_network.scenarios) == 4, "The network should have 4 scenarios!"

        assert len(updated_network.scenarios[1].resourcescenarios) > 0, "Data was not cloned!"

        #Find the correct scenarios to compare, to satisfy stupid postgres
        for original_scenario in network.scenarios:
            for updated_scenario in network.scenarios:
                if original_scenario.name == updated_scenario.name:
                    scen_2_vals = set([rs.dataset.value for rs in original_scenario.resourcescenarios])
                    scen_1_vals = set([rs.dataset.value for rs in updated_scenario.resourcescenarios])

                    assert scen_2_vals == scen_1_vals, "Data was not cloned correctly"
                    break


  #      scen_1_constraint  = network.scenarios[0].constraints.Constraint[0].value
        #scen_2_constraint  = updated_network.scenarios[1].constraints.Constraint[0].value
#
 #       assert scen_1_constraint == scen_2_constraint, "Constraints did not clone correctly!"

        scen_1_resourcegroupitems = network.scenarios[0].resourcegroupitems
        scen_2_resourcegroupitems = updated_network.scenarios[1].resourcegroupitems

        assert len(scen_1_resourcegroupitems) == len(scen_2_resourcegroupitems)

    def test_get_inherited_data(self, client, network_with_child_scenario):

        network = network_with_child_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]

        assert len(child.resourcescenarios) == 0, "There's data in the child but there shouldn't be"

        #Check that the new scenario contains no data (as we've requested only its own data)
        retrieved_child_scenario = client.get_scenario(child.id)
        assert len(retrieved_child_scenario.resourcescenarios) == 0


        #Check that the new scenario contains all its data (that of its parent)
        retrieved_child_scenario = client.get_scenario(child.id, get_parent_data=True)
        assert len(retrieved_child_scenario.resourcescenarios) == len(parent.resourcescenarios)

    def test_get_third_level_inherited_data(self, client, network_with_grandchild_scenario):

        network = network_with_grandchild_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]
        grandchild  = sorted_scenario[2]

        assert len(child.resourcescenarios) == 0, "There's data in the child but there shouldn't be"
        assert len(grandchild.resourcescenarios) == 0, "There's data in the child but there shouldn't be"

        #Check that the new scenario contains no data (as we've requested only its own data)
        retrieved_child_scenario = client.get_scenario(child.id)
        assert len(retrieved_child_scenario.resourcescenarios) == 0

        #Check that the grandchild scenario also contains no data (as we've requested only its own data)
        retrieved_grandchild_scenario = client.get_scenario(grandchild.id)
        assert len(retrieved_grandchild_scenario.resourcescenarios) == 0


        #Check that the grandchild scenario contains all its data (that of its grandparent)
        retrieved_grandchild_scenario = client.get_scenario(grandchild.id, get_parent_data=True)
        assert len(retrieved_grandchild_scenario.resourcescenarios) == len(parent.resourcescenarios)


        #do the same test for groups
        group_id = network.resourcegroups[0].id
        parent_group_items = client.get_resourcegroupitems(group_id, parent.id)
        assert len(parent_group_items) > 0
        grandchild_group_items = client.get_resourcegroupitems(group_id, grandchild.id)
        assert len(grandchild_group_items) == 0
        inherited_group_items = client.get_resourcegroupitems(group_id, grandchild.id, get_parent_items=True)
        assert len(inherited_group_items) == len(parent_group_items)



    def test_inherited_get_resource_scenario(self, client, network_with_child_scenario):

        network = network_with_child_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]

        ra_to_query = parent.resourcescenarios[0].resource_attr_id

        #Request an RS without doing the parent lookup
        with pytest.raises(HydraError):
            rs = client.get_resource_scenario(ra_to_query, child.id)

        rs = client.get_resource_scenario(ra_to_query, child.id, get_parent_data=True)
        assert rs.scenario_id == parent.id

    def test_inherited_get_scenario_data(self, client, network_with_child_scenario):

        network = network_with_child_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]

        child_scenario_data = client.get_scenario_data(child.id)
        assert len(child_scenario_data) == 0

        parent_scenario_data = client.get_scenario_data(parent.id)
        inherited_scenario_data = client.get_scenario_data(child.id, get_parent_data=True)

        assert len(inherited_scenario_data) == len(parent_scenario_data)

    def test_inherited_get_resource_data(self, client, network_with_child_scenario):

        network = network_with_child_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]

        resource_to_query = network.nodes[0]

        child_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           child.id)

        assert len(child_resource_data) == 0

        parent_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           parent.id)

        inherited_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           child.id,
                                                           get_parent_data=True)

        assert len(inherited_resource_data) == len(parent_resource_data)

    def test_inherited_add_data_to_child(self, client, network_with_child_scenario):

        network = network_with_child_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]

        #Specifically chose an attribute to update to remove non-determinisim
        #when trying to choose a resource to query later. Attribute 'node_attr_c' is a scalar, as
        #defined in the network creation. We use the name instead of an index or ID because
        #postgres ordering is non-deterministic.
        ra_to_update = None
        for a in network.nodes[0].attributes:
            if a.name == 'node_attr_a':
                ra_to_update = a.id

        #Update a value in the parent, leaving 1 different value between parent and child
        for rs in parent.resourcescenarios:
            if rs.resource_attr_id == ra_to_update:
                rs_to_update = rs
                break

        #Set the new value to 999
        rs_to_update.dataset.value = 999

        client.update_resourcedata(child.id,
                                        [rs_to_update])


        #Check that the new scenario contains all its data (that of its parent)
        new_scenario = client.get_scenario(child.id, get_parent_data=True)
        assert len(new_scenario.resourcescenarios) == len(parent.resourcescenarios)

        #Check that the new scenario contains no data (as we've requested only its own data)
        new_scenario = client.get_scenario(child.id)
        assert len(new_scenario.resourcescenarios) == 1

        child_scenario_data = client.get_scenario_data(child.id)
        assert len(child_scenario_data) == 1

        parent_scenario_data = client.get_scenario_data(parent.id)
        inherited_scenario_data = client.get_scenario_data(child.id, get_parent_data=True)

        #This returns 1 more for the inherited data because of the presence of
        #a new dataset in this result set (this function returns the UNIQUE) datasets
        #in each scenario.
        assert len(inherited_scenario_data) == len(parent_scenario_data)+1

        resource_to_query = network.nodes[0]

        child_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           child.id)

        assert len(child_resource_data) == 1

        parent_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           parent.id)

        inherited_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           child.id,
                                                           get_parent_data=True)

        assert len(inherited_resource_data) == len(parent_resource_data)


    def test_inherited_add_data_to_grandchild(self, client, network_with_grandchild_scenario):
        """
            Test inheritence by checking the get_scenario_data and get_scenario functions return the data
            of the grandchild and not the child, of the same RA
        """

        network = network_with_grandchild_scenario

        sorted_scenario = sorted(network.scenarios, key=lambda x : x.id )

        parent = sorted_scenario[0]
        child  = sorted_scenario[1]
        grandchild  = sorted_scenario[2]

        #Specifically chose an attribute to update to remove non-determinisim
        #when trying to choose a resource to query later. Attributea'node_attr_c' is a scalar, as
        #defined in the network creation. We use the name instead of an index or ID because
        #postgres ordering is non-deterministic.
        ra_to_update = None
        for a in network.nodes[0].attributes:
            if a.name == 'node_attr_a':
                ra_to_update = a.id

        #Update a value in the parent, leaving 1 different value between parent and child
        for rs in parent.resourcescenarios:
            if rs.resource_attr_id == ra_to_update:
                rs_to_update = rs
                break

        previous_rs_value = rs_to_update.dataset.value

        rs_to_update.dataset.value = 999

        client.update_resourcedata(child.id, [rs_to_update])

        rs_to_update.dataset.value = 1000

        client.update_resourcedata(grandchild.id, [rs_to_update])

        #check the child and grandchild have the same amount of data
        child_scenario = client.get_scenario(child.id, get_parent_data=True)
        grandchild_scenario = client.get_scenario(grandchild.id, get_parent_data=True)
        assert len(child_scenario.resourcescenarios) == len(grandchild_scenario.resourcescenarios)

        #find the RS in each scenario
        for rs in child_scenario.resourcescenarios:
            if rs.resource_attr_id == rs_to_update.resource_attr_id:
                assert int(rs.dataset.value) == 999
        #Check the grandchild has the correct value
        for rs in grandchild_scenario.resourcescenarios:
            if rs.resource_attr_id == rs_to_update.resource_attr_id:
                assert int(rs.dataset.value) == 1000


        #Check that the new scenario contains no data (as we've requested only its own data)
        child_scenario = client.get_scenario(child.id)
        grandchild_scenario = client.get_scenario(grandchild.id)
        assert len(child_scenario.resourcescenarios) == 1
        assert len(grandchild_scenario.resourcescenarios) == 1
        assert int(child_scenario.resourcescenarios[0].dataset.value) == 999
        assert int(grandchild_scenario.resourcescenarios[0].dataset.value) == 1000


        child_scenario_data = client.get_scenario_data(child.id)
        grandchild_scenario_data = client.get_scenario_data(grandchild.id)
        assert len(child_scenario_data) == 1
        assert len(grandchild_scenario_data) == 1

        parent_scenario_data = client.get_scenario_data(parent.id)
        child_scenario_data = client.get_scenario_data(child.id, get_parent_data=True)
        grandchild_scenario_data = client.get_scenario_data(grandchild.id, get_parent_data=True)

        #This returns 1 more for the inherited data because of the presence of
        #a new dataset in this result set (this function returns the UNIQUE) datasets
        #in each scenario.
        assert child_scenario_data != grandchild_scenario_data
        assert len(child_scenario_data) == len(parent_scenario_data)+1
        assert len(grandchild_scenario_data) == len(parent_scenario_data)+1

        resource_to_query = network.nodes[0]

        child_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           child.id)

        assert len(child_resource_data) == 1

        parent_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           parent.id)

        inherited_resource_data = client.get_resource_data('NODE',
                                                           resource_to_query.id,
                                                           child.id,
                                                           get_parent_data=True)

        assert len(inherited_resource_data) == len(parent_resource_data)


    def test_compare(self, client, network_with_data):

        network =  network_with_data


        assert len(network.scenarios) == 1, "The network should have only one scenario!"

        network = JSONObject(client.get_network(network.id))

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        clone = client.clone_scenario(scenario_id, retain_results=True)
        new_scenario = client.get_scenario(clone.id)

        scenario_diff = JSONObject(client.compare_scenarios(scenario.id, new_scenario.id))

        assert len(scenario_diff.resourcescenarios) == 0, "Scenarios should be the same but are not"

        resource_scenario = new_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset({
            'type' : 'descriptor',
            'name' : 'Max Capacity',
            'unit_id' : client.get_unit_by_abbreviation("m s^-1").id,
            'value' : 'I am an updated test!',
        })

        client.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        item_to_remove = new_scenario.resourcegroupitems[0].id

        client.delete_resourcegroupitem(item_to_remove)

        updated_network = JSONObject(client.get_network(new_scenario.network_id))

        scenarios = updated_network.scenarios

        scenario_1 = None
        scenario_2 = None
        for s in scenarios:
            if s.id == new_scenario.id:
                scenario_1 = s
            else:
                scenario_2 = s

        scenario_diff = JSONObject(client.compare_scenarios(scenario_1.id, scenario_2.id))

        #print "Comparison result: %s"%(scenario_diff)

        assert len(scenario_diff.resourcescenarios) == 1, "Data comparison was not successful!"

     #   assert len(scenario_diff.constraints.common_constraints) == 1, "Constraint comparison was not successful!"

     #   assert len(scenario_diff.constraints.scenario_2_constraints) == 1, "Constraint comparison was not successful!"

        assert len(scenario_diff.groups.scenario_2_items) == 1, "Group comparison was not successful!"
        assert scenario_diff.groups.scenario_1_items == [], "Group comparison was not successful!"

    def test_purge_scenario(self, client, network_with_data):

        #Make a network with 2 scenarios
        client.clone_scenario(network_with_data.scenarios[0].id)
        net = client.get_network(network_with_data.id)

        scenarios_before = net.scenarios

        assert len(scenarios_before) == 2

        client.purge_scenario(scenarios_before[1].id)

        updated_net = JSONObject(client.get_network(net.id))

        scenarios_after = updated_net.scenarios

        assert len(scenarios_after) == 1

        with pytest.raises(HydraError):
            client.get_scenario(scenarios_before[1].id)

        assert scenarios_after[0].id == scenarios_before[0].id

    def test_delete_scenario(self, client, network_with_data):

        #Make a network with 2 scenarios
        client.clone_scenario(network_with_data.scenarios[0].id)
        net = client.get_network(network_with_data.id)

        scenarios_before = net.scenarios

        assert len(scenarios_before) == 2

        client.set_scenario_status(scenarios_before[1].id, 'X')

        updated_net = JSONObject(client.get_network(net.id))

        scenarios_after_delete = updated_net.scenarios

        assert len(scenarios_after_delete) == 1

        client.set_scenario_status(scenarios_before[1].id, 'A')

        updated_net2 = JSONObject(client.get_network(net.id))

        scenarios_after_reactivate = updated_net2.scenarios

        assert len(scenarios_after_reactivate) == 2

        client.set_scenario_status(scenarios_before[1].id, 'X')
        client.clean_up_network(net.id)
        updated_net3 = JSONObject(client.get_network(net.id))
        scenarios_after_cleanup = updated_net3.scenarios
        assert len(scenarios_after_cleanup) == 1
        with pytest.raises(HydraError):
            client.get_scenario(scenarios_before[1].id)


    def test_delete_resource_scenario(self, client, network_with_data):
        """
        """

        scenario = network_with_data.scenarios[0]

        before_rs = scenario.resourcescenarios

        rs_to_delete = before_rs[0]

        client.delete_resource_scenario(scenario.id, rs_to_delete.resource_attr_id, quiet=True)

        #test the quiet feature by trying to delete a non-existent RS
        with pytest.raises(HydraError):
            client.delete_resource_scenario(scenario.id, 999, quiet=False)

        client.delete_resource_scenario(scenario.id, 999, quiet=True)

        updated_scenario = client.get_scenario(scenario.id)

        assert len(updated_scenario.resourcescenarios) == len(before_rs)-1

    def test_delete_resource_scenarios(self, client, network_with_data):
        """
        """
        scenario = network_with_data.scenarios[0]

        before_rs = scenario.resourcescenarios

        rs_to_delete = [before_rs[0].resource_attr_id,before_rs[1].resource_attr_id]

        client.delete_resource_scenarios(scenario.id, rs_to_delete, quiet=True)

        #test the quiet feature by trying to delete a non-existent RS
        with pytest.raises(HydraError):
            client.delete_resource_scenarios(scenario.id, rs_to_delete, quiet=False)

        client.delete_resource_scenarios(scenario.id, rs_to_delete, quiet=True)

        updated_scenario = client.get_scenario(scenario.id)

        assert len(updated_scenario.resourcescenarios) == len(before_rs)-2

    def test_lock_scenario(self, client, network_with_data):

        network =  network_with_data

        network = client.get_network(network.id)

        scenario_to_lock = network.scenarios[0]
        scenario_id = scenario_to_lock.id

        log.info('Cloning scenario %s'%scenario_id)
        clone = client.clone_scenario(scenario_id)
        unlocked_scenario = client.get_scenario(clone.id)

        log.info("Locking scenario")
        client.lock_scenario(scenario_id)

        locked_scenario = client.get_scenario(scenario_id)

        assert locked_scenario.locked == 'Y'

        dataset = Dataset()

        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit_id = client.get_unit_by_abbreviation("m s^-1").id

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

        updated_ds = JSONObject(client.update_dataset(ds.id, ds.name, ds.type, ds.value, ds.unit_id, ds.metadata))

        updated_unlocked_scenario = client.get_scenario(unlocked_scenario.id)

        updated_locked_scenario = client.get_scenario(locked_scenario.id)

        locked_resource_scenarios_value = None
        for rs in updated_locked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                locked_resource_scenarios_value = rs.datset

        unlocked_resource_scenarios_value = None
        for rs in updated_unlocked_scenario.resourcescenarios:
            if rs.resource_attr_id == resource_attr_id:
                unlocked_resource_scenarios_value = rs.dataset

        with pytest.raises(HydraError):
            client.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        #THe most complicated situation is this:
        #Change a dataset in an unlocked scenario, which is shared by a locked scenario.
        #The original dataset should stay connected to the locked scenario and a new
        #dataset should be created for the edited scenario.
        client.add_data_to_attribute(unlocked_scenario.id, resource_attr_id, dataset)

        updated_unlocked_scenario = client.get_scenario(unlocked_scenario.id)

        #This should not have changed
        updated_locked_scenario = client.get_scenario(locked_scenario.id)

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

        with pytest.raises(HydraError):
            client.delete_resourcegroupitem(item_to_remove)

        log.info("Locking scenario")
        client.unlock_scenario(scenario_id)

        locked_scenario = client.get_scenario(scenario_id)

        assert locked_scenario.locked == 'N'


    def test_get_attribute_data(self, client, network_with_data):
        """
            Test for retrieval of data for an attribute in a scenario.
        """

        new_net = network_with_data

        s = new_net.scenarios[0]

        nodes = new_net.nodes

        resource_attr = client.testutils.get_by_name('node_attr_a', nodes[0].attributes)

        attr_id = resource_attr.attr_id

        all_matching_ras = []
        for n in nodes:
            for ra in n.attributes:
                if ra.attr_id == attr_id:
                    all_matching_ras.append(ra)
                    continue

        retrieved_resource_scenarios = client.get_attribute_datasets(attr_id, s.id)

        rs_dict = {}
        for rs in retrieved_resource_scenarios:
            rs_dict[rs.resource_attr_id] = rs

        assert len(retrieved_resource_scenarios) == len(all_matching_ras)

        for rs in s.resourcescenarios:
            if rs_dict.get(rs.resource_attr_id):
                matching_rs = rs_dict[rs.resource_attr_id]
                assert str(rs.dataset.hash) == str(matching_rs.dataset.hash)

    def test_copy_data_from_scenario(self, client, network_with_data):

        """
            Test copy_data_from_scenario : test that one scenario
            can be updated to contain the data of another with the same
            resource attrs.
        """

        network = network_with_data


        network = client.get_network(network.id)

        scenario = network.scenarios[0]
        source_scenario_id = scenario.id

        clone = client.clone_scenario(source_scenario_id, retain_results=True)
        cloned_scenario = client.get_scenario(clone.id)

        resource_scenario = cloned_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit_id = client.get_unit_by_abbreviation("m s^-1").id

        dataset.value = 'I am an updated test!'

        client.add_data_to_attribute(source_scenario_id, resource_attr_id, dataset)

        scenario_diff = JSONObject(client.compare_scenarios(source_scenario_id, cloned_scenario.id))

        assert len(scenario_diff.resourcescenarios) == 1, "Data comparison was not successful!"

        client.copy_data_from_scenario([resource_attr_id], cloned_scenario.id, source_scenario_id)

        scenario_diff = JSONObject(client.compare_scenarios(source_scenario_id, cloned_scenario.id))

        assert len(scenario_diff.resourcescenarios) == 0, "Scenario update was not successful!"

    def test_merge_scenario(self, client, networkmaker):

        """
            Test merge_scenario : test that one scenario
            can be updated to contain the data of another with the same
            resource attrs.
        """

        sourcenetwork = networkmaker.create()
        targetnetwork = networkmaker.create()

        source_scenario = sourcenetwork.scenarios[0]
        target_scenario = targetnetwork.scenarios[0]
        source_scenario_id = source_scenario.id

        resource_scenario = source_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit_id = client.get_unit_by_abbreviation("m s^-1").id
        dataset.value = 'I am an updated test!'

        client.add_data_to_attribute(source_scenario_id, resource_attr_id, dataset)

        source_scenario = client.get_scenario(source_scenario.id)
        target_scenario = client.get_scenario(target_scenario.id)
        source_rs = sorted(source_scenario.resourcescenarios, key=lambda x: x.resource_attr_id)
        target_rs = sorted(target_scenario.resourcescenarios, key=lambda x: x.resource_attr_id)
        differences = []
        for i, rs in enumerate(source_rs):
            if rs.dataset.value != target_rs[i].dataset.value:
                differences.append(rs.id)
        assert len(differences) > 0

        client.merge_scenarios(source_scenario.id, target_scenario.id)

        #Now check that the scenarios are the same by making sure all the datasets
        #are the same
        updated_target_network = client.get_network(targetnetwork.id)
        merged_scenario = client.get_scenario(sorted(updated_target_network.scenarios, key=lambda x: x.cr_date)[-1].id)
        source_scenario = client.get_scenario(source_scenario.id)
        source_rs = sorted(source_scenario.resourcescenarios, key=lambda x: x.resource_attr_id)
        merged_rs = sorted(merged_scenario.resourcescenarios, key=lambda x: x.resource_attr_id)
        differences = []
        for i, rs in enumerate(source_rs):
            if rs.dataset.value != merged_rs[i].dataset.value:
                differences.append(rs.id)
        assert len(differences) == 0


    def test_set_resourcescenario_dataset(self, client, network_with_data):

        """
            Test the direct setting of a dataset id on a resource scenario
        """

        network = network_with_data


        network = client.get_network(network.id)

        scenario = network.scenarios[0]
        source_scenario_id = scenario.id

        clone = client.clone_scenario(source_scenario_id)
        cloned_scenario = client.get_scenario(clone.id)

        resource_scenario = cloned_scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit_id = client.get_unit_by_abbreviation("m s^-1").id

        dataset.value = 'I am an updated test!'

        new_ds = client.add_dataset(dataset.type, dataset.value, dataset.unit_id, {}, dataset.name, flush=True)

        client.set_rs_dataset(resource_attr_id, source_scenario_id, new_ds.id)

        updated_net = client.get_network(network.id)

        updated_scenario = updated_net.scenarios[0]
        scenario_rs = updated_scenario.resourcescenarios
        for rs in scenario_rs:
            if rs.resource_attr_id == resource_attr_id:
                assert rs.dataset.value == 'I am an updated test!'

    def test_add_data_to_attribute(self, client, network_with_data):

        network = network_with_data

        empty_ra = network.links[0].attributes[-1]

        scenario = network.scenarios[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = Dataset()
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit_id = client.get_unit_by_abbreviation("m s^-1").id

        dataset.value = 'I am an updated test!'

        updated_resource_scenario = client.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        new_resource_scenario = client.add_data_to_attribute(scenario_id, empty_ra.id, dataset)

        assert updated_resource_scenario.dataset.value == 'I am an updated test!', "Value was not updated correctly!!"
        assert new_resource_scenario.dataset.value == 'I am an updated test!', "Value was not updated correctly!!"
