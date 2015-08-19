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
from suds import WebFault
import json
log = logging.getLogger(__name__)

class TimeSeriesTest(server.SoapServerTest):
    """
        Test for timeseries-based functionality
    """
    def test_relative_timeseries(self):
        """
            Test for relative timeseries for example, x number of hours from hour 0.
        """
        net = self.build_network()

        relative_ts = self.create_relative_timeseries()

        s = net['scenarios'].Scenario[0]
        for rs in s['resourcescenarios'].ResourceScenario:
            if rs['value']['type'] == 'timeseries':
                rs['value']['value'] = relative_ts
        
        new_network_summary = self.client.service.add_network(net)
        new_net = self.client.service.get_network(new_network_summary.id)

        new_s = new_net.scenarios.Scenario[0]
        new_rss = new_s.resourcescenarios.ResourceScenario
        for new_rs in new_rss:
            if new_rs.value.type == 'timeseries':
                ret_ts_dict = {}
                ret_ts_dict = json.loads(new_rs.value.value).values()[0]
                client_ts   = json.loads(relative_ts)['0']
                for new_timestep in client_ts.keys():
                    assert ret_ts_dict.get(new_timestep) == client_ts[new_timestep]
        
        return new_net

    def test_arbitrary_timeseries(self):
        net = self.build_network()

        arbitrary_ts = self.create_arbitrary_timeseries()

        s = net['scenarios'].Scenario[0]
        for rs in s['resourcescenarios'].ResourceScenario:
            if rs['value']['type'] == 'timeseries':
                rs['value']['value'] =arbitrary_ts 
        
        new_network_summary = self.client.service.add_network(net)
        new_net = self.client.service.get_network(new_network_summary.id)

        new_s = new_net.scenarios.Scenario[0]
        new_rss = new_s.resourcescenarios.ResourceScenario
        for new_rs in new_rss:
            if new_rs.value.type == 'timeseries':
                ret_ts_dict = {}
                ret_ts_dict = json.loads(new_rs.value.value).values()[0]
                client_ts   = json.loads(arbitrary_ts)['0']
                for new_timestep in client_ts.keys():
                    assert ret_ts_dict[new_timestep] == client_ts[new_timestep]

    def test_get_relative_data_between_times(self):
        net = self.test_relative_timeseries()
        scenario = net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'timeseries':
                val_to_query = d.value
                break

        now = datetime.datetime.now()

        x = self.client.service.get_vals_between_times(
            val_to_query.id,
            0,
            5,
            None,
            0.5,
            )
        assert len(x.data) > 0

        invalid_qry = self.client.service.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            )

        assert eval(invalid_qry.data) == [] 

    def test_seasonal_timeseries(self):
        net = self.build_network()

        relative_ts = self.create_seasonal_timeseries()

        s = net['scenarios'].Scenario[0]
        for rs in s['resourcescenarios'].ResourceScenario:
            if rs['value']['type'] == 'timeseries':
                rs['value']['value'] = relative_ts
        
        new_network_summary = self.client.service.add_network(net)
        new_net = self.client.service.get_network(new_network_summary.id)
        
        scenario = new_net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'timeseries':
                val_to_query = d.value
                break

        val_a = json.loads(val_to_query.value).values()[0]

        jan_val = self.client.service.get_val_at_time(
            val_to_query.id,
            datetime.datetime(2000, 01, 10, 00, 00, 00)
           )
        feb_val = self.client.service.get_val_at_time(
            val_to_query.id,
            datetime.datetime(2000, 02, 10, 00, 00, 00)
           )
        mar_val = self.client.service.get_val_at_time(
            val_to_query.id,
            datetime.datetime(2000, 03, 10, 00, 00, 00)
           )
        oct_val = self.client.service.get_val_at_time(
            val_to_query.id,
            datetime.datetime(2000, 10, 10, 00, 00, 00)
           )
        
        local_val = json.loads(val_to_query.value).values()[0]
        assert json.loads(jan_val.data) == local_val['9999-01-01']
        assert json.loads(feb_val.data) == local_val['9999-02-01']
        assert json.loads(mar_val.data) == local_val['9999-03-01']
        assert json.loads(oct_val.data) == local_val['9999-03-01']
        
        start_time = datetime.datetime(2000, 07, 10, 00, 00, 00)
        vals = self.client.service.get_vals_between_times(
            val_to_query.id,
            start_time,
            start_time + datetime.timedelta(minutes=75),
            'minutes',
            1,
            )

        data = json.loads(vals.data)
        original_val = local_val['9999-03-01']
        assert len(data) == 76
        for val in data:
            assert original_val == val



    def test_multiple_vals_at_time(self):
        net = self.build_network()

        relative_ts = self.create_seasonal_timeseries()

        s = net['scenarios'].Scenario[0]
        for rs in s['resourcescenarios'].ResourceScenario:
            if rs['value']['type'] == 'timeseries':
                rs['value']['value'] = relative_ts
        
        new_network_summary = self.client.service.add_network(net)
        new_net = self.client.service.get_network(new_network_summary.id)
        
        scenario = new_net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'timeseries':
                val_to_query = d.value
                break

        val_a = json.loads(val_to_query.value)

        qry_times =             [
            datetime.datetime(2000, 01, 10, 00, 00, 00),
            datetime.datetime(2000, 02, 10, 00, 00, 00),
            datetime.datetime(2000, 03, 10, 00, 00, 00),
            datetime.datetime(2000, 10, 10, 00, 00, 00),
            ]


        seasonal_vals = self.client.service.get_multiple_vals_at_time(
            val_to_query.id,
            qry_times,
           )

        return_val = json.loads(seasonal_vals['dataset_%s'%val_to_query.id])

        dataset_vals = val_a['0.0']

        assert return_val[str(qry_times[0])] == dataset_vals['9999-01-01']
        assert return_val[str(qry_times[1])] == dataset_vals['9999-02-01']
        assert return_val[str(qry_times[2])] == dataset_vals['9999-03-01']
        assert return_val[str(qry_times[3])] == dataset_vals['9999-03-01']
        
        start_time = datetime.datetime(2000, 07, 10, 00, 00, 00)
        vals = self.client.service.get_vals_between_times(
            val_to_query.id,
            start_time,
            start_time + datetime.timedelta(minutes=75),
            'minutes',
            1,
            )

        data = json.loads(vals.data)
        original_val = dataset_vals['9999-03-01']
        assert len(data) == 76
        for val in data:
            assert original_val == val

    def test_get_data_between_times(self):
        net = self.create_network_with_data()
        scenario = net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'timeseries':
                val_to_query = d.value
                break

        val_a = json.loads(val_to_query.value)['index'].values()[0]
        val_b = json.loads(val_to_query.value)['index'].values()[1]

        now = datetime.datetime.now()

        vals = self.client.service.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            1,
            )

        data = json.loads(vals.data)
        assert len(data) == 76
        for val in data[60:75]:
            x = val_b
            assert x == val_b
        for val in data[0:59]:
            x = val_a
            assert x == val_a

    def test_descriptor_get_data_between_times(self):
        net = self.create_network_with_data()
        scenario = net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'descriptor':
                val_to_query = d.value
                break

        now = datetime.datetime.now()

        value = self.client.service.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            )

        assert json.loads(value.data) == ['test']

    def create_seasonal_timeseries(self):
        """
            Create a timeseries which has relative timesteps:
            1, 2, 3 as opposed to timestamps
        """
        t1 ='9999-01-01' 
        t2 ='9999-02-01' 
        t3 ='9999-03-01' 
        val_1 = [[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]], [[9,8,7],[6,5,4]]] 

        val_2 = ["1.0", "2.0", "3.0"]
        val_3 = ["3.0", "", ""]

        timeseries = json.dumps({0:{t1:val_1, t2:val_2, t3:val_3}})

        return timeseries 

    def create_relative_timeseries(self):
        """
            Create a timeseries which has relative timesteps:
            1, 2, 3 as opposed to timestamps
        """
        """
            Create a timeseries which has relative timesteps:
            1, 2, 3 as opposed to timestamps
        """
        t1 = "1.0"
        t2 = "2.0"
        t3 = "3.0"
        val_1 = [[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]], [[9,8,7],[6,5,4]]]
        val_2 = ["1.0", "2.0", "3.0"]
        val_3 = ["3.0", "", ""]

        timeseries = json.dumps({0:{t1:val_1, t2:val_2, t3:val_3}})

        return timeseries 

    def create_arbitrary_timeseries(self):
        """
            Create a timeseries which has relative timesteps:
            1, 2, 3 as opposed to timestamps
        """
        t1 = 'arb'
        t2 = 'it'
        t3 = 'rary'
        val_1 = [[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]], [[9,8,7],[6,5,4]]]
        val_2 = ["1.0", "2.0", "3.0"]
        val_3 = ["3.0", "", ""]

        timeseries = json.dumps({0:{t1:val_1, t2:val_2, t3:val_3}})

        return timeseries 

#Commented out because an imbalanced array is now allowed. We may add checks
#for this at a later date if needed, but for now we are going to leave such
#validation up to the client.
#class ArrayTest(server.SoapServerTest):
#    def test_array_format(self):
#        bad_net = self.build_network()
#
#        s = bad_net['scenarios'].Scenario[0]
#        for rs in s['resourcescenarios'].ResourceScenario:
#            if rs['value']['type'] == 'array':
#                rs['value']['value'] = json.dumps([[1, 2] ,[3, 4, 5]])
#        
#        self.assertRaises(WebFault, self.client.service.add_network,bad_net)
#        
#        net = self.build_network()
#        n = self.client.service.add_network(net)
#        good_net = self.client.service.get_network(n.id)
#        
#        s = good_net.scenarios.Scenario[0]
#        for rs in s.resourcescenarios.ResourceScenario:
#            if rs.value.type == 'array':
#                rs.value.value = json.dumps([[1, 2] ,[3, 4, 5]])
#                #Get one of the datasets, make it uneven and update it.
#                self.assertRaises(WebFault, self.client.service.update_dataset,rs)

class DataCollectionTest(server.SoapServerTest):

    def test_get_collections_like_name(self):
        collections = self.client.service.get_collections_like_name('test')
 
        assert len(collections) > 0; "collections were not retrieved correctly!"
   
    def test_get_collection_datasets(self):
        collections = self.client.service.get_collections_like_name('test')
        
        datasets = self.client.service.get_collection_datasets(collections.DatasetCollection[-1].id)
 
        assert len(datasets) > 0, "Datasets were not retrieved correctly!"

    def test_add_collection(self):
        
        network = self.create_network_with_data(ret_full_net = False)

        scenario_id = network.scenarios.Scenario[0].id
        
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        collection = self.client.factory.create('ns1:DatasetCollection')

        grp_dataset_ids = self.client.factory.create("integerArray")
        dataset_id = scenario_data.Dataset[0].id
        grp_dataset_ids.integer.append(dataset_id)
        for d in scenario_data.Dataset:
            if d.type == 'timeseries' and d.id != dataset_id:
                grp_dataset_ids.integer.append(d.id)
                break

        collection.dataset_ids = grp_dataset_ids 
        collection.name  = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = self.client.service.add_dataset_collection(collection)

        assert newly_added_collection.id is not None, "Dataset collection does not have an ID!"
        assert len(newly_added_collection.dataset_ids.integer) == 2, "Dataset collection does not have any items!"  

    def test_get_all_collections(self):
        
        network = self.create_network_with_data(ret_full_net = False)

        scenario_id = network.scenarios.Scenario[0].id
        
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        collection = self.client.factory.create('ns1:DatasetCollection')

        grp_dataset_ids = self.client.factory.create("integerArray")
        dataset_id = scenario_data.Dataset[0].id
        grp_dataset_ids.integer.append(dataset_id)
        for d in scenario_data.Dataset:
            if d.type == 'timeseries' and d.id != dataset_id:
                grp_dataset_ids.integer.append(d.id)
                break

        collection.dataset_ids = grp_dataset_ids 
        collection.name  = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = self.client.service.add_dataset_collection(collection)
        collections = self.client.service.get_all_dataset_collections(collection)
        assert newly_added_collection.id in [g.id for g in collections.DatasetCollection]

    def test_add_dataset_to_collection(self):
        
        network = self.create_network_with_data(ret_full_net = False)

        scenario_id = network.scenarios.Scenario[0].id
        
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        collection = self.client.factory.create('ns1:DatasetCollection')

        grp_dataset_ids = self.client.factory.create("integerArray")
        dataset_id = scenario_data.Dataset[0].id
        grp_dataset_ids.integer.append(dataset_id)
        for d in scenario_data.Dataset:
            if d.type == 'timeseries' and d.id != dataset_id:
                grp_dataset_ids.integer.append(d.id)
                break
        
        dataset_id_to_add = None
        for d in scenario_data.Dataset:
            if d.type == 'array' and d.id != dataset_id:
                dataset_id_to_add = d.id
                break

        collection.dataset_ids = grp_dataset_ids 
        collection.name  = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = self.client.service.add_dataset_collection(collection)
        
        previous_dataset_ids = []
        for d_id in newly_added_collection.dataset_ids.integer:
            previous_dataset_ids.append(d_id)
        
        #This acts as a test for the 'check_dataset_in_collection' code
        assert self.client.service.check_dataset_in_collection(dataset_id_to_add, newly_added_collection.id) == 'N'
        assert self.client.service.check_dataset_in_collection(99999, newly_added_collection.id) == 'N'
        self.assertRaises(WebFault, self.client.service.check_dataset_in_collection, 99999, 99999)

        self.client.service.add_dataset_to_collection(dataset_id_to_add, newly_added_collection.id)
        
        assert self.client.service.check_dataset_in_collection(dataset_id_to_add, newly_added_collection.id) == 'Y'
        
        updated_collection = self.client.service.get_dataset_collection(newly_added_collection.id)
        
        new_dataset_ids = []
        for d_id in updated_collection.dataset_ids.integer:
            new_dataset_ids.append(d_id)
        
        assert set(new_dataset_ids) - set(previous_dataset_ids) == set([dataset_id_to_add])
        


    def test_add_datasets_to_collection(self):
        
        network = self.create_network_with_data(ret_full_net = False)

        scenario_id = network.scenarios.Scenario[0].id
        
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        collection = self.client.factory.create('ns1:DatasetCollection')

        grp_dataset_ids = self.client.factory.create("integerArray")
        dataset_id = scenario_data.Dataset[0].id
        grp_dataset_ids.integer.append(dataset_id)
        for d in scenario_data.Dataset:
            if d.type == 'timeseries' and d.id != dataset_id:
                grp_dataset_ids.integer.append(d.id)
                break
        
        dataset_ids_to_add = self.client.factory.create("intArray")
        for d in scenario_data.Dataset:
            if d.type == 'array' and d.id != dataset_id:
                dataset_ids_to_add.int.append(d.id)

        collection.dataset_ids = grp_dataset_ids 
        collection.name  = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = self.client.service.add_dataset_collection(collection)
        
        previous_dataset_ids = []
        for d_id in newly_added_collection.dataset_ids.integer:
            previous_dataset_ids.append(d_id)
        
        self.client.service.add_datasets_to_collection(dataset_ids_to_add, newly_added_collection.id)
        
        updated_collection = self.client.service.get_dataset_collection(newly_added_collection.id)
        
        new_dataset_ids = []
        for d_id in updated_collection.dataset_ids.integer:
            new_dataset_ids.append(d_id)
        
        assert set(new_dataset_ids) - set(previous_dataset_ids) == set(dataset_ids_to_add.int)
        

    def test_remove_dataset_from_collection(self):
        
        network = self.create_network_with_data(ret_full_net = False)

        scenario_id = network.scenarios.Scenario[0].id
        
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        collection = self.client.factory.create('ns1:DatasetCollection')

        grp_dataset_ids = self.client.factory.create("integerArray")
        dataset_id = scenario_data.Dataset[0].id
        grp_dataset_ids.integer.append(dataset_id)
        for d in scenario_data.Dataset:
            if d.type == 'timeseries' and d.id != dataset_id:
                grp_dataset_ids.integer.append(d.id)
                break
        
        collection.dataset_ids = grp_dataset_ids 
        collection.name  = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = self.client.service.add_dataset_collection(collection)
        
        previous_dataset_ids = []
        for d_id in newly_added_collection.dataset_ids.integer:
            previous_dataset_ids.append(d_id)
        
        self.client.service.remove_dataset_from_collection(dataset_id, newly_added_collection.id)
        
        updated_collection = self.client.service.get_dataset_collection(newly_added_collection.id)
        
        new_dataset_ids = []
        for d_id in updated_collection.dataset_ids.integer:
            new_dataset_ids.append(d_id)
        
        assert set(previous_dataset_ids) - set(new_dataset_ids) == set([dataset_id])
        
class SharingTest(server.SoapServerTest):

    def _get_project(self):
        p = self.client.service.get_project_by_name("Unittest Project")
        return p

    def test_hide_data(self):
        """
            Test for the hiding of data.
            Create a network with some data.
            Hide the timeseries created, check if another user can see it.
            Share the time series with one users. Check if they can see it but a third user can't.
        """

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client

        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB", "UserC"], 'Y')
        
        scenario = network_1.scenarios.Scenario[0]
        
        data = [x.value for x in scenario.resourcescenarios.ResourceScenario]

        data_to_hide = data[-1].id

        self.client.service.hide_dataset(data_to_hide, ["UserB"], 'Y', 'Y', 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        netA = self.client.service.get_network(network_1.id)
        scenario = netA.scenarios.Scenario[0]
        
        data = [x.value for x in scenario.resourcescenarios.ResourceScenario]

        for d in data:
            if d.id == data_to_hide:
                assert d.hidden == 'Y'
                assert d.value is not None
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.hidden == 'N'
                assert d.value is not None

        #Check user B can see the dataset
        self.client.service.logout("UserB")

        self.login("UserC", 'password')
        #Check user C cannot see the dataset
        netB = self.client.service.get_network(network_1.id)
        
        scenario = netB.scenarios.Scenario[0]
        
        data = [x.value for x in scenario.resourcescenarios.ResourceScenario]
        
        for d in data:
            if d.id == data_to_hide:
                assert d.hidden == 'Y'
                assert d.value is None
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.hidden == 'N'
                assert d.value is not None

        self.client.service.logout("UserC")

        self.client = old_client

    def test_replace_hidden_data(self):
        """
            test_replace_hidden_data
            Test for the case where one user hides data and another
            user sets the data to something else.
            
            User A Creates a network with some data
            User A Hides the timeseries created.
            User A shares network with User B
            
            Check user B cannot see timeseries value
            User B creates a new timeseries, and replaces the hidden one.
            Save network.
            Attribute now should have a new, unhidden dataset assigned to that attribute.
        """

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client

        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()
        
        network_1 = self.client.service.get_network(network_1.id)
        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB", "UserC"], 'N')

        scenario = network_1.scenarios.Scenario[0]

        data = [x for x in scenario.resourcescenarios.ResourceScenario]

        for d in data:
            if d.value.type == 'timeseries':
                attr_to_be_changed = d.resource_attr_id
                data_to_hide = d.value.id

        self.client.service.hide_dataset(data_to_hide, [], 'Y', 'Y', 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        netA = self.client.service.get_network(network_1.id)
        scenario = netA.scenarios.Scenario[0]
        
        #Find the hidden piece of data and replace it with another
        #to simulate a case of two people working on one attribute
        #where one cannot see the value of it.
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                #THis piece of data is indeed the hidden one.
                assert d.value.hidden == 'Y'
                #set the value of the attribute to be a different
                #timeseries.
                dataset = self.client.factory.create('hyd:Dataset')

                dataset.type = 'timeseries'
                dataset.name = 'replacement time series'
                dataset.unit = 'feet cubed'
                dataset.dimension = 'cubic capacity'

                t1 = datetime.datetime.now()
                t2 = t1+datetime.timedelta(hours=1)

                ts_val = {0: {t1: [11, 21, 31, 41, 51],
                            t2: [12, 22, 32, 42, 52]}}
                dataset.value = ts_val
                d.value = dataset
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.value.hidden == 'N'
                assert d.value.value is not None

        updated_net = self.client.service.update_network(netA)
        updated_net = self.client.service.get_network(netA.id)
        scenario = updated_net.scenarios.Scenario[0]
        #After updating the network, check that the new dataset
        #has been applied
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                assert d.value.hidden == 'N'
                assert d.value.id     != data_to_hide
        #Now validate that the dataset was not overwritten, but replaced
        #by getting the old dataset and ensuring user B can still not see it.
        hidden_dataset = self.client.service.get_dataset(data_to_hide)
        assert hidden_dataset.hidden == 'Y'
        assert hidden_dataset.value  == None

        self.client.service.logout("UserB")

        self.client = old_client

    def test_edit_hidden_data(self):
        """
            test_edit_hidden_data
            Test for the case where one user hides data and another
            user sets the data to something else.
            
            User A Creates a network with some data
            User A Hides the timeseries created.
            User A shares network with User B
            
            Check user B cannot see timeseries value
            User B sets value of timeseries to something else.
            Save network.
            Attribute now should have a new, unhidden dataset assigned to that attribute.
        """

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client

        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB"], 'N')

        scenario = network_1.scenarios.Scenario[0]

        data = [x for x in scenario.resourcescenarios.ResourceScenario]

        for d in data:
            if d.value.type == 'timeseries':
                attr_to_be_changed = d.resource_attr_id
                data_to_hide = d.value.id
                break

        self.client.service.hide_dataset(data_to_hide, [], 'Y', 'Y', 'Y')
        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        netA = self.client.service.get_network(network_1.id)
        scenario = netA.scenarios.Scenario[0]
        
        #Find the hidden piece of data and replace it with another
        #to simulate a case of two people working on one attribute
        #where one cannot see the value of it.
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                #THis piece of data is indeed the hidden one.
                assert d.value.hidden == 'Y'
                t1 = datetime.datetime.now()
                t2 = t1+datetime.timedelta(hours=1)

                ts_val = {0: {t1: [11, 21, 31, 41, 51],
                            t2: [12, 22, 32, 42, 52]}}
                #Reassign the value of the dataset to something new.
                d.value.value = json.dumps(ts_val)
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.value.hidden == 'N'
                assert d.value.value is not None

        updated_net = self.client.service.update_network(netA)
        updated_net = self.client.service.get_network(updated_net.id)
        scenario = updated_net.scenarios.Scenario[0]
        #After updating the network, check that the new dataset
        #has been applied
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                assert d.value.hidden == 'N'
                assert d.value.id     != data_to_hide
        #Now validate that the dataset was not overwritten, but replaced
        #by getting the old dataset and ensuring user B can still not see it.
        hidden_dataset = self.client.service.get_dataset(data_to_hide)
        assert hidden_dataset.hidden == 'Y'
        assert hidden_dataset.value  == None

        self.client.service.logout("UserB")

        self.client = old_client

    def test_get_extents(self):
        """
        Extents test: Test that the min X, max X, min Y and max Y of a
        network are retrieved correctly.
        """
        net = self.create_network_with_data()

        extents = self.client.service.get_network_extents(net.id)

        assert extents.min_x == 10
        assert extents.max_x == 100
        assert extents.min_y == 9
        assert extents.max_y == 99

class RetrievalTest(server.SoapServerTest):

    def _make_timeseries(self):
        t1 = datetime.datetime.now()
        t2 = t1+datetime.timedelta(hours=1)
        
        t1 = t1.strftime(self.fmt)
        t2 = t2.strftime(self.fmt)

        dataset = self.client.factory.create('hyd:Dataset')

        dataset.type = 'timeseries'
        dataset.name = 'time series to retrieve'
        dataset.unit = 'feet cubed'
        dataset.dimension = 'cubic capacity'
        ts_val = {0: {t1: [11, 21, 31, 41, 51],
              t2: [12, 22, 32, 42, 52]}}

        dataset.value = json.dumps(ts_val)
        new_d = self.client.service.add_dataset(dataset)
        return new_d

    def test_get_dataset(self):
        """
        Test to get a single dataset by ID.
        Should return the dataset if found and throw a not found error if not.
        """
        dataset_1 = self._make_timeseries()

        retrieved_d = self.client.service.get_dataset(dataset_1['id'])

        assert str(dataset_1.value) == str(retrieved_d.value)

        self.assertRaises(WebFault, self.client.service.get_dataset, int(dataset_1['id'])+1)

    def test_get_datasets(self):
        """
            Test to get a list of datasets by ID.
            Should return a list of datasets if found and return an empty list if not.
        """
        dataset_1 = self._make_timeseries()
        dataset_2 = self._make_timeseries()

        dataset_ids = self.client.factory.create('intArray')
        dataset_ids.int.append(dataset_1.id)
        dataset_ids.int.append(dataset_2.id)

        retrieved_ds = self.client.service.get_datasets(dataset_ids)
        assert retrieved_ds is not None
        
        assert str(dataset_1.value) == str(retrieved_ds.Dataset[0].value)
        assert str(dataset_2.value) == str(retrieved_ds.Dataset[1].value)


    def test_get_node_data(self):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = self.create_network_with_data()
        scenario_id = net.scenarios.Scenario[0].id       
        node_id     = net.nodes.Node[0].id
        node_data = self.client.service.get_node_data(node_id, scenario_id)
        assert len(node_data) > 0
        node_id     = net.nodes.Node[1].id
        node_data = self.client.service.get_node_data(node_id, scenario_id)
        node_data2 = self.client.service.get_node_data(node_id, scenario_id, net.nodes.Node[0].types.TypeSummary[0].id)
        assert len(node_data) > 0
        assert len(node_data) == len(node_data2)

        link_id     = net.links.Link[0].id
        link_data = self.client.service.get_link_data(link_id, scenario_id)
        assert len(link_data) > 0
        link_id     = net.links.Link[1].id
        link_data = self.client.service.get_link_data(link_id, scenario_id)
        assert len(link_data) > 0

    def test_get_node_attribute_data(self):
        net = self.create_network_with_data()
        nodes = net.nodes.Node
        nodearray = self.client.factory.create("integerArray")
        nodearray.integer = [n.id for n in nodes]
        attrarray = self.client.factory.create("integerArray")
        attrarray.integer = [nodes[0].attributes.ResourceAttr[0].attr_id]

        attr_data = self.client.service.get_node_attribute_data(nodearray, attrarray)
        #Check something has been returned 
        assert attr_data.resourceattrs is not None
        assert attr_data.resourcescenarios is not None

        res_attrs = attr_data.resourceattrs.ResourceAttr
        res_scenarios = attr_data.resourcescenarios.ResourceScenario
        #Check the correct number of things have been returned
        #10 nodes, 1 attr per node = 10 resourceattrs
        #10 resourceattrs, 1 scenario = 10 resource scenarios
        assert len(res_attrs) == 10
        assert len(res_scenarios) == 10

        ra_ids = [r.id for r in res_attrs]
        for rs in res_scenarios:
            assert rs.resource_attr_id in ra_ids

    def _create_scalar(self):

        scalar = dict(
            id=None,
            type = 'scalar',
            name = 'Flow speed',
            unit = 'm s^-1',
            dimension = 'Velocity',
            hidden = 'N',
            value = 0.002,
            metadata = json.dumps({}),
        )

        return scalar

    def _create_descriptor(self):
        descriptor = dict(
            id        = None,
            type      = 'descriptor',
            name      = 'description of water level',
            unit      = None,
            dimension = None,
            hidden    = 'N',
            value     = 'high',
            metadata = json.dumps({}),
        )
        
        return descriptor

    def _create_array(self):
        arr = json.dumps([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
       
        metadata = {'created_by':'Test user'}

        dataset = dict(
            id=None,
            type = 'array',
            name = 'my array',
            unit = 'bar',
            dimension = 'Pressure',
            hidden = 'N',
            value = arr,
            metadata = json.dumps(metadata), 
        )

        return dataset

    def _create_timeseries(self):

        t1 = datetime.datetime.now()
        t2 = t1+datetime.timedelta(hours=1)
        t3 = t1+datetime.timedelta(hours=2)

        t1 = t1.strftime(self.fmt)
        t2 = t2.strftime(self.fmt)
        t3 = t3.strftime(self.fmt)
        
        val_1 = 1.234
        val_2 = 2.345
        val_3 = 3.456

        ts_val = json.dumps({0: {t1: val_1,
                      t2: val_2,
                      t3: val_3}})

        metadata_array = json.dumps({'created_by':'Test user',
                          'is used for':'data search'})

        dataset = dict(
            id=None,
            type = 'timeseries',
            name = 'my time series',
            unit = 'cm^3',
            dimension = 'Volume',
            hidden = 'N',
            value = ts_val,
            metadata = metadata_array, 
        )

        return dataset 

    def test_data_search(self):
        """
            Test for the 'search_datasets' function.

            This function should retrieve a list of datasets given a set of 
            filters, including:
                ID,
                Name,
                Collection name,
                Type,
                Dimension,
                Unit
                Scenario,
                Metadata
        """

       
        datasets = self.client.factory.create('ns1:DatasetArray')
        #create some datasets
        #Scalar, descriptor, array, 2 * timeseries
        array = self._create_array() 
        datasets.Dataset.append(array)
        scalar = self._create_scalar()
        datasets.Dataset.append(scalar)
        descriptor = self._create_descriptor() 
        datasets.Dataset.append(descriptor)
        ts_1 = self._create_timeseries() 
        datasets.Dataset.append(ts_1)
        ts_2 = self._create_timeseries()
        datasets.Dataset.append(ts_2)

        dataset_ids = self.client.service.bulk_insert_data(datasets)
        array['id'] = dataset_ids.integer[0]
        scalar['id'] = dataset_ids.integer[1]
        descriptor['id'] = dataset_ids.integer[2]
        ts_1['id'] = dataset_ids.integer[3]
        ts_2['id'] = dataset_ids.integer[4]

        #create a dataset collection and put one timeseries into it.

        grp_dataset_ids = self.client.factory.create("integerArray")
        grp_dataset_ids.integer.append(ts_1['id'])
        grp_dataset_ids.integer.append(ts_2['id'])

        collection = dict(
            dataset_ids = grp_dataset_ids,
            name  = 'timeseries collection %s'%(datetime.datetime.now())
        )

        timeseries_collection = self.client.service.add_dataset_collection(collection)
        collection_name = timeseries_collection.name

        #search for datset with ID

        res_1 = self.client.service.search_datasets(array['id'])
        assert len(res_1.Dataset) == 1
        assert res_1.Dataset[0].id == array['id']
        assert res_1.Dataset[0].name == array['name']
        
        #search for dataset by name
        res_1 = self.client.service.search_datasets(name=array['name'])
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.name == array['name']
        assert array['id'] in ids

        #search for scalars
        res_1 = self.client.service.search_datasets(data_type='scalar')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.type == 'scalar'
        assert scalar['id'] in ids

        #search for descriptors
        res_1 = self.client.service.search_datasets(data_type='descriptor')
        assert len(res_1.Dataset) >= 1
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.type == 'descriptor'
        assert descriptor['id'] in ids

        #search for arrays
        res_1 = self.client.service.search_datasets(data_type='array')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.type == 'array'
        assert array['id'] in ids

        #search for timeseries
        res_1 = self.client.service.search_datasets(data_type='timeseries', metadata_val='search')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.type == 'timeseries'
        assert ts_1['id'] in ids
        assert ts_2['id'] in ids
        #search by non-existant type
        res_1 = self.client.service.search_datasets(data_type='notadatatype')
        assert res_1 == ''

        #We have at least 2 timeseries, so let's page to 1
        res_1 = self.client.service.search_datasets(data_type='timeseries', metadata_val='search', page_size=1, inc_val='Y')
        assert len(res_1.Dataset) == 1
        val = json.loads(res_1.Dataset[0].value).values()[0]
        assert 1.234 in val.values()
        assert 2.345 in val.values()
        assert 3.456 in val.values()

        res_1 = self.client.service.search_datasets(data_type='timeseries', metadata_val='search', page_start=1000)
        assert res_1 == ''

        #search by non-existant type
        res_1 = self.client.service.search_datasets(data_type='notadatatype')
        assert res_1 == ''

        #search by dimension x 2 (non-exisent dimension, exisiting dimension)

        res_1 = self.client.service.search_datasets(dimension='Volume')
        for d in res_1.Dataset:
            assert d.dimension == 'Volume'

        res_1 = self.client.service.search_datasets(dimension='iamnotadimension')
        assert res_1 == ''

        #search by unit x2 (non-exisent unit, exisiting unit)
        res_1 = self.client.service.search_datasets(unit='m s^-1')
        for d in res_1.Dataset:
            assert d.unit == 'm s^-1'

        res_1 = self.client.service.search_datasets(unit='iamnotaunit')
        assert res_1 == ''

        #search by scenario
        net = self.create_network_with_data(num_nodes=5)
        scenario = net.scenarios.Scenario[0]
        res_1 = self.client.service.search_datasets(scenario_id=scenario.id)
        assert len(res_1.Dataset) == len(scenario.resourcescenarios.ResourceScenario)

        #search by metadata
        res_1 = self.client.service.search_datasets(metadata_name='created_by', inc_metadata='Y')
        assert res_1 != ''
        for d in res_1.Dataset:
            assert 'created_by' in json.loads(d.metadata) 

        #search by metadata
        res_1 = self.client.service.search_datasets(metadata_name='used for', metadata_val='earch', inc_metadata='Y')
        assert res_1 != ''
        for d in res_1.Dataset:
            assert 'is used for' in json.loads(d.metadata) 

        res_1 = self.client.service.search_datasets(metadata_val='search', inc_metadata='Y')
        assert res_1 != ''
        for d in res_1.Dataset:
            assert 'is used for' in json.loads(d.metadata)

        res_1 = self.client.service.search_datasets(metadata_name='non-existent', inc_metadata='Y')
        assert res_1 == ''

        #search by collection name (return only one timeseries)
        res_1 = self.client.service.search_datasets(collection_name=collection_name)
        assert len(res_1.Dataset) == 2
        ts_ids = [ts_1['id'], ts_2['id']]
        for d in res_1.Dataset:
            assert d.id in ts_ids

        #combinations:
        #search by type, dimension
        res_1 = self.client.service.search_datasets(data_type='scalar', dimension='speed')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.type == 'scalar' and d.dimension == 'Speed'
        #matching unit, dimension
        res_1 = self.client.service.search_datasets(unit='m s^-1', dimension='speed')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.unit == 'm s^-1' and d.dimension == 'Speed'

        #mismatching unit, dimension
        res_1 = self.client.service.search_datasets(unit='cm^3', dimension='speed')
        assert res_1 == '' 

        #partial name, dimension
        res_1 = self.client.service.search_datasets(name='flow sp', dimension='speed')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.name == scalar['name'] and d.dimension == 'Speed'

        #name, unit
        res_1 = self.client.service.search_datasets(name='array', unit='bar')
        assert len(res_1.Dataset) >= 1
        ids = []
        for d in res_1.Dataset:
            ids.append(d.id)
            assert d.name == array['name'] and d.unit == 'bar'

        #collection name, name
        res_1 = self.client.service.search_datasets(name='array',collection_name=collection_name)
        assert res_1 == ''
        res_1 = self.client.service.search_datasets(name='time',collection_name=collection_name)
        assert len(res_1.Dataset) == 2
        ts_ids = [ts_1['id'], ts_2['id']]
        for d in res_1.Dataset:
            assert d.id in ts_ids

        res_1 = self.client.service.search_datasets(unconnected='Y')
        assert len(res_1.Dataset) >= 1
        ds_ids = [d.id for d in res_1.Dataset]
        assert ts_1['id'] in ds_ids
        assert ts_2['id'] in ds_ids
        
        attr_id = net.nodes.Node[0].attributes.ResourceAttr[0].attr_id
        attr_dataset_ids = []
        for rs in net.scenarios.Scenario[0].resourcescenarios.ResourceScenario:
            if rs.attr_id == attr_id:
                attr_dataset_ids.append(rs.value.id)
        res_1 = self.client.service.search_datasets(attr_id=attr_id)
        res_dataset_ids = [d.id for d in res_1.Dataset]
        for res_id in attr_dataset_ids:
            assert res_id in res_dataset_ids 

        link = net.links.Link[1]
        type_id = link.types.TypeSummary[0].id
        ra_ids = [ra.id for ra in link.attributes.ResourceAttr]

        link_type_dataset_ids = []
        for rs in net.scenarios.Scenario[0].resourcescenarios.ResourceScenario:
            if rs.resource_attr_id in ra_ids:
                link_type_dataset_ids.append(rs.value.id)
        res_1 = self.client.service.search_datasets(type_id=type_id)
        assert res_1 != ''
        res_dataset_ids = [d.id for d in res_1.Dataset]
        for res_id in link_type_dataset_ids:
            assert res_id in res_dataset_ids 

if __name__ == '__main__':
    server.run()
