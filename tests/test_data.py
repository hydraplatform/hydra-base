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
import pytest
import datetime
import logging
import json
import hydra_base as hb
from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import ResourceNotFoundError

from hydra_base.util import flatten_dict, count_levels

log = logging.getLogger(__name__)


@pytest.fixture()
def relative_dataframe():
    """
        Create a timeseries which has relative timesteps:
        1, 2, 3 as opposed to timestamps
    """
    t1 = 1
    t2 = 2
    t3 = 3
    val_1 = [[[11, 22, 33], [55, 44, 66]], [[100, 200, 300], [400, 500, 600]], [[99, 88, 77], [66, 55, 44]]]
    val_2 = ["1.1", "2.2", "3.3"]
    val_3 = ["3.3", "", ""]

    timeseries = json.dumps({0: {t1: val_1, t2: val_2, t3: val_3}})

    return timeseries


@pytest.fixture()
def arbitrary_dataframe():
    """
        Create a timeseries which has relative timesteps:
        1, 2, 3 as opposed to timestamps
    """
    t1 = 'arb'
    t2 = 'it'
    t3 = 'rary'
    val_1 = [[[0.1, 0.2, 33], [0.5, 0.4, 0.6]], [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]], [[0.9,0.8,0.7],[0.6,0.5,0.4]]]
    val_2 = ["1.0", "2.0", "3.0"]
    val_3 = ["3.0", "", ""]

    timeseries = json.dumps({0:{t1:val_1, t2:val_2, t3:val_3}})

    return timeseries


@pytest.fixture()
def seasonal_timeseries():
    """
        Create a timeseries which has relative timesteps:
        1, 2, 3 as opposed to timestamps
    """
    t1 ='9999-01-01'
    t2 ='9999-02-01'
    t3 ='9999-03-01'
    val_1 = [[[1, 2, 33], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]], [[9,8,7],[6,5,4]]]

    val_2 = ["1.0", "2.0", "3.0"]
    val_3 = ["3.0", "", ""]

    timeseries = json.dumps({0: {t1: val_1, t2: val_2, t3: val_3}})

    return timeseries


class TestTimeSeries:
    """
        Test for timeseries-based functionality
    """
    #@pytest.mark.xfail(reason='Relative timesteps are being converted to timestamps. ')
    def test_relative_dataframe(self, client, network, relative_dataframe):
        """
            Test for relative timeseries for example, x number of hours from hour 0.
        """

        s = network['scenarios'][0]
        assert len(s['resourcescenarios']) > 0
        for rs in s['resourcescenarios']:
            if rs['dataset']['type'] == 'dataframe':
                rs['dataset']['value'] = relative_dataframe

        new_network_summary = client.add_network(network)
        new_net = client.get_network(new_network_summary.id, include_data=True)

        new_s = new_net['scenarios'][0]
        new_rss = new_s['resourcescenarios']

        assert len(new_rss) > 0
        for new_rs in new_rss:
            if new_rs.dataset.type == 'dataframe':
                ret_ts_dict = list(json.loads(new_rs.dataset.value).values())[0]
                client_ts   = json.loads(relative_dataframe)['0']
                for new_timestep in client_ts.keys():
                    # TODO this bit appears broken. Somewhere in hydra the timesteps
                    # are convert to timestamps.
                    assert ret_ts_dict.get(new_timestep) == client_ts[new_timestep]


    def test_arbitrary_dataframe(self, client, network, arbitrary_dataframe):

        s = network['scenarios'][0]
        for rs in s['resourcescenarios']:
            if rs['dataset']['type'] == 'dataframe':
                rs['dataset']['value'] = arbitrary_dataframe

        new_network_summary = client.add_network(network)
        new_net = client.get_network(new_network_summary.id, include_data=True)

        new_s = new_net.scenarios[0]
        new_rss = new_s.resourcescenarios
        for new_rs in new_rss:
            if new_rs.dataset.type == 'dataframe':
                ret_ts_dict = {}
                ret_ts_dict = list(json.loads(new_rs.dataset.value).values())[0]
                client_ts   = json.loads(arbitrary_dataframe)['0']
                for new_timestep in client_ts.keys():
                    assert ret_ts_dict[new_timestep] == client_ts[new_timestep]

    def test_get_relative_data_between_times(self, client, network, relative_dataframe):

        # TODO The following is shared with `test_relative_dataframe` it could be put in a fixture
        s = network['scenarios'][0]
        assert len(s['resourcescenarios']) > 0
        for rs in s['resourcescenarios']:
            if rs['dataset']['type'] == 'dataframe':
                rs['dataset']['value'] = relative_dataframe

        new_network_summary = client.add_network(network)
        new_net = client.get_network(new_network_summary.id, include_data=True)
        scenario = new_net.scenarios[0]
        val_to_query = None
        for rs in scenario.resourcescenarios:
            if rs.dataset.type == 'dataframe':
                val_to_query = rs.dataset
                break

        now = datetime.datetime.now()
        x = client.get_vals_between_times(
            val_to_query.id,
            0,
            5,
            None,
            0.5,
            )
        assert len(x.data) > 0

        invalid_qry = client.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            5
            )

        assert eval(invalid_qry.data) == []

    def test_seasonal_timeseries(self, client, network, seasonal_timeseries):

        s = network['scenarios'][0]
        for rs in s['resourcescenarios']:
            if rs['dataset']['type'] == 'timeseries':
                rs['dataset']['value'] = seasonal_timeseries

        new_network_summary = client.add_network(network)
        new_net = client.get_network(new_network_summary.id, include_data=True)

        scenario = new_net.scenarios[0]
        val_to_query = None
        for rs in scenario.resourcescenarios:
            if rs.dataset.type == 'timeseries':
                val_to_query = rs.dataset
                break

        val_a = list(json.loads(val_to_query.value).values())[0]

        jan_val = client.get_val_at_time(
            val_to_query.id,
            [datetime.datetime(2000, 1, 10, 00, 00, 00)]
           )
        feb_val = client.get_val_at_time(
            val_to_query.id,
            [datetime.datetime(2000, 2, 10, 00, 00, 00)]
           )
        mar_val = client.get_val_at_time(
            val_to_query.id,
            [datetime.datetime(2000, 3, 10, 00, 00, 00)]
           )
        oct_val = client.get_val_at_time(
            val_to_query.id,
            [datetime.datetime(2000, 10, 10, 00, 00, 00)]
           )

        local_val = list(json.loads(val_to_query.value).values())[0]
        assert json.loads(jan_val.data) == local_val['9999-01-01']
        assert json.loads(feb_val.data) == local_val['9999-02-01']
        assert json.loads(mar_val.data) == local_val['9999-03-01']
        assert json.loads(oct_val.data) == local_val['9999-03-01']

        start_time = datetime.datetime(2000, 7, 10, 00, 00, 00)
        vals = client.get_vals_between_times(
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

    def test_multiple_vals_at_time(self, client, network_with_data, seasonal_timeseries):

        s = network_with_data['scenarios'][0]

        for rs in s['resourcescenarios']:
            if rs['dataset']['type'] == 'timeseries':
                rs['dataset']['value'] = seasonal_timeseries

        client.update_network(network_with_data)
        new_net = client.get_network(network_with_data.id, include_data=True)

        scenario = new_net.scenarios[0]
        val_to_query = None
        for rs in scenario.resourcescenarios:
            if rs.dataset.type == 'timeseries':
                val_to_query = rs.dataset
                break

        val_a = json.loads(val_to_query.value)

        dtformat = hb.config.get('DEFAULT', 'datetime_format', "%Y-%m-%dT%H:%M:%S.%f000Z")
        fmt = datetime.datetime.strftime
        qry_times = [
            fmt(datetime.datetime(2000, 1, 10, 00, 00, 00), dtformat),
            fmt(datetime.datetime(2000, 2, 10, 00, 00, 00), dtformat),
            fmt(datetime.datetime(2000, 3, 10, 00, 00, 00), dtformat),
            fmt(datetime.datetime(2000, 10, 10, 00, 00, 00), dtformat),
            ]

        seasonal_vals = client.get_multiple_vals_at_time(
            [val_to_query.id],
            qry_times,
           )

        #TODO: Figure out why the mysqlclient library with python 2.7.14 was causing this to break
        return_val = seasonal_vals['dataset_%s'%int(val_to_query.id)]

        dataset_vals = val_a['0']

        assert return_val[qry_times[0]] == dataset_vals['9999-01-01']
        assert return_val[qry_times[1]] == dataset_vals['9999-02-01']
        assert return_val[qry_times[2]] == dataset_vals['9999-03-01']
        assert return_val[qry_times[3]] == dataset_vals['9999-03-01']

        start_time = datetime.datetime(2000, 7, 10, 00, 00, 00)
        vals = client.get_vals_between_times(
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

    def test_get_data_between_times(self, client, network_with_data):

        # Convenience renaming
        net = network_with_data

        scenario = net.scenarios[0]
        val_to_query = None
        for rs in scenario.resourcescenarios:
            if rs.dataset.type == 'timeseries':
                val_to_query = rs.dataset
                break

        val_a, val_b = list(json.loads(val_to_query.value)['test_column'].values())[:2]

        now = datetime.datetime.now(datetime.timezone.utc)

        vals = client.get_vals_between_times(
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

    def test_descriptor_get_data_between_times(self, client, network_with_data):
        net = network_with_data
        scenario = net.scenarios[0]
        val_to_query = None
        for rs in scenario.resourcescenarios:
            if rs.dataset.type == 'descriptor':
                val_to_query = rs.dataset
                break

        now = datetime.datetime.now()

        value = client.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            1
            )

        assert json.loads(value.data) == ['test']



#Commented out because an imbalanced array is now allowed. We may add checks
#for this at a later date if needed, but for now we are going to leave such
#validation up to the client.
#class ArrayTest(server.HydraBaseTest):
#    def test_array_format(self):
#        bad_net = self.build_network()
#
#        s = bad_net['scenarios'][0]
#        for rs in s['resourcescenarios']:
#            if rs['value']['type'] == 'array':
#                rs['value']['value'] = json.dumps([[1, 2] ,[3, 4, 5]])
#
#        self.assertRaises(WebFault, client.add_network,bad_net)
#
#        net = self.build_network()
#        n = client.add_network(net)
#        good_net = client.get_network(n.id)
#
#        s = good_net.scenarios[0]
#        for rs in s.resourcescenarios:
#            if rs.value.type == 'array':
#                rs.value.value = json.dumps([[1, 2] ,[3, 4, 5]])
#                #Get one of the datasets, make it uneven and update it.
#                self.assertRaises(WebFault, client.update_dataset,rs)

@pytest.fixture
def collection_json_object():
    collection = JSONObject(dict(
        type="descriptor",
        name="Test collection"
    ))
    return collection


@pytest.fixture()
def network_with_dataset_collection(client, network_with_data, collection_json_object):
    network = network_with_data

    scenario_id = network.scenarios[0].id
    scenario_data = client.get_scenario_data(scenario_id)

    collection = collection_json_object

    group_dataset_ids = [scenario_data[0].id, ]
    for d in scenario_data:
        if d.type == 'timeseries' and d.id not in group_dataset_ids:
            group_dataset_ids.append(d.id)
            break

    collection.dataset_ids = group_dataset_ids
    collection.name = 'test soap collection %s' % (datetime.datetime.now())

    newly_added_collection = client.add_dataset_collection(collection)
    return newly_added_collection


class TestDataCollection:

    def test_get_collections_like_name(self, client, network_with_dataset_collection):
        collections = client.get_collections_like_name('test')
        assert len(collections) > 0, "collections were not retrieved correctly!"

    def test_get_collection_datasets(self, client, network_with_dataset_collection):
        collections = client.get_collections_like_name('test')
        datasets = client.get_collection_datasets(collections[-1]['id'])

        assert len(datasets) > 0, "Datasets were not retrieved correctly!"

    def test_add_collection(self, client, network_with_dataset_collection):
        collection = network_with_dataset_collection

        assert collection.id is not None, "Dataset collection does not have an ID!"
        assert len(collection['items']) == 2, "Dataset collection does not have any items!"

    def test_delete_collection(self, client, network_with_dataset_collection):

        collection = network_with_dataset_collection

        # Get all collections and make sure this collection is present
        all_collections_pre = client.get_all_dataset_collections()

        all_collection_ids_pre = [c.id for c in all_collections_pre]

        assert collection.id in all_collection_ids_pre

        # Delete the collection
        client.delete_dataset_collection(collection.id)

        # Get all the collections again and make sure the deleted collection is not present
        all_collections_post = client.get_all_dataset_collections()
        all_collection_ids_post = [c.id for c in all_collections_post]

        assert collection.id not in all_collection_ids_post

        with pytest.raises(hb.exceptions.HydraError):
            client.get_dataset_collection(collection.id)

    def test_get_all_collections(self, client, network_with_dataset_collection):

        collection = network_with_dataset_collection

        collections = client.get_all_dataset_collections()
        assert collection.id in [dc.id for dc in collections]

    def test_add_dataset_to_collection(self, client, network_with_data, collection_json_object):

        network = network_with_data

        scenario_id = network.scenarios[0].id

        scenario_data = client.get_scenario_data(scenario_id)

        collection = collection_json_object

        group_dataset_ids = [scenario_data[0].id, ]
        for d in scenario_data:
            if d.type == 'timeseries' and d.id not in group_dataset_ids:
                group_dataset_ids.append(d.id)
                break

        dataset_id_to_add = None
        for d in scenario_data:
            if d.type == 'array' and d.id not in group_dataset_ids:
                dataset_id_to_add = d.id
                break

        collection.dataset_ids = group_dataset_ids
        collection.name = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = client.add_dataset_collection(collection)

        previous_dataset_ids = []
        for item in newly_added_collection['items']:
            previous_dataset_ids.append(item.dataset_id)

        # This acts as a test for the 'check_dataset_in_collection' code
        assert client.check_dataset_in_collection(dataset_id_to_add, newly_added_collection.id) == 'N'
        assert client.check_dataset_in_collection(99999, newly_added_collection.id) == 'N'

        with pytest.raises(hb.exceptions.HydraError):
            client.check_dataset_in_collection(99999, 99999)

        client.add_dataset_to_collection(dataset_id_to_add, newly_added_collection.id)

        assert client.check_dataset_in_collection(dataset_id_to_add, newly_added_collection.id) == 'Y'

        updated_collection = client.get_dataset_collection(newly_added_collection.id)

        new_dataset_ids = []
        for item in updated_collection['items']:
            new_dataset_ids.append(item.dataset_id)

        assert set(new_dataset_ids) - set(previous_dataset_ids) == set([dataset_id_to_add])

    def test_add_datasets_to_collection(self, client, network_with_data, collection_json_object):

        network = network_with_data

        scenario_id = network.scenarios[0].id

        scenario_data = client.get_scenario_data(scenario_id)

        collection = collection_json_object

        group_dataset_ids = [scenario_data[0].id, ]
        for d in scenario_data:
            if d.type == 'timeseries' and d.id not in group_dataset_ids:
                group_dataset_ids.append(d.id)
                break

        dataset_ids_to_add = []
        for d in scenario_data:
            if d.type == 'array' and d.id not in group_dataset_ids:
                dataset_ids_to_add.append(d.id)

        collection.dataset_ids = group_dataset_ids
        collection.name = 'test soap collection %s'%(datetime.datetime.now())

        newly_added_collection = client.add_dataset_collection(collection)

        previous_dataset_ids = []
        for item in newly_added_collection['items']:
            previous_dataset_ids.append(item.dataset_id)

        client.add_datasets_to_collection(dataset_ids_to_add, newly_added_collection.id)

        updated_collection = client.get_dataset_collection(newly_added_collection.id)

        new_dataset_ids = []
        for item in updated_collection['items']:
            new_dataset_ids.append(item.dataset_id)

        assert set(new_dataset_ids) - set(previous_dataset_ids) == set(dataset_ids_to_add)

    def test_remove_dataset_from_collection(self, client, network_with_data, collection_json_object):

        network = network_with_data

        scenario_id = network.scenarios[0].id
        scenario_data = client.get_scenario_data(scenario_id)

        collection = collection_json_object
        dataset_id = scenario_data[0].id
        group_dataset_ids = [dataset_id, ]
        for d in scenario_data:
            if d.type == 'timeseries' and d.id not in group_dataset_ids:
                group_dataset_ids.append(d.id)
                break

        collection.dataset_ids = group_dataset_ids
        collection.name = 'test soap collection %s' % (datetime.datetime.now())

        collection = client.add_dataset_collection(collection)

        previous_dataset_ids = []
        for item in collection['items']:
            previous_dataset_ids.append(item.dataset_id)

        client.remove_dataset_from_collection(dataset_id, collection.id)

        updated_collection = client.get_dataset_collection(collection.id)

        new_dataset_ids = []
        for item in updated_collection['items']:
            new_dataset_ids.append(item.dataset_id)

        assert set(previous_dataset_ids) - set(new_dataset_ids) == set([dataset_id])

    def test_delete_dataset_thats_in_a_collection(self, client, network_with_data, collection_json_object):

        network = network_with_data

        scenario_id = network.scenarios[0].id
        scenario_data = client.get_scenario_data(scenario_id)

        collection = collection_json_object
        dataset_id = None
        group_dataset_ids = []
        for d in scenario_data:
            if dataset_id is None and d.type == 'timeseries':
                dataset_id = d.id
                group_dataset_ids.append(d.id)
            elif d.type == 'timeseries' and d.id not in group_dataset_ids:
                group_dataset_ids.append(d.id)

        collection.dataset_ids = group_dataset_ids
        collection.name = 'test soap collection %s' % (datetime.datetime.now())

        newly_added_collection = client.add_dataset_collection(collection)

        # Make dataset_id into an orphaned dataset.
        for s in network.scenarios:
            for rs in s.resourcescenarios:
                if rs.dataset_id == dataset_id:
                    client.delete_resourcedata(scenario_id, rs)

        new_collection = client.get_dataset_collection(newly_added_collection.id)

        new_dataset_ids = []

        for item in new_collection['items']:
            new_dataset_ids.append(item.dataset_id)

        assert dataset_id in new_dataset_ids

        client.delete_dataset(dataset_id)

        updated_collection = client.get_dataset_collection(newly_added_collection.id)
        updated_dataset_ids = []
        for item in updated_collection['items']:
            updated_dataset_ids.append(item.dataset_id)

        assert dataset_id not in updated_dataset_ids

class TestSharing:

    def test_hide_data(self, client, network_with_data):
        """
            Test for the hiding of data.
            Create a network with some data.
            Hide the timeseries created, check if another user can see it.
            Share the time series with one users. Check if they can see it but a third user can't.
        """

        #Let User B view network 1, but not edit it (read_only is 'Y')
        client.share_network(network_with_data.id, ["UserB", "UserC"], 'Y', 'N')

        data_to_hide = network_with_data.scenarios[0].resourcescenarios[-1].dataset.id

        client.hide_dataset(data_to_hide, ["UserB"], 'Y', 'Y', 'Y')

        client.user_id = pytest.user_c.id
        netA = client.get_network(network_with_data.id, include_data=True)

        for rs in netA.scenarios[0].resourcescenarios:
            d = rs.dataset
            if d.id == data_to_hide:
                assert d.hidden == 'Y'
                assert d.value is None
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.hidden == 'N'
                assert d.value is not None

        netB = client.get_network(network_with_data.id, include_data=True)

        for rs in netB.scenarios[0].resourcescenarios:
            d = rs.dataset
            if d.id == data_to_hide:
                assert d.hidden == 'Y'
                assert d.value is None
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.hidden == 'N'
                assert d.value is not None

        directly_retrieved_scenario = client.get_scenario(netB.scenarios[0].id)

        for rs in directly_retrieved_scenario.resourcescenarios:
            d = rs.dataset
            if d.id == data_to_hide:
                assert d.hidden == 'Y'
                assert d.value is None
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.hidden == 'N'
                assert d.value is not None

        scenario_data = client.get_scenario_data(netB.scenarios[0].id)

        for d in scenario_data:
            if d.id == data_to_hide:
                assert d.hidden == 'Y'
                assert d.value is None
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.hidden == 'N'
                assert d.value is not None

        client.user_id = pytest.root_user_id

    def test_replace_hidden_data(self, client, network_with_data):
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

        #Let User B view network 1, but not edit it (read_only is 'Y')
        client.share_network(network_with_data.id, ["UserB", "UserC"], 'N', 'N')

        for d in  network_with_data.scenarios[0].resourcescenarios:
            if d.dataset.type == 'timeseries':
                attr_to_be_changed = d.resource_attr_id
                data_to_hide = d.dataset.id

        client.hide_dataset(data_to_hide, [], 'Y', 'Y', 'Y')
        client.user_id = pytest.user_b.id
        netA = client.get_network(network_with_data.id)

        #Find the hidden piece of data and replace it with another
        #to simulate a case of two people working on one attribute
        #where one cannot see the value of it.
        for d in netA.scenarios[0].resourcescenarios:
            if d.resource_attr_id == attr_to_be_changed:
                #THis piece of data is indeed the hidden one.
                assert d.dataset.hidden == 'Y'
                #set the value of the attribute to be a different
                #timeseries.
                dataset = client.JSONObject(dict(
                    type = 'timeseries',
                    name = 'replacement time series',
                    unit = 'feet cubed',
                ))

                t1 = datetime.datetime.now()
                t2 = t1+datetime.timedelta(hours=1)

                ts_val = {0: {t1: [11, 21, 31, 41, 51],
                            t2: [12, 22, 32, 42, 52]}}
                dataset.value = ts_val
                d.dataset = dataset
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.dataset.hidden == 'N'
                assert d.dataset.value is not None

        updated_net = client.update_network(netA)
        updated_net = client.get_network(netA.id)
        #After updating the network, check that the new dataset
        #has been applied
        for d in updated_net.scenarios[0].resourcescenarios:
            if d.resource_attr_id == attr_to_be_changed:
                assert d.dataset.hidden == 'N'
                assert d.dataset.id     != data_to_hide
        #Now validate that the dataset was not overwritten, but replaced
        #by getting the old dataset and ensuring user B can still not see it.
        hidden_dataset = client.get_dataset(data_to_hide)
        assert hidden_dataset.hidden == 'Y'
        assert hidden_dataset.value  == None

        client.user_id = pytest.root_user_id

    def test_edit_hidden_data(self, client, network_with_data):
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


        #Let User B view network 1, but not edit it (read_only is 'Y')
        client.share_network(network_with_data.id, ["UserB"], 'N', 'N')

        for d in network_with_data.scenarios[0].resourcescenarios:
            if d.dataset.type == 'timeseries':
                attr_to_be_changed = d.resource_attr_id
                data_to_hide = d.dataset.id
                break

        client.hide_dataset(data_to_hide, [], 'Y', 'Y', 'Y')

        #The next actions are done under user b's identity
        client.user_id = pytest.user_b.id
        netA = client.get_network(network_with_data.id)

        #Find the hidden piece of data and replace it with another
        #to simulate a case of two people working on one attribute
        #where one cannot see the value of it.
        for d in netA.scenarios[0].resourcescenarios:
            if d.resource_attr_id == attr_to_be_changed:
                #THis piece of data is indeed the hidden one.
                assert d.dataset.hidden == 'Y'
                t1 = datetime.datetime.now()
                t2 = t1+datetime.timedelta(hours=1)

                ts_val = {0: {t1: [11, 21, 31, 41, 51],
                            t2: [12, 22, 32, 42, 52]}}
                #Reassign the value of the dataset to something new.
                d.dataset.value = json.dumps(ts_val)
            else:
                #The rest of the data is unhidden, so should be there.
                assert d.dataset.hidden == 'N'
                assert d.dataset.value is not None

        updated_net = client.update_network(netA)
        updated_net = client.get_network(updated_net.id)
        #After updating the network, check that the new dataset
        #has been applied
        for d in  updated_net.scenarios[0].resourcescenarios:
            if d.resource_attr_id == attr_to_be_changed:
                assert d.dataset.hidden == 'N'
                assert d.dataset.id     != data_to_hide
        #Now validate that the dataset was not overwritten, but replaced
        #by getting the old dataset and ensuring user B can still not see it.
        hidden_dataset = client.get_dataset(data_to_hide)
        assert hidden_dataset.hidden == 'Y'
        assert hidden_dataset.value  == None

        #set it back to the root user when we're done
        client.user_id = pytest.root_user_id

class TestiUtilities:
    """
        Test hydra's internal utilities relating to data transformations etc.
    """
    @pytest.mark.parametrize("test_input,expected", [
        ({}, 0),
        ({'a':1}, 1),
        ({'a': {'b': 2}}, 2),
        ({'a': {'b':{}}}, 2),
        ({1: {2: 2}}, 2),
    ])
    def test_count_levels(self, client, test_input, expected):
        assert count_levels(test_input) == expected

    @pytest.mark.parametrize("test_input,expected", [
        ({}, {}),
        ({'a':1}, {'a':1}),
        ({'a': {'b': 2}}, {'a_b': 2}),
        ({'a': {'b':{}}}, {'a_b': {}}),
        ({1: {2: 2}}, {'1_2': 2}),
    ])
    def test_flatten_dict(self, client, test_input, expected):
        assert flatten_dict(test_input) == expected
