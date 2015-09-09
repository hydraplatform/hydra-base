
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
import logging
import suds
log = logging.getLogger(__name__)

class GroupTest(server.SoapServerTest):
    """
        Test for network-based functionality
    """

    def test_get_resourcegroup(self):
        network = self.create_network_with_data()
        n = network.resourcegroups.ResourceGroup[0]
        s = network.scenarios.Scenario[0]

        resourcegroup_without_data = self.client.service.get_resourcegroup(n.id)

        for ra in resourcegroup_without_data.attributes.ResourceAttr:
            assert not hasattr(ra, 'resourcescenario')

        resourcegroup_with_data = self.client.service.get_resourcegroup(n.id, s.id)

        attrs_with_data = []
        for ra in resourcegroup_with_data.attributes.ResourceAttr:
            if hasattr(ra, 'resourcescenario'):
                if ra.resourcescenario:
                    attrs_with_data.append(ra.id)
        assert len(attrs_with_data) > 0

        group_items = self.client.service.get_resourcegroupitems(n.id, s.id)
        assert len(group_items) > 0

    def test_add_resourcegroup(self):

        network = self.create_network_with_data()

        group = self.client.factory.create('hyd:ResourceGroup')
        group.network_id=network.id
        group.id = -1
        group.name = 'test new group'
        group.description = 'test new group'

        tmpl = self.create_template()

        type_summary_arr = self.client.factory.create('hyd:TypeSummaryArray')

        type_summary      = self.client.factory.create('hyd:TypeSummary')
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.types.TemplateType[2].id
        type_summary.name = tmpl.types.TemplateType[2].name

        type_summary_arr.TypeSummary.append(type_summary)

        group.types = type_summary_arr

        new_group = self.client.service.add_group(network.id, group)

        group_attr_ids = []
        for resource_attr in new_group.attributes.ResourceAttr:
            group_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.types.TemplateType[2].typeattrs.TypeAttr:
            assert typeattr.attr_id in group_attr_ids

        new_network = self.client.service.get_network(network.id)

        assert len(new_network.resourcegroups.ResourceGroup) == len(network.resourcegroups.ResourceGroup)+1; "new resource group was not added correctly"

        return new_network
    
    def test_add_resourcegroupitem(self):
        

        network = self.test_add_resourcegroup()

        scenario = network.scenarios.Scenario[0]

        group    = network.resourcegroups.ResourceGroup[-1]
        node_id = network.nodes.Node[0].id

        item = self.client.factory.create('hyd:ResourceGroupItem')
        item.ref_key = 'NODE'
        item.ref_id  = node_id
        item.group_id = group.id

        new_item = self.client.service.add_resourcegroupitem(item, scenario.id)

        assert new_item.ref_id == node_id
        assert new_item.id is not None


    def test_delete_resourcegroup(self):
        net = self.test_add_resourcegroup()

        group_to_delete = net.resourcegroups.ResourceGroup[0]

        self.client.service.delete_group(group_to_delete.id)

        updated_net = self.client.service.get_network(net.id)

        group_ids = []
        for g in updated_net.resourcegroups.ResourceGroup:
            group_ids.append(g.id)

        assert group_to_delete.id not in group_ids

        self.client.service.activate_group(group_to_delete.id)

        updated_net = self.client.service.get_network(net.id)

        group_ids = []
        for g in updated_net.resourcegroups.ResourceGroup:
            group_ids.append(g.id)

        assert group_to_delete.id in group_ids


    def test_purge_group(self):
        net = self.create_network_with_data()
        scenario_id = net.scenarios.Scenario[0].id
        group_id_to_delete = net.resourcegroups.ResourceGroup[0].id

        group_datasets = self.client.service.get_resourcegroup_data(group_id_to_delete, scenario_id)
        log.info("Deleting group %s", group_id_to_delete)
        self.client.service.purge_group(group_id_to_delete)

        updated_net = self.client.service.get_network(net.id, 'Y')
        assert updated_net.resourcegroups is None

        for rs in group_datasets.ResourceScenario:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the group no timeseries to which it was attached
            #should still exist.
            d = rs.value
            if d.type == 'timeseries':
                self.assertRaises(suds.WebFault, self.client.service.get_dataset, d.id)


if __name__ == '__main__':
    server.run()
