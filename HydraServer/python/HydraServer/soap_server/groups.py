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
from spyne.model.primitive import Integer, Unicode
from spyne.decorator import rpc
from hydra_complexmodels import ResourceGroup,\
    ResourceGroupItem,\
    Scenario
from hydra_base import HydraService
from HydraServer.lib import groups as resourcegroups

class ResourceGroupService(HydraService):
    """
        The resource group soap service
    """

    @rpc(ResourceGroup, Integer, _returns=ResourceGroup)
    def add_resourcegroup(ctx, group, network_id):
        """
            Add a new group to a network.
        """
        group_i = resourcegroups.add_resourcegroup(group,
                                                   network_id,
                                                   **ctx.in_header.__dict__)
        return ResourceGroup(group_i)

    @rpc(Integer, _returns=Unicode)
    def delete_resourcegroup(ctx, group_id):
        """
            Add a new group to a scenario.
        """
        success = 'OK'
        resourcegroups.delete_resourcegroup(group_id,
                                            **ctx.in_header.__dict__)
        return success

    @rpc(ResourceGroup, _returns=ResourceGroup)
    def update_resourcegroup(ctx, group):
        """
            Add a new group to a network.
        """

        group_i = resourcegroups.update_resourcegroup(group)
        return ResourceGroup(group_i)


    @rpc(ResourceGroupItem, Integer, _returns=ResourceGroupItem)
    def add_resourcegroupitem(ctx, group_item, scenario_id):
        group_item_i = resourcegroups.add_resourcegroupitem(group_item,
                                                            scenario_id,
                                                            **ctx.in_header.__dict__)
        return ResourceGroupItem(group_item_i)

    @rpc(Integer, _returns=Unicode)
    def delete_resourcegroupitem(ctx, item_id):
        resourcegroups.delete_resourcegroupitem(item_id,
                                                  **ctx.in_header.__dict__)
        return 'OK'
