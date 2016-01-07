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
    ResourceGroupItem
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
        
        Args:
            group (ResourceGroup): The group complex model to be added to the network
            network_id (int): The ID of the network to receive the new group

        Returns:
            ResourceGroup: The added resource group, with a newly created ID

        Raises:
            ResourceNotFoundError: If the network is not found
        """
        group_i = resourcegroups.add_resourcegroup(group,
                                                   network_id,
                                                   **ctx.in_header.__dict__)
        return ResourceGroup(group_i)

    @rpc(Integer, _returns=Unicode)
    def delete_resourcegroup(ctx, group_id):
        """
        Delete a resource group from a network (including all group items)
        
        Args:
            group_id (int): The ID of the group to remove

        Returns:
            String: 'OK'

        Raises:
            ResourceNotFoundError: IF the resource group is not found.
        """
        success = 'OK'
        resourcegroups.delete_resourcegroup(group_id,
                                            **ctx.in_header.__dict__)
        return success

    @rpc(ResourceGroup, _returns=ResourceGroup)
    def update_resourcegroup(ctx, group):
        """
        Update an invividual resource group (name, description, attributes, etc)

        Args:
            group (ResourceGroup): The updated resource group complex model. Must contain an 'id'

        Returns:
            ResourceGroup: The updated resource group (should be the same as that sent in)

        Raises:
            ResourceNotFoundError: If the group isn't found.
        """

        group_i = resourcegroups.update_resourcegroup(group)
        return ResourceGroup(group_i)


    @rpc(ResourceGroupItem, Integer, _returns=ResourceGroupItem)
    def add_resourcegroupitem(ctx, group_item, scenario_id):
        """
        Add an item to a group (must happen within a scenario)

        Args:
            group_item (ResourceGroupItem): The item complex model to add. This must contain a group_id
            scenario_id (int): The scenario to add the item to

        Returns:
            ResourceGroupItem: The added group item, with a uniqe ID

        Raises:
            ResourceNotFoundError: If the group or scenario are not found.
        """
        group_item_i = resourcegroups.add_resourcegroupitem(group_item,
                                                            scenario_id,
                                                            **ctx.in_header.__dict__)
        return ResourceGroupItem(group_item_i)

    @rpc(Integer, _returns=Unicode)
    def delete_resourcegroupitem(ctx, item_id):
        """
        Delete an item from a resource group

        Args:
            item_id (int): The id of the item to delete

        Returns:
            string: 'OK'

        Raises:
            ResourceNotFoundError: If the item is not found
        """
        resourcegroups.delete_resourcegroupitem(item_id,
                                                  **ctx.in_header.__dict__)
        return 'OK'
