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
from spyne.model.primitive import String, Integer32
from spyne.decorator import rpc
from hydra_base import HydraService
from HydraServer.lib import sharing


class SharingService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Integer32, String(max_occurs='unbounded'), 
         String(pattern="[YN]"), String(pattern="[YN]", default='Y'), _returns=String())
    def share_network(ctx, network_id, usernames, read_only, share):
        """
            Share a network with a list of users, identified by their usernames.
            
            The read_only flag ('Y' or 'N') must be set
            to 'Y' to allow write access or sharing.

            The share flat ('Y' or 'N') must be set to 'Y' to allow the 
            project to be shared with other users 
        """

        sharing.share_network(network_id,
                              usernames,
                              read_only,
                              share,
                              **ctx.in_header.__dict__)

        return 'OK'

    @rpc(Integer32, String(max_occurs='unbounded'),
         String(pattern="[YN]"), String(pattern="[YN]"), _returns=String)
    def share_project(ctx, project_id, usernames, read_only, share):
        """
            Share an entire project with a list of users, identifed by 
            their usernames. 
            
            The read_only flag ('Y' or 'N') must be set
            to 'Y' to allow write access or sharing.

            The share flat ('Y' or 'N') must be set to 'Y' to allow the 
            project to be shared with other users
        """
        sharing.share_project(project_id,
                              usernames,
                              read_only,
                              share,
                              **ctx.in_header.__dict__)
        return "OK"

    @rpc(Integer32, String(max_occurs="unbounded"), 
         String(pattern="[YN]"), String(pattern="[YN]"), String(pattern="[YN]"),
         _returns=String)
    def set_project_permission(ctx, project_id, usernames, read, write, share):
        """
            Set permissions on a project to a list of users, identifed by 
            their usernames. 
            
            The read flag ('Y' or 'N') sets read access, the write
            flag sets write access. If the read flag is 'N', then there is
            automatically no write access or share access.
        """
        sharing.set_project_permission(project_id,
                                       usernames,
                                       read,
                                       write,
                                       share,
                                       **ctx.in_header.__dict__)

    @rpc(Integer32, String(max_occurs="unbounded"), 
         String(pattern="[YN]"), String(pattern="[YN]"), String(pattern="[YN]"),
         _returns=String)
    def set_network_permission(ctx, network_id, usernames, read, write, share):
        """
            Set permissions on a network to a list of users, identifed by 
            their usernames. The read flag ('Y' or 'N') sets read access, the write
            flag sets write access. If the read flag is 'N', then there is
            automatically no write access or share access.
        """

        sharing.set_network_permission(network_id,
                                       usernames,
                                       read,
                                       write,
                                       share,
                                       **ctx.in_header.__dict__)

        return "OK"

    @rpc(Integer32, String(max_occurs="unbounded"),
         String(pattern="[YN]"), String(pattern="[YN]"), String(pattern="[YN]"),
         _returns=String)
    def hide_dataset(ctx, dataset_id, exceptions, read, write, share):
        """
            Hide a particular piece of data so it can only be seen by its owner.
            Only an owner can hide (and unhide) data.
            Data with no owner cannot be hidden.
            
            The exceptions paramater lists the usernames of those with permission to view the data
            read, write and share indicate whether these users can read, edit and share this data.
        """
        sharing.hide_dataset(dataset_id,
                             exceptions,
                             read,
                             write,
                             share,
                             **ctx.in_header.__dict__)

        return "OK"

    @rpc(Integer32,
         _returns=String)
    def unhide_dataset(ctx, dataset_id):
        """
            Un Hide a particular piece of data so it can only be seen by its owner.
            Only an owner can hide (and unhide) data.
        """
        sharing.unhide_dataset(dataset_id,
                             **ctx.in_header.__dict__)

        return "OK"


