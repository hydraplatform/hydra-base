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
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import User,\
        Role,\
        Perm,\
        RolePerm,\
        RoleUser

from hydra_base import HydraService
from lib import users
import logging
log = logging.getLogger(__name__)


class UserService(HydraService):
    """
        The user soap service
    """

    @rpc(Integer, _returns=Unicode)
    def get_username(ctx, uid):
        """
            Add a new user.
        """
        username = users.get_username(uid, **ctx.in_header.__dict__)
        return username



    @rpc(User, _returns=User)
    def add_user(ctx, user):
        """
            Add a new user.
        """
        user_i = users.add_user(user, **ctx.in_header.__dict__)
        #u = User()
        #u.__dict__ = user_i.__dict__
        #return u
        
        return User(user_i)

    @rpc(User, _returns=User)
    def update_user_display_name(ctx, user):
        """
            Update a user's display name
        """
        user_i = users.update_user_display_name(user, **ctx.in_header.__dict__)

        return User(user_i)


    @rpc(Integer, Unicode, _returns=User)
    def update_user_password(ctx, user_id, new_password):
        """
            Update a user's password
        """
        user_i = users.update_user_password(user_id, 
                                            new_password,
                                            **ctx.in_header.__dict__)

        return User(user_i)

    @rpc(Unicode, _returns=User)
    def get_user_by_name(ctx, username):
        """
            Get a user by username
        """
        user_i = users.get_user_by_name(username, **ctx.in_header.__dict__)
        if user_i:
            return User(user_i)
        
        return None

    @rpc(Integer, _returns=Unicode)
    def delete_user(ctx, user_id):
        """
            Delete a user.
        """
        success = 'OK'
        users.delete_user(user_id, **ctx.in_header.__dict__)
        return success


    @rpc(Role, _returns=Role)
    def add_role(ctx, role):
        """
            Add a new role.
        """
        role_i = users.add_role(role, **ctx.in_header.__dict__)
        return Role(role_i)

    @rpc(Integer, _returns=Unicode)
    def delete_role(ctx, role_id):
        """
            Delete a role.
        """
        success = 'OK'
        users.delete_role(role_id, **ctx.in_header.__dict__)
        return success

    @rpc(Perm, _returns=Perm)
    def add_perm(ctx, perm):
        """
            Add a new permission
        """
        perm_i = users.add_perm(perm, **ctx.in_header.__dict__)
        return Perm(perm_i)

    @rpc(Integer, _returns=Unicode)
    def delete_perm(ctx, perm_id):
        """
            Delete a permission
        """
        success = 'OK'
        users.delete_perm(perm_id)
        return success

    @rpc(Integer, Integer, _returns=Role)
    def set_user_role(ctx, user_id, role_id):
        role_i = users.set_user_role(user_id,
                                     role_id,
                                     **ctx.in_header.__dict__)

        return Role(role_i)

    @rpc(Integer, Integer, _returns=Unicode)
    def delete_user_role(ctx, user_id, role_id):
        success = 'OK'
        users.delete_user_role(user_id, role_id, **ctx.in_header.__dict__)
        return success

    @rpc(Integer, Integer, _returns=Role)
    def set_role_perm(ctx, role_id, perm_id):
        role_i = users.set_role_perm(role_id,
                                     perm_id,
                                     **ctx.in_header.__dict__)
        return Role(role_i)

    @rpc(Integer, Integer, _returns=Unicode)
    def delete_role_perm(ctx, role_id, perm_id):
        success = 'OK'
        users.delete_role_perm(role_id, perm_id, **ctx.in_header.__dict__)
        return success


    @rpc(Role, _returns=Role)
    def update_role(ctx, role):
        """
            Update the role.
            Used to add permissions and users to a role.
        """
        role_i = users.update_role(role, **ctx.in_header.__dict__)
        return Role(role_i)

            
    @rpc(_returns=SpyneArray(User))
    def get_all_users(ctx):
        """
            Get the username & ID of all users.
        """

        all_user_dicts = users.get_all_users(**ctx.in_header.__dict__)
        all_user_cms = [User(u) for u in all_user_dicts]
        return all_user_cms

    @rpc(_returns=SpyneArray(Perm))
    def get_all_perms(ctx):
        """
            Get all permissions
        """
        all_perm_dicts = users.get_all_perms(**ctx.in_header.__dict__)
        all_perm_cms = [Perm(p) for p in all_perm_dicts]
        return all_perm_cms

    @rpc(_returns=SpyneArray(Role))
    def get_all_roles(ctx):
        """
            Get all roles
        """
        all_role_dicts = users.get_all_roles(**ctx.in_header.__dict__)
        all_role_cms   = [Role(r) for r in all_role_dicts]
        return all_role_cms

    @rpc(Integer, _returns=Role)
    def get_role(ctx, role_id):
        """
            Get a role by its ID.
        """
        role_i = users.get_role(role_id, **ctx.in_header.__dict__)        
        return Role(role_i)


    @rpc(Unicode, _returns=Role)
    def get_role_by_code(ctx, role_code):
        """
            Get a role by its code
        """
        role_i = users.get_role_by_code(role_code, **ctx.in_header.__dict__)

        return Role(role_i)

    @rpc(Unicode, _returns=SpyneArray(Role))
    def get_user_roles(ctx, user_id):
        """
            Get the roles assigned to a user.
            @param: user_id
        """
        roles = users.get_user_roles(user_id, **ctx.in_header.__dict__)
        return [Role(r) for r in roles]

    @rpc(Integer, _returns=Perm)
    def get_perm(ctx, perm_id):
        """
            Get all permissions
        """
        perm = users.get_perm(perm_id, **ctx.in_header.__dict__)
        perm_cm = Perm(perm)
        return perm_cm

    @rpc(Unicode, _returns=Perm)
    def get_perm_by_code(ctx, perm_code):
        """
            Get a permission by its code 
        """
        perm = users.get_perm_by_code(perm_code, **ctx.in_header.__dict__)
        perm_cm = Perm(perm)
        return perm_cm

    @rpc(Unicode, _returns=SpyneArray(Perm))
    def get_user_permissions(ctx, user_id):
        """
            Get all the permissions granted to the user, based
            on all the roles that the user is in.
            @param: user_id
        """
        perms = users.get_user_permissions(user_id, **ctx.in_header.__dict__)
        return [Perm(p) for p in perms]
