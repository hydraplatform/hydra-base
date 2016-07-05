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
        Perm

from hydra_base import HydraService
from HydraServer.lib import users
import logging
log = logging.getLogger(__name__)


class UserService(HydraService):
    """
        The user soap service
    """

    @rpc(Integer, _returns=Unicode)
    def get_username(ctx, uid):
        """

        Get a user's username.

        Args:
            uid (int): The ID of the user

        Returns:
            string: the user's username

        Raises:
            ResourceNotFoundError: If the uid is not found
            
        """
        username = users.get_username(uid, **ctx.in_header.__dict__)
        return username



    @rpc(User, _returns=User)
    def add_user(ctx, user):
        """
        Add a new user.

        Args:
            user (hydra_complexmodels.User): The user to be added, including username, display name and password

        Returns:
            hydra_complexmodels.User: The newly added user, without password

        Raises:
            ResourceNotFoundError: If the uid is not found
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

        Args:
            user (hydra_complexmodels.User): The user to be updated. Only the display name is updated

        Returns:
            hydra_complexmodels.User: The updated user, without password

        Raises:
            ResourceNotFoundError: If the uid is not found
        """
        user_i = users.update_user_display_name(user, **ctx.in_header.__dict__)

        return User(user_i)


    @rpc(Integer, Unicode, _returns=User)
    def update_user_password(ctx, user_id, new_password):
        """
        Update a user's password

        Args:
            user_id (int): The ID user to be updated.
            new_password (string): The new password.

        Returns:
            hydra_complexmodels.User: The updated user, without password

        Raises:
            ResourceNotFoundError: If the user_id is not found
        """
        user_i = users.update_user_password(user_id, 
                                            new_password,
                                            **ctx.in_header.__dict__)

        return User(user_i)

    @rpc(Unicode, _returns=User)
    def get_user_by_name(ctx, username):
        """
        Get a user by username

        Args:
            username (string): The username being searched for

        Returns:
            hydra_complexmodels.User: The requested user, or None 

        """
        user_i = users.get_user_by_name(username, **ctx.in_header.__dict__)
        if user_i:
            return User(user_i)
        
        return None

    @rpc(Integer, _returns=Unicode)
    def delete_user(ctx, user_id):
        """
        Delete a user permenantly from the DB.

        Args:
            user_id (int): The ID user to be deleted.

        Returns:
            string: 'OK' 

        Raises:
            ResourceNotFoundError: If the user_id is not found
        """
        success = 'OK'
        users.delete_user(user_id, **ctx.in_header.__dict__)
        return success


    @rpc(Role, _returns=Role)
    def add_role(ctx, role):
        """
        Add a new role. A role is the highest level in the permission tree
        and is container for permissions. Users are then assigned a role, which
        gives them the specified permissions

        Args:
            role (hydra_complexmodels.Role): The role to be added 

        Returns:
            hydra_complexmodels.Role: The newly created role, complete with ID

        """
        role_i = users.add_role(role, **ctx.in_header.__dict__)
        return Role(role_i)

    @rpc(Integer, _returns=Unicode)
    def delete_role(ctx, role_id):
        """
        Permenantly delete a role (and all its sub-permissions and users)

        Args:
            role_id (int): The role to be deleted 

        Returns:
            string: 'OK' 

        Raises:
            ResourceNotFoundError: If the role_id is not found
        """
        success = 'OK'
        users.delete_role(role_id, **ctx.in_header.__dict__)
        return success

    @rpc(Perm, _returns=Perm)
    def add_perm(ctx, perm):
        """
        Add a new permission. A permission defines a particular action that
        a user can perform. A permission is independent of roles and are
        added to roles later.

        Args:
            perm (hydra_complexmodels.Perm): The new permission.

        Returns:
            hydra_complexmodels.Perm: The new permission 

        """
        perm_i = users.add_perm(perm, **ctx.in_header.__dict__)
        return Perm(perm_i)

    @rpc(Integer, _returns=Unicode)
    def delete_perm(ctx, perm_id):
        """
        Permenantly delete a permission

        Args:
            perm_id (int): The permission to be deleted 

        Returns:
            string: 'OK' 

        Raises:
            ResourceNotFoundError: If the perm_id is not found
        """
        success = 'OK'
        users.delete_perm(perm_id)
        return success

    @rpc(Integer, Integer, _returns=Role)
    def set_user_role(ctx, user_id, role_id):
        """
        Assign a user to a role 

        Args:
            user_id (int): The user to be given the new role
            role_id (int): The role to receive the new user

        Returns:
            hydra_complexmodels.Role: Returns the role to which the user has been added. (contains all the user roles).

        Raises:
            ResourceNotFoundError: If the user_id or role_id do not exist
        """
        role_i = users.set_user_role(user_id,
                                     role_id,
                                     **ctx.in_header.__dict__)

        return Role(role_i)

    @rpc(Integer, Integer, _returns=Unicode)
    def delete_user_role(ctx, user_id, role_id):
        """
        Remove a user from a role 

        Args:
            user_id (int): The user to be removed from the role
            role_id (int): The role to lose the user

        Returns:
            string: 'OK'

        Raises:
            ResourceNotFoundError: If the role does not contain the user or if the user or role do not exist.
        """

        success = 'OK'
        users.delete_user_role(user_id, role_id, **ctx.in_header.__dict__)
        return success

    @rpc(Integer, Integer, _returns=Role)
    def set_role_perm(ctx, role_id, perm_id):
        """
        Assign a permission to a role 

        Args:
            role_id (int): The role to receive the permission
            perm_id (int): The permission being added to the role

        Returns:
            hydra_complexmodels.Role: The newly updated role, complete with new permission

        Raises:
            ResourceNotFoundError: If the role or permission do not exist 
        """
        role_i = users.set_role_perm(role_id,
                                     perm_id,
                                     **ctx.in_header.__dict__)
        return Role(role_i)

    @rpc(Integer, Integer, _returns=Unicode)
    def delete_role_perm(ctx, role_id, perm_id):
        """
        Remove a permission from a role 

        Args:
            role_id (int): The role to lose the permission 
            perm_id (int): The permission being removed from the role

        Returns:
            string: 'OK'

        Raises:
            ResourceNotFoundError: If the role does not contain the permission or if the userermission or role do not exist.
        """


        success = 'OK'
        users.delete_role_perm(role_id, perm_id, **ctx.in_header.__dict__)
        return success


    @rpc(Role, _returns=Role)
    def update_role(ctx, role):
        """
        Update a role.
        Used to add multiple permissions and users to a role in one go.

        Args:
            role (hydra_complexmodels.Role): The role with new users & permissions

        Returns:
            hydra_complexmodels.Role: The newly updated role

        Raises:
            ResourceNotFoundError: If the role or any users or permissions do not exist
        """
        role_i = users.update_role(role, **ctx.in_header.__dict__)
        return Role(role_i)

            
    @rpc(_returns=SpyneArray(User))
    def get_all_users(ctx):
        """
        Get the username & ID of all users.

        Args:

        Returns:
            List(hydra_complexmodels.User): All the users in the system

        Raises:

        """

        all_user_dicts = users.get_all_users(**ctx.in_header.__dict__)
        all_user_cms = [User(u) for u in all_user_dicts]
        return all_user_cms

    @rpc(_returns=SpyneArray(Perm))
    def get_all_perms(ctx):
        """
        Get all permissions

        Args:

        Returns:
            List(hydra_complexmodels.Perm): All the permissions in the system

        Raises:


        """
        all_perm_dicts = users.get_all_perms(**ctx.in_header.__dict__)
        all_perm_cms = [Perm(p) for p in all_perm_dicts]
        return all_perm_cms

    @rpc(_returns=SpyneArray(Role))
    def get_all_roles(ctx):
        """
        Get all roles

        Args:

        Returns:
            List(hydra_complexmodels.Role): All the roles in the system

        Raises:


        """
        all_role_dicts = users.get_all_roles(**ctx.in_header.__dict__)
        all_role_cms   = [Role(r) for r in all_role_dicts]
        return all_role_cms

    @rpc(Integer, _returns=Role)
    def get_role(ctx, role_id):
        """
        Get a role by its ID.

        Args:
            role_id (int): The ID of the role to retrieve
        Returns:
            hydra_complexmodels.Role: The role

        Raises:
            ResourceNotFoundError: If the role does not exist

        """
        role_i = users.get_role(role_id, **ctx.in_header.__dict__)        
        return Role(role_i)


    @rpc(Unicode, _returns=Role)
    def get_role_by_code(ctx, role_code):
        """
        Get a role by its code instead of ID (IDS can change between databases. codes
        are more stable)

        Args:
            role_code (string): The code of the role to retrieve
        Returns:
            hydra_complexmodels.Role: The role

        Raises:
            ResourceNotFoundError: If the role does not exist


        """
        role_i = users.get_role_by_code(role_code, **ctx.in_header.__dict__)

        return Role(role_i)

    @rpc(Unicode, _returns=SpyneArray(Role))
    def get_user_roles(ctx, user_id):
        """
        Get the roles assigned to a user. (A user can have multiple roles)

        Args:
            user_id (int): The ID of the user whose roles you want 

        Returns:
            List(hydra_complexmodels.Role): The roles of the user

        Raises:
            ResourceNotFoundError: If the user does not exist

        """
        roles = users.get_user_roles(user_id, **ctx.in_header.__dict__)
        return [Role(r) for r in roles]

    @rpc(Integer, _returns=Perm)
    def get_perm(ctx, perm_id):
        """
        Get a permission, by ID

        Args:
            perm_id (int): The ID of the permission

        Returns:
            hydra_complexmodels.Perm: The permission 

        Raises:
            ResourceNotFoundError: If the permission does not exist

        """
        perm = users.get_perm(perm_id, **ctx.in_header.__dict__)
        perm_cm = Perm(perm)
        return perm_cm

    @rpc(Unicode, _returns=Perm)
    def get_perm_by_code(ctx, perm_code):
        """
        Get a permission by its code.  Permission IDS change between hydra instances. Codes are more stable.

        Args:
            perm_id (int): The code of the permission.

        Returns:
            hydra_complexmodels.Perm: The permission 

        Raises:
            ResourceNotFoundError: If the permission does not exist

        """
        perm = users.get_perm_by_code(perm_code, **ctx.in_header.__dict__)
        perm_cm = Perm(perm)
        return perm_cm

    @rpc(Unicode, _returns=SpyneArray(Perm))
    def get_user_permissions(ctx, user_id):
        """
        Get all the permissions granted to the user, based on all the roles that the user is in.
        Args:
            user_id (int): The user whose permissions you want

        Returns:
            List(hydra_complexmodels.Perm): The user's permissions 

        Raises:
            ResourceNotFoundError: If the user does not exist


        """
        perms = users.get_user_permissions(user_id, **ctx.in_header.__dict__)
        return [Perm(p) for p in perms]
