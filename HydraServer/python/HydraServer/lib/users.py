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
from sqlalchemy.exc import InvalidRequestError

from HydraServer.db.model import User, Role, Perm, RoleUser, RolePerm
from HydraServer.db import DBSession

import bcrypt

from HydraLib.HydraException import ResourceNotFoundError, HydraError
import logging

log = logging.getLogger(__name__)

def _get_user_id(username):
    try:
        rs = DBSession.query(User.user_id).filter(User.username==username).one()
        return rs.user_id
    except:
        return None

def get_username(uid,**kwargs):
    rs = DBSession.query(User.username).filter(User.user_id==uid).one()
    return rs.username

def add_user(user,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'add_user')
    u = User()
    
    u.username     = user.username
    u.display_name = user.display_name
    
    user_id = _get_user_id(u.username)

    #If the user is already there, cannot add another with 
    #the same username.
    if user_id is not None:
        raise HydraError("User %s already exists!"%user.username)

    u.password = bcrypt.hashpw(user.password.encode('utf-8'), bcrypt.gensalt())
    
    DBSession.add(u)
    DBSession.flush()

    return u

def update_user_display_name(user,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'edit_user')
    try:
        user_i = DBSession.query(User).filter(User.user_id==user.id).one()
        user_i.display_name = user.display_name
        return user_i
    except InvalidRequestError:
        raise ResourceNotFoundError("User (id=%s) not found"%(user.id))

def update_user_password(new_pwd_user_id, new_password,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'edit_user')
    try:
        user_i = DBSession.query(User).filter(User.user_id==new_pwd_user_id).one()
        user_i.password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        return user_i
    except InvalidRequestError:
        raise ResourceNotFoundError("User (id=%s) not found"%(new_pwd_user_id))

def get_user_by_name(uname,**kwargs):
    """
    """
    try:
        user_i = DBSession.query(User).filter(User.username==uname).one()
        return user_i
    except InvalidRequestError:    
        return None

def delete_user(deleted_user_id,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'edit_user')
    try:
        user_i = DBSession.query(User).filter(User.user_id==deleted_user_id).one()
        DBSession.delete(user_i)
    except InvalidRequestError:    
        raise ResourceNotFoundError("User (user_id=%s) does not exist"%(deleted_user_id))


    return 'OK'


def add_role(role,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'add_role')
    role_i = Role(role_name=role.name, role_code=role.code)
    DBSession.add(role_i)
    DBSession.flush()

    return role_i

def delete_role(role_id,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        role_i = DBSession.query(Role).filter(Role.role_id==role_id).one()
        DBSession.delete(role_i)
    except InvalidRequestError:    
        raise ResourceNotFoundError("Role (role_id=%s) does not exist"%(role_id))

    return 'OK'

def add_perm(perm,**kwargs):
    """
    """
    #check_perm(kwargs.get('user_id'), 'add_perm')
    perm_i = Perm(perm_name=perm.name, perm_code=perm.code)
    DBSession.add(perm_i)
    DBSession.flush()

    return perm_i

def delete_perm(perm_id,**kwargs):
    """
    """

    #check_perm(kwargs.get('user_id'), 'edit_perm')
    try:
        perm_i = DBSession.query(Perm).filter(Perm.perm_id==perm_id).one()
        DBSession.delete(perm_i)
    except InvalidRequestError:    
        raise ResourceNotFoundError("Permission (id=%s) does not exist"%(perm_id))

    return 'OK' 

def set_user_role(new_user_id, role_id,**kwargs):
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        roleuser_i = RoleUser(user_id=new_user_id, role_id=role_id)
        DBSession.add(roleuser_i) 
        DBSession.flush()
    except InvalidRequestError:    
        raise ResourceNotFoundError("User Role does not exist")

    return roleuser_i.role

def delete_user_role(deleted_user_id, role_id,**kwargs):

    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        roleuser_i = DBSession.query(RoleUser).filter(RoleUser.user_id==deleted_user_id, RoleUser.role_id==role_id).one()
        DBSession.delete(roleuser_i)
    except InvalidRequestError:    
        raise ResourceNotFoundError("User Role does not exist")

    return 'OK'

def set_role_perm(role_id, perm_id,**kwargs):
    #check_perm(kwargs.get('user_id'), 'edit_perm')
    roleperm_i = RolePerm(role_id=role_id, perm_id=perm_id)
    DBSession.add(roleperm_i)
    DBSession.flush()

    return roleperm_i.role

def delete_role_perm(role_id, perm_id,**kwargs):
    #check_perm(kwargs.get('user_id'), 'edit_perm')
    try:
        roleperm_i = DBSession.query(RolePerm).filter(RolePerm.role_id==role_id, RolePerm.perm_id==perm_id).one()
        DBSession.delete(roleperm_i)
    except InvalidRequestError:    
        raise ResourceNotFoundError("Role Perm does not exist")

    return 'OK'


def update_role(role,**kwargs):
    """
        Update the role.
        Used to add permissions and users to a role.
    """
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        role_i = DBSession.query(Role).filter(Role.role_id==role.id).one()
        role_i.role_name = role.name
        role_i.role_code = role.code
    except InvalidRequestError:    
        raise ResourceNotFoundError("Role (role_id=%s) does not exist"%(role.id))

    for perm in role.permissions:
        roleperm_i = RolePerm(role_id=role.id, 
                              perm_id=perm.id
                              )

        DBSession.add(roleperm_i)

    for user in role.users:
        roleuser_i = RoleUser(user_id=user.id,
                                         perm_id=perm.id
                                        )

        DBSession.add(roleuser_i)

    DBSession.flush()
    return role_i

        
def get_all_users(**kwargs):
    """
        Get the username & ID of all users.
    """

    rs = DBSession.query(User).all()

    return rs

def get_all_perms(**kwargs):
    """
        Get all permissions
    """
    rs = DBSession.query(Perm).all()
    return rs

def get_all_roles(**kwargs):
    """
        Get all roles
    """
    rs = DBSession.query(Role).all()
    return rs

def get_role(role_id,**kwargs):
    """
        Get a role by its ID.
    """
    try:
        role = DBSession.query(Role).filter(Role.role_id==role_id).one()
        return role
    except: 
        raise HydraError("Role not found (role_id=%s)", role_id)
    
def get_user_roles(uid,**kwargs):
    """
        Get the roles for a user.
        @param user_id
    """
    try:
        user_roles = DBSession.query(Role).filter(Role.role_id==RoleUser.role_id,
                                                  RoleUser.user_id==uid).all()
        return user_roles
    except: 
        raise HydraError("Roles not found for user (user_id=%s)", uid)

def get_user_permissions(uid, **kwargs):
    """
        Get the roles for a user.
        @param user_id
    """
    try:
        user_perms = DBSession.query(Perm).filter(Perm.perm_id==RolePerm.perm_id,
                                                  RolePerm.role_id==Role.role_id,
                                                  Role.role_id==RoleUser.role_id,
                                                  RoleUser.user_id==uid).all()
        return user_perms
    except: 
        raise HydraError("Permissions not found for user (user_id=%s)", uid)

def get_role_by_code(role_code,**kwargs):
    """
        Get a role by its code
    """
    try:
        role = DBSession.query(Role).filter(Role.role_code==role_code).one()
        return role
    except:
        raise ResourceNotFoundError("Role not found (role_code=%s)"%(role_code))
    

def get_perm(perm_id,**kwargs):
    """
        Get all permissions
    """

    try:
        perm = DBSession.query(Perm).filter(Perm.perm_id==perm_id).one()
        return perm
    except:
        raise ResourceNotFoundError("Permission not found (perm_id=%s)"%(perm_id))

def get_perm_by_code(perm_code,**kwargs):
    """
        Get a permission by its code 
    """

    try:
        perm = DBSession.query(Perm).filter(Perm.perm_code==perm_code).one()
        return perm
    except:
        raise ResourceNotFoundError("Permission not found (perm_code=%s)"(perm_code))
