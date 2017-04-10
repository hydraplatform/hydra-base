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
from HydraServer.db import DBSession
from HydraServer.db.model import Perm, User, RolePerm, RoleUser
from sqlalchemy.orm.exc import NoResultFound
from HydraLib.HydraException import PermissionError

def check_perm(user_id, permission_code):
    """
        Checks whether a user has permission to perform an action.
        The permission_code parameter should be a permission contained in tPerm.

        If the user does not have permission to perfom an action, a permission
        error is thrown.
    """
    try:
        perm = DBSession.query(Perm).filter(Perm.perm_code==permission_code).one()
    except NoResultFound:
        raise PermissionError("No permission %s"%(permission_code))


    try:
        res = DBSession.query(User).join(RoleUser, RoleUser.user_id==User.user_id).\
            join(Perm, Perm.perm_id==perm.perm_id).\
            join(RolePerm, RolePerm.perm_id==Perm.perm_id).filter(User.user_id==user_id).one()
    except NoResultFound:
        raise PermissionError("Permission denied. User %s does not have permission %s"%
                        (user_id, permission_code))
