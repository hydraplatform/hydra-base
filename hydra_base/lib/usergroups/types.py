import logging
from models import UserGroup, UserGroupType, UserGroupMember, UserGroupRole
from hydra_base import db
from sqlalchemy.orm.exc import NoResultFound
from hydra_base.exceptions import HydraError, ResourceNotFoundError
from hydra_base.util.permissions import required_perms, required_role

LOG = logging.getLogger('hydra_usergroups')

def _get_usergroup_type(id, user_id, write=False):
    """
        Get a user group type by ID.
        args:
            id: The ID of the group
            user_id: THe user making the request
            write: Whether to check for write permission (if it's to be updated or deleted)
        returns:
            UserGroupType ORM object
        raises:
            ResourceNotFoundError if the type with specified ID does not exist
    """

    try:
        grouptype_i = db.DBSession.query(UserGroupType).filter(UserGroupType.id == id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User Group Type with ID {id}")

    grouptype_i.check_read_permission(user_id)

    if write is True:
        grouptype_i.check_write_permission(user_id)

    return grouptype_i

#This role could be within the scope of a group, and not a global fole
@required_role('admin')
def add_usergrouptype(usergrouptype, **kwargs):
    """
    Add a usergroup types.
    args:
        usergroup (JSONObject): The name of the type
    returns:
        hydra_base.JSONObject representing the new usergroup type
    thows:
        HydraError if a group with this name already exists
    """
    LOG.info("Adding user group type with name %s", usergrouptype.name)
    usergrouptype_i = UserGroupType()
    #this is a general way to set the attributes of an ORM object without having
    #to revisit it every time a new column is added. It relies on the JSONObjects
    #containing the correct attributes and values.
    for name, value in usergrouptype.items():
        LOG.debug("[Usergrouptype]: Setting %s : %s", name, value)
        setattr(usergrouptype_i, name, value)

    db.DBSession.add(usergrouptype_i)
    db.DBSession.flush()
    LOG.info("Usergroup type %s added with ID %s", usergrouptype.name, usergrouptype_i.id)

@required_role('admin')
def update_usergrouptype(usergrouptype, **kwargs):
    """
    Add a usergroup types.
    args:
        usergroup (JSONObject): The name of the type
    returns:
        hydra_base.JSONObject representing the new usergroup type
    thows:
        HydraError if a group with this name already exists
    """
    LOG.info("Updating user group type [ %s ]", usergrouptype.id)

    user_id = kwargs.get('user_id')

    usergrouptype_i = _get_usergroup_type(usergrouptype, user_id, write=True)

    #this is a general way to set the attributes of an ORM object without having
    #to revisit it every time a new column is added. It relies on the JSONObjects
    #containing the correct attributes and values.
    for name, value in usergrouptype.items():
        LOG.debug("[Usergrouptype]: Setting %s : %s", name, value)
        setattr(usergrouptype_i, name, value)

    db.DBSession.flush()

    LOG.info("User group type [ %s ] updated", usergrouptype.id)

@required_role('admin')
def delete_usergrouptype(type_id, **kwargs):
    """
        Remove a usergrouptype
        args:
            type_id (int): The ID of the user group type to delete
        returns:
            None
        raises:
            ResourceNotFoundError if the type with specified ID does not exist
    """
    LOG.info("Deleting user group type [ %s ]", type_id)
    user_id = kwargs.get('user_id')
    usergrouptype_i = _get_usergroup_type(type_id, user_id, write=True)
    db.DBSession.delete(usergrouptype_i)
    db.DBSession.flush()
    LOG.info("User group type [ %s ] deleted", type_id)
