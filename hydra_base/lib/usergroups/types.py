import logging
from hydra_base.db.models import UserGroup, UserGroupType, UserGroupMember, GroupRoleUser
from hydra_base import db
from hydra_base.lib.objects import JSONObject
from sqlalchemy.orm.exc import NoResultFound
from hydra_base.exceptions import HydraError, ResourceNotFoundError
from hydra_base.util.permissions import required_perms, required_role

LOG = logging.getLogger('hydra_usergroups')

IGNORE_COLUMNS = ['updated_at', 'updated_by', 'cr_date']

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

    return grouptype_i

@required_role('admin')
def get_usergrouptypes(**kwargs):
    """
    Get all usergroup types.
    args:

    returns:
        list(hydra_base.JSONObject) representing the usergroup types
    thows:
        HydraError if a group with this name already exists
    """
    LOG.info("Getting all user group types")

    usergrouptypes_i = db.DBSession.query(UserGroupType).all()

    LOG.info("Retrieved %s, group types", len(usergrouptypes_i))

    return usergrouptypes_i

@required_role('admin')
def get_usergrouptype(usergrouptypeid, **kwargs):
    """
    Get a usergroup type.
    args:
        usergrouptypeid (JSONObject): The name of the type
    returns:
        hydra_base.JSONObject representing the usergroup type
    thows:
        HydraError if a group with this ID doesn't exist
    """
    LOG.info("Getting user group type [ %s ]", usergrouptypeid)

    user_id = kwargs.get('user_id')

    usergrouptype_i = _get_usergroup_type(usergrouptypeid, user_id, write=True)

    LOG.info("Retrieved group type [ %s ] updated", usergrouptypeid)

    return JSONObject(usergrouptype_i)

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
        if name not in IGNORE_COLUMNS:
            setattr(usergrouptype_i, name, value)

    db.DBSession.add(usergrouptype_i)
    db.DBSession.flush()
    #load any defaulted columns (like cr_date)
    db.DBSession.refresh(usergrouptype_i)
    LOG.info("Usergroup type %s added with ID %s", usergrouptype.name, usergrouptype_i.id)
    return usergrouptype_i

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

    usergrouptype_i = _get_usergroup_type(usergrouptype.id, user_id, write=True)

    #this is a general way to set the attributes of an ORM object without having
    #to revisit it every time a new column is added. It relies on the JSONObjects
    #containing the correct attributes and values.
    for name, value in usergrouptype.items():
        LOG.info("[Usergrouptype]: Setting %s : %s", name, value)
        if name not in IGNORE_COLUMNS:
            setattr(usergrouptype_i, name, value)

    db.DBSession.flush()

    LOG.info("User group type [ %s ] updated", usergrouptype.id)

    return usergrouptype_i

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
