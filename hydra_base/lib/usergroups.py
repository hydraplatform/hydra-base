"""
  Functions for management of UserGroups and Organisations
"""
from typing import (
    Dict,
    Set,
    List,
    Sequence
)

from hydra_base import (
    db,
    config
)

from hydra_base.db.model.usergroups import (
    UserGroup,
    Organisation,
    GroupMembers,
    GroupAdmins,
    ResourceAccess,
    Perm
)

from hydra_base.db.model import (
    User,
    Project
)

from hydra_base.lib.project import get_networks
from hydra_base.lib.users import get_user
from hydra_base.util import (
    export,
    organisation_admin,
    usergroup_admin
)

from hydra_base.exceptions import (
    HydraError,
    ResourceNotFoundError,
    PermissionError
)

from sqlalchemy.orm.exc import NoResultFound

project_max_nest_depth = int(config.get("limits", "project_max_nest_depth", 32))


@export
def add_organisation(name: str, **kwargs) -> Organisation:
    """
      Create an Organisation and its Everyone UserGroup
      Requires Hydra admin permission on caller.
    """
    user = get_user(kwargs["user_id"])
    if not user.is_admin():
        raise PermissionError(f"Only Hydra admins may create an Organisation {name=}")

    org = Organisation(name=name)
    db.DBSession.add(org)
    db.DBSession.flush()
    add_usergroup(name=Organisation.everyone, organisation_id=org.id, **kwargs)
    add_usergroup(name=Organisation.guests_group, organisation_id=org.id, **kwargs)

    return org


@export
@organisation_admin
def add_usergroup(name: str, organisation_id: int, **kwargs) -> UserGroup:
    """
      Create a UserGroup within an Organisation
    """
    group = UserGroup(name=name, organisation_id=organisation_id)
    db.DBSession.add(group)
    db.DBSession.flush()

    return group


def _purge_usergroup(group_id: int, **kwargs) -> None:
    """
      Empty a UserGroup by removing all members and administrators
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")

    for member_id in group.members:
        remove_user_from_usergroup(uid=member_id, group_id=group_id, **kwargs)

    for admin_id in group.admins:
        remove_usergroup_administrator(uid=admin_id, group_id=group_id, **kwargs)


@export
def delete_usergroup(group_id: int, purge: bool=True, **kwargs) -> None:
    """
      Delete a UserGroup
    """
    if purge:
        _purge_usergroup(group_id, **kwargs)

    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    db.DBSession.delete(group)
    db.DBSession.flush()


@export
def delete_organisation(organisation_id: int, **kwargs) -> None:
    """
      Delete an Organisation and all its UserGroups
      Requires Hydra admin permission on caller.
    """
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == organisation_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {organisation_id}")

    user = get_user(kwargs["user_id"])
    if not user.is_admin():
        raise PermissionError(f"Only Hydra admins may delete an Organisation {organisation_id=}")

    groups = get_groups_by_organisation_id(org.id)
    for group in groups:
        delete_usergroup(group.id, purge=True, **kwargs)
    db.DBSession.delete(org)
    db.DBSession.flush()


@export
def get_groups_by_organisation(organisation: Organisation, **kwargs) -> List[UserGroup]:
    """
      Retrieve all UserGroups within an Organisation
    """
    return get_groups_by_organisation_id(organisation.id)


@export
def get_groups_by_organisation_id(organisation_id: int, **kwargs) -> List[UserGroup]:
    """
      Retrieve all UserGroups in the Organisation with <organisation_id> argument
    """
    groups = db.DBSession.query(UserGroup).filter(UserGroup.organisation_id == organisation_id).all()
    return groups


@export
def get_organisation_group(organisation_id: int, groupname: str, **kwargs) -> UserGroup:
    """
      Retrieve the UserGroup with name <groupname> from the Organisation with
      id <organisation_id>
    """
    qfilter = (
        UserGroup.organisation_id == organisation_id,
        UserGroup.name == groupname
    )

    try:
        group = db.DBSession.query(UserGroup).filter(*qfilter).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Group found with {organisation_id=} {groupname=}")

    return group


@export
def get_all_organisations(**kwargs) -> List[Organisation]:
    """
      Retrieve all defined Organisations
    """
    orgs = db.DBSession.query(Organisation).all()
    return orgs


@export
@organisation_admin
def add_user_to_organisation(uid: int, organisation_id: int, **kwargs) -> None:
    """
      Add the User with id <uid> to the Organisation with id <organisation_id>
    """
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == organisation_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {organisation_id}")

    # Add User to org's Everyone...
    eo = get_organisation_group(organisation_id, Organisation.everyone)
    add_user_to_usergroup(uid, group_id=eo.id, **kwargs)


@export
@organisation_admin
def remove_user_from_organisation(uid: int, organisation_id: int, **kwargs) -> None:
    """
      Removes the User with id <uid> from the Organisation with id <organisation_id>
    """
    eo = get_organisation_group(organisation_id, Organisation.everyone)
    remove_user_from_usergroup(uid, group_id=eo.id, **kwargs)


@export
@organisation_admin
def add_guest_to_organisation(uid: int, organisation_id: int, **kwargs) -> None:
    """
      Adds the User with id <uid> to the Guests group of Organisation
      with id <organisation_id>
    """
    org = get_organisation_by_id(organisation_id)

    if is_organisation_member(uid=uid, organisation_id=organisation_id):
        raise HydraError(f"User {uid=} is a member of Organisation {organisation_id=} "
                f"and so may not be added as a Guest")

    org_guests = get_organisation_group(organisation_id, Organisation.guests_group)
    add_user_to_usergroup(uid, group_id=org_guests.id, **kwargs)


@export
@organisation_admin
def remove_guest_from_organisation(uid: int, organisation_id: int, **kwargs) -> None:
    """
      Removes the User with id <uid> from the Guests UserGroup of Organisation
      with id <organisation_id>
    """
    guests = get_organisation_group(organisation_id, Organisation.guests_group)
    remove_user_from_usergroup(uid=uid, group_id=guests.id, **kwargs)


@export
def is_user_guest_of_organisation(uid: int, organisation_id: int, **kwargs) -> bool:
    guests = get_organisation_group(organisation_id, Organisation.guests_group)
    return is_usergroup_member(uid, guests.id)


@export
def get_all_guests(organisation_id: int, **kwargs) -> List[User]:
    """
      Returns instances of all Users who are members of the Guest UserGroup
      of the Organisation with id <organisation_id>
    """
    guests = get_organisation_group(organisation_id, Organisation.guests_group)
    return get_all_usergroup_members(group_id=guests.id)


@export
def get_organisation_administrators(organisation_id: int, **kwargs) -> Set[int]:
    """
      Returns the ids of all Users who are administrators of the
      Organisation with id <organisation_id>
    """
    eo = get_organisation_group(organisation_id=organisation_id, groupname=Organisation.everyone)
    admins = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(eo.admins)).all()
    return admins


@export
def get_usergroup_by_id(group_id: int, **kwargs) -> UserGroup:
    """
      Retrieve the UserGroup with the <group_id> argument
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    return group


@export
def get_organisation_by_id(organisation_id: int, **kwargs) -> Organisation:
    """
      Returns the Organisation with id specified by the <organisation_id> argument
    """
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == organisation_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {organisation_id}")
    return org


@export
def get_organisation_by_name(organisation_name: str, **kwargs) -> Organisation:
    """
      Returns the Organisation with name specified by the <organisation_name> argument
    """
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.name == organisation_name).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with name: {organisation_name}")
    return org


@export
@usergroup_admin
def add_user_to_usergroup(uid: int, group_id: int, **kwargs) -> None:
    """
      Adds the User with id specified by the <uid> argument to the UserGroup
      with id specified by the <group_id> argument.
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")

    if is_usergroup_member(uid, group_id):
        raise HydraError(f"User {uid=} is already a member of UserGroup {group_id=}")

    gm = GroupMembers(user_id=uid, group_id=group_id)
    group._members.append(gm)
    db.DBSession.flush()


@export
@usergroup_admin
def remove_user_from_usergroup(uid: int, group_id: int, **kwargs) -> None:
    """
      Removes the User with id specified by the <uid> argument from the UserGroup
      with id specified by the <group_id> argument.
    """
    qfilter = (
        GroupMembers.group_id == group_id,
        GroupMembers.user_id == uid
    )
    try:
        gm = db.DBSession.query(GroupMembers).filter(*qfilter).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No such UserGroup member with {uid=} {group_id=}")
    db.DBSession.delete(gm)
    db.DBSession.flush()


@export
@usergroup_admin
def add_users_to_usergroup(user_ids: Sequence[int], group_id: int, **kwargs) -> None:
    """
      Adds every User identified by a user_id in the container <user_ids> to
      the UserGroup with id specified by <group_id>
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")

    for uid in user_ids:
        gm = GroupMembers(user_id=uid, group_id=group_id)
        group._members.append(gm)

    db.DBSession.flush()


@export
def transfer_user_between_usergroups(uid: int, from_gid: int, to_gid: int, **kwargs) -> None:
    """
      Move a User from one UserGroup to another. Requires that both
      UserGroups exist within the same Organisation.

      Note that any administrator privileges are not transferred with Users.
    """
    if not is_usergroup_member(uid, from_gid):
        raise HydraError(f"User {uid=} is not a member of UserGroup {from_gid=}")

    if is_usergroup_member(uid, to_gid):
        raise HydraError(f"User {uid=} is already a member of UserGroup {to_gid=}")

    from_group = get_usergroup_by_id(from_gid)
    to_group = get_usergroup_by_id(to_gid)

    if not from_group.organisation_id == to_group.organisation_id:
        raise HydraError("From and To UserGroups must exist within the same Organisation "
                f"{from_group.organisation_id=} {to_group.organisation_id=}")

    remove_user_from_usergroup(uid, group_id=from_gid, **kwargs)
    add_user_to_usergroup(uid, group_id=to_gid, **kwargs)


@export
def get_all_usergroup_members(group_id: int, **kwargs) -> List[User]:
    """
      Returns a list of all Users who are members of the UserGroup with id
      specified by the <group_id> argument.
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    members = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.members)).all()
    return members


@export
@usergroup_admin
def add_usergroup_administrator(uid: int, group_id: int, **kwargs) -> None:
    """
      Adds the User with id <uid> as an administrator of UserGroup
      with id <group_id>
    """
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    ga = GroupAdmins(user_id=uid, group_id=group_id)
    group._admins.append(ga)
    db.DBSession.flush()


@export
@usergroup_admin
def remove_usergroup_administrator(uid: int, group_id: int, **kwargs) -> None:
    """
      Removes the User with id <uid> as an administrator of UserGroup
      with id <group_id>
    """
    qfilter = (
        GroupAdmins.group_id == group_id,
        GroupAdmins.user_id == uid
    )
    try:
        ga = db.DBSession.query(GroupAdmins).filter(*qfilter).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup administrator with {uid=} {group_id=}")

    db.DBSession.delete(ga)
    db.DBSession.flush()


@export
@organisation_admin
def add_organisation_administrator(uid: int, organisation_id: int, **kwargs) -> None:
    """
      Adds the User with id <uid> as an administrator of Organisation
      with id <organisation_id>
    """
    eo = get_organisation_group(organisation_id, Organisation.everyone)
    add_usergroup_administrator(uid=uid, group_id=eo.id, **kwargs)


@export
def is_organisation_administrator(uid: int, organisation_id: int, **kwargs) -> bool:
    """
      Is the User with id <uid> an administrator of the Organisation with
      id <organisation_id>
    """
    user = get_user(uid)
    if user.is_admin():
        return True
    eo = get_organisation_group(organisation_id, Organisation.everyone)
    return eo.id in usergroups_administered_by_user(uid=uid)


@export
def get_usergroup_administrators(group_id: int, **kwargs) -> List[User]:
    """
      Returns all Users who are administrators of the UserGroup with id <group_id>
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    admins = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.admins)).all()
    return admins


@export
def is_usergroup_administrator(uid: int, group_id: int, **kwargs) -> bool:
    """
      Is the User with id <uid> an administrator of the UserGroup with
      id <group_id>?

      Note that Hydra admins are administrators of all UserGroups.
    """
    user = get_user(uid)
    if user.is_admin():
        return True
    return group_id in usergroups_administered_by_user(uid=uid)


@export
def is_usergroup_member(uid: int, group_id: int, **kwargs) -> bool:
    """
      Is the User with id <uid> a member of the UserGroup with
      id <group_id>?
    """
    return group_id in usergroups_with_member_user(uid=uid)


@export
def usergroups_administered_by_user(uid: int, **kwargs) -> Set[int]:
    """
      Returns a set of ids of UserGroups administered by the
      User with id <uid>
    """
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return {g.group_id for g in user.administers}


@export
def usergroups_with_member_user(uid: int, **kwargs) -> Set[int]:
    """
      Returns a set of ids of UserGroups of which the User with
      id <uid> is a member
    """
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return {g.group_id for g in user.groups}


@export
def is_organisation_member(uid: int, organisation_id: int, **kwargs) -> bool:
    """
      Is the User with id <uid> a member of the Organisation with
      id <organisation_id>?
    """
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return organisation_id in {o.id for o in user.organisations}


@export
def get_all_organisation_members(organisation_id: int, **kwargs) -> List[User]:
    """
      Returns all Users who are members of the Organisation with id <organisation_id>

      Note that this does not include non-member Guests
    """
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == organisation_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {organisation_id}")

    members = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(org.members)).all()
    return members


def _add_resource_access(res: str, usergroup_id: int, res_id: int, access: Perm, **kwargs) -> Perm:
    ra = ResourceAccess(resource=res.upper(), usergroup_id=usergroup_id, resource_id=res_id, access=int(access))
    db.DBSession.add(ra)
    db.DBSession.flush()

    return ra.access


@export
def set_resource_access(res: str, usergroup_id: int, res_id: int, access: Perm, **kwargs) -> ResourceAccess:
    qfilter = (
        ResourceAccess.resource == res.upper(),
        ResourceAccess.usergroup_id == usergroup_id,
        ResourceAccess.resource_id == res_id
    )
    try:
        current = db.DBSession.query(ResourceAccess).filter(*qfilter).one()
    except NoResultFound:
        return _add_resource_access(res, usergroup_id, res_id, access)

    current.access = int(access)
    db.DBSession.flush()

    return current


@export
def get_resource_access_mask(res: str, usergroup_id: int, res_id: int, do_raise: bool=True, **kwargs) -> Perm:
    qfilter = (
        ResourceAccess.resource == res.upper(),
        ResourceAccess.usergroup_id == usergroup_id,
        ResourceAccess.resource_id == res_id
    )
    try:
        mask = db.DBSession.query(ResourceAccess.access).filter(*qfilter).scalar()
    except NoResultFound:
        if do_raise:
            raise ResourceNotFoundError(f"No resource access mask for {res.upper()} {res_id=}")
        else:
            return None
    return mask


def _usergroup_has_perm(perm: Perm, usergroup_id: int, resource: str, resource_id: int, **kwargs) -> bool:
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return perm & mask


@export
def usergroup_can_read(usergroup_id: int, resource: str, resource_id: int, **kwargs) -> bool:
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Read


@export
def usergroup_can_write(usergroup_id: int, resource: str, resource_id: int, **kwargs) -> bool:
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Write


@export
def usergroup_can_share(usergroup_id: int, resource: str, resource_id: int, **kwargs) -> bool:
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Share


@export
def any_usergroup_can_read(usergroup_ids: Sequence[int], resource: str, resource_id: int, **kwargs) -> bool:
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Read, ugid, resource, resource_id):
            return True

    return False


@export
def any_usergroup_can_write(usergroup_ids: Sequence[int], resource: str, resource_id: int, **kwargs) -> bool:
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Write, ugid, resource, resource_id):
            return True

    return False


@export
def any_usergroup_can_share(usergroup_ids: Sequence[int], resource: str, resource_id: int, **kwargs) -> bool:
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Share, ugid, resource, resource_id):
            return True

    return False


@export
def user_has_permission_by_membership(uid: int, perm: Perm, resource: str, resource_id: int, **kwargs) -> bool:
    groups = usergroups_with_member_user(uid)
    for group in groups:
        if _usergroup_has_perm(perm, group.id, resource, resource_id):
            return True

    return False


@export
def make_project_visible_to_usergroup(group_id: int, project_id: int, **kwargs):
    group = get_usergroup_by_id(group_id)
    return set_resource_access("PROJECT", group.id, project_id, Perm.Read)


@export
def make_project_visible_to_organisation(organisation_id: int, project_id: int, **kwargs):
    eo = get_organisation_group(organisation_id, Organisation.everyone)
    return set_resource_access("PROJECT", eo.id, project_id, Perm.Read)


@export
def get_permissions_map(**kwargs) -> Dict[str, int]:
    return {p.name: p.value for p in Perm}


@export
def get_default_organisation_usergroup_name(**kwargs) -> str:
    return Organisation.everyone


@export
def get_guest_organisation_usergroup_name(**kwargs) -> str:
    return Organisation.guests_group


def _flatten_children(p) -> List[int]:
    """
      Flattens a nested list of projects of the form returned by
      Project.get_child_projects() into a simple list.
    """
    hier = [p.id]
    if getattr(p, "projects", None):  # null if empty or not present
        for pp in p.projects:
            hier += _flatten_children(pp)

    return hier


@export
def get_all_usergroup_projects(group_id: int, include_children: bool=False, **kwargs) -> Set[int]:
    group = get_usergroup_by_id(group_id)
    qfilter = (
        ResourceAccess.resource == "PROJECT",
        ResourceAccess.usergroup_id == group.id
    )

    projs = db.DBSession.query(ResourceAccess.resource_id, ResourceAccess.access).filter(*qfilter).all()
    visible_ids = {proj_id for proj_id, mask in projs if mask & Perm.Read}
    if include_children:
        projects = db.DBSession.query(Project).filter(Project.id.in_(visible_ids)).all()
        child_ids = []
        for project in projects:
            children = project.get_child_projects(user_id=kwargs["user_id"], levels=project_max_nest_depth)
            for child in children:
                child_ids += _flatten_children(child)

        return visible_ids | set(child_ids)
    else:
        return visible_ids


@export
def get_all_usergroup_networks(group_id: int, **kwargs) -> Set[int]:
    visible_projects = get_all_usergroup_projects(group_id, include_children=True, **kwargs)

    net_ids = set()
    for project_id in visible_projects:
        networks = get_networks(project_id, user_id=kwargs["user_id"])
        net_ids.update({net.id for net in networks})

    return net_ids


@export
def get_all_user_projects(uid: int, **kwargs) -> Set[int]:
    groups = usergroups_with_member_user(uid)
    user_projects = set()
    for group in groups:
        user_projects.update(get_all_usergroup_projects(group, include_children=True, **kwargs))

    return user_projects


@export
def get_all_organisation_projects(organisation_id: int, **kwargs) -> Set[int]:
    eo = get_organisation_group(organisation_id, Organisation.everyone)
    return get_all_usergroup_projects(eo.id, include_children=True, **kwargs)


@export
@organisation_admin
def organisation_report(organisation_id: int, **kwargs) -> Dict:
    """
      Returns a brief report describing the UserGroups defined
      within an Organisation and the count of members in each.
    """
    org_members = get_all_organisation_members(organisation_id)
    org_admins = get_organisation_administrators(organisation_id)
    org_guests = get_all_guests(organisation_id)
    report = {
        "members": len(org_members),
        "admins": [a.username for a in org_admins],
        "guests": [a.username for a in org_guests],
        "groups": []
    }

    groups = get_groups_by_organisation_id(organisation_id)
    for group in groups:
        if group.name in (Organisation.everyone, Organisation.guests_group):
            continue
        report["groups"].append({
            "name": group.name,
            "members": len(group.members|group.admins),
            "admins": len(group.admins)
        })
    return report


if __name__ == "__main__":
    # Display the names of functions exported
    # via __all__ by the @export decorator
    [*(print(f) for f in sorted(__all__))]
    print(f"{len(__all__)=}")
