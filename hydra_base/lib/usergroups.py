import inspect
import time

from typing import (
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
    OrganisationMembers,
    ResourceAccess,
    Perm
)

from hydra_base.db.model import (
    User,
    Project
)

from hydra_base.lib.project import get_networks
from hydra_base.util import export

from hydra_base.exceptions import (
    HydraError,
    ResourceNotFoundError
)

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

project_max_nest_depth = int(config.get("limits", "project_max_nest_depth", 32))


@export
def add_organisation(name: str, **kwargs) -> Organisation:
    org = Organisation(name=name)
    db.DBSession.add(org)
    db.DBSession.flush()
    add_usergroup(name=Organisation.everyone, organisation=org)

    return org


@export
def add_usergroup(name: str, organisation: Organisation=None, **kwargs) -> UserGroup:
    oid = getattr(organisation, "id", None)
    group = UserGroup(name=name, organisation_id=oid)
    db.DBSession.add(group)
    db.DBSession.flush()

    return group


def _purge_usergroup(group_id: int, **kwargs) -> None:
    """
      Remove Administrators
      Remove Members
    """
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")

    for admin_id in group.admins:
        remove_usergroup_administrator(uid=admin_id, group_id=group_id)

    for member_id in group.members:
        remove_user_from_usergroup(uid=member_id, group_id=group_id)


@export
def delete_usergroup(group_id: int, purge: bool=True, **kwargs) -> None:
    if purge:
        _purge_usergroup(group_id)

    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    db.DBSession.delete(group)
    db.DBSession.flush()


@export
def delete_organisation(org_id: int, **kwargs) -> None:
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == org_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {org_id}")

    groups = get_groups_by_organisation_id(org.id)
    for group in groups:
        delete_usergroup(group.id, purge=True)
    db.DBSession.delete(org)
    db.DBSession.flush()


@export
def get_groups_by_organisation(organisation: Organisation, **kwargs) -> List[UserGroup]:
    return get_groups_by_organisation_id(organisation.id)


@export
def get_groups_by_organisation_id(organisation_id: int, **kwargs) -> List[UserGroup]:
    groups = db.DBSession.query(UserGroup).filter(UserGroup.organisation_id == organisation_id).all()
    return groups


@export
def get_organisation_group(organisation_id: int, groupname: str, **kwargs) -> UserGroup:
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
    orgs = db.DBSession.query(Organisation).all()
    return orgs


@export
def add_user_to_organisation(uid: int, org_id: int, **kwargs) -> None:
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == org_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {org_id}")

    om = OrganisationMembers(user_id=uid, organisation_id=org_id)
    org._members.append(om)
    db.DBSession.flush()
    # Add User to org's Everyone...
    eo = get_organisation_group(org_id, Organisation.everyone)
    add_user_to_usergroup(uid, eo.id)


@export
def get_usergroup_by_id(group_id: int, **kwargs) -> UserGroup:
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    return group


@export
def add_user_to_usergroup(uid: int, group_id: int, **kwargs) -> None:
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
def remove_user_from_usergroup(uid: int, group_id: int, **kwargs) -> None:
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
def add_users_to_usergroup(user_ids: Sequence[int], group_id: int, **kwargs) -> None:
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
    if not is_usergroup_member(uid, from_gid):
        raise HydraError(f"User {uid=} is not a member of UserGroup {from_gid=}")

    if is_usergroup_member(uid, to_gid):
        raise HydraError(f"User {uid=} is already a member of UserGroup {to_gid=}")

    remove_user_from_usergroup(uid, from_gid)
    add_user_to_usergroup(uid, to_gid)


@export
def get_usergroup_members(group_id: int, **kwargs) -> List[User]:
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    members = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.members)).all()
    return members


@export
def add_usergroup_administrator(uid: int, group_id: int, **kwargs) -> None:
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    ga = GroupAdmins(user_id=uid, group_id=group_id)
    group._admins.append(ga)
    db.DBSession.flush()


@export
def remove_usergroup_administrator(uid: int, group_id: int, **kwargs) -> None:
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
def get_usergroup_administrators(group_id: int, **kwargs) -> List[User]:
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    admins = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.admins)).all()
    return admins


@export
def is_usergroup_administrator(uid: int, group_id: int, **kwargs) -> bool:
    return group_id in usergroups_administered_by_user(uid=uid)


@export
def is_usergroup_member(uid: int, group_id: int, **kwargs) -> bool:
    return group_id in usergroups_with_member_user(uid=uid)


@export
def usergroups_administered_by_user(uid: int, **kwargs) -> Set[int]:
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return {g.group_id for g in user.administers}


@export
def usergroups_with_member_user(uid, **kwargs):
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return {g.group_id for g in user.groups}


@export
def is_organisation_member(uid, org_id, **kwargs):
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return org_id in {o.organisation_id for o in user.organisations}


@export
def get_all_organisation_members(org_id, **kwargs):
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == org_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {org_id}")

    members = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(org.members)).all()
    return members


def _add_resource_access(res, usergroup_id, res_id, access, **kwargs):
    ra = ResourceAccess(resource=res.upper(), usergroup_id=usergroup_id, resource_id=res_id, access=int(access))
    db.DBSession.add(ra)
    db.DBSession.flush()

    return ra.access


@export
def set_resource_access(res, usergroup_id, res_id, access, **kwargs):
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
def get_resource_access_mask(res, usergroup_id, res_id, **kwargs):
    qfilter = (
        #ResourceAccess.holder == holder,
        ResourceAccess.resource == res.upper(),
        ResourceAccess.usergroup_id == usergroup_id,
        ResourceAccess.resource_id == res_id
    )

    """
    gfilter = {
        "holder": holder,
        "resource": res,
        "holder_id": holder_id,
        "resource_id": res_id
    }
    t2 = time.time()
    m1 = db.DBSession.get(ResourceAccess, gfilter).access
    t3 = time.time()


    mask = db.DBSession.query(ResourceAccess.access).filter(ResourceAccess.holder == holder,\
            ResourceAccess.resource == res, ResourceAccess.holder_id == holder_id, ResourceAccess.resource_id == res_id).scalar()
    """

    #t0 = time.time()
    mask = db.DBSession.query(ResourceAccess.access).filter(*qfilter).scalar()
    #t1 = time.time()

    return mask


def _usergroup_has_perm(perm, usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return perm & mask

@export
def usergroup_can_read(usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Read

@export
def usergroup_can_write(usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Write

@export
def usergroup_can_share(usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Share

@export
def any_usergroup_can_read(usergroup_ids, resource, resource_id, **kwargs):
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Read, ugid, resource, resource_id):
            return True

    return False

@export
def any_usergroup_can_write(usergroup_ids, resource, resource_id, **kwargs):
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Write, ugid, resource, resource_id):
            return True

    return False

@export
def any_usergroup_can_share(usergroup_ids, resource, resource_id, **kwargs):
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Share, ugid, resource, resource_id):
            return True

    return False

@export
def user_has_permission_by_membership(uid, perm, resource, resource_id, **kwargs):
    groups = usergroups_with_member_user(uid)
    for group in groups:
        if _usergroup_has_perm(perm, group.id, resource, resource_id):
            return True

    return False

@export
def get_permissions_map(**kwargs):
    return {p.name: p.value for p in Perm}

@export
def get_default_organisation_usergroup_name(**kwargs):
    return Organisation.everyone

def _flatten_children(p):
    hier = [p.id]
    if getattr(p, "projects", None):  # null if empty or not present
        for pp in p.projects:
            hier += _flatten_children(pp)

    return hier

@export
def get_all_usergroup_projects(group_id, include_children=False, **kwargs):
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
def get_all_usergroup_networks(group_id, **kwargs):
    visible_projects = get_all_usergroup_projects(group_id, include_children=True, user_id=kwargs["user_id"])

    net_ids = set()
    for project_id in visible_projects:
        networks = get_networks(project_id, user_id=kwargs["user_id"])
        net_ids.update({net.id for net in networks})

    return net_ids

@export
def get_all_user_projects(uid, **kwargs):
    groups = usergroups_with_member_user(uid)
    user_projects = set()
    for group in groups:
        user_projects.update(get_all_usergroup_projects(group, include_children=True, user_id=kwargs["user_id"]))

    return user_projects


if __name__ == "__main__":
    pass
