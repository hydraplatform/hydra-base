import time

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

from hydra_base.exceptions import (
    HydraError,
    ResourceNotFoundError
)

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

project_max_nest_depth = int(config.get("limits", "project_max_nest_depth", 32))


__all__ = (
    "add_organisation",
    "add_usergroup",
    "delete_organisation",
    "delete_usergroup",
    "get_groups_by_organisation",
    "get_groups_by_organisation_id",
    "get_usergroup_by_id",
    "add_user_to_organisation",
    "add_user_to_usergroup",
    "remove_user_from_usergroup",
    "add_users_to_usergroup",
    "get_usergroup_members",
    "add_usergroup_administrator",
    "remove_usergroup_administrator",
    "get_usergroup_administrators",
    "is_usergroup_administrator",
    "is_usergroup_member",
    "usergroups_administered_by_user",
    "usergroups_with_member_user",
    "is_organisation_member",
    "get_all_organisations",
    "get_all_organisation_members",
    "set_resource_access",
    "get_resource_access_mask",
    "get_organisation_group",
    "usergroup_can_read",
    "usergroup_can_write",
    "usergroup_can_share",
    "get_permissions_map",
    "any_usergroup_can_read",
    "any_usergroup_can_write",
    "any_usergroup_can_share",
    "get_default_organisation_usergroup_name",
    "user_has_permission_by_membership",
    "transfer_user_between_usergroups",
    "get_all_usergroup_projects",
    "get_all_usergroup_networks"
)


def add_organisation(name, **kwargs):
    org = Organisation(name=name)
    db.DBSession.add(org)
    db.DBSession.flush()
    add_usergroup(name=Organisation.everyone, organisation=org)

    return org


def add_usergroup(name, organisation=None, **kwargs):
    oid = getattr(organisation, "id", None)
    group = UserGroup(name=name, organisation_id=oid)
    db.DBSession.add(group)
    db.DBSession.flush()

    return group


def _purge_usergroup(group_id, **kwargs):
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


def delete_usergroup(group_id, purge=True, **kwargs):
    if purge:
        _purge_usergroup(group_id)

    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    db.DBSession.delete(group)
    db.DBSession.flush()


def delete_organisation(org_id, **kwargs):
    try:
        org = db.DBSession.query(Organisation).filter(Organisation.id == org_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Organisation found with id: {org_id}")

    groups = get_groups_by_organisation_id(org.id)
    for group in groups:
        delete_usergroup(group.id, purge=True)
    db.DBSession.delete(org)
    db.DBSession.flush()


def get_groups_by_organisation(organisation, **kwargs):
    return get_groups_by_organisation_id(organisation.id)


def get_groups_by_organisation_id(organisation_id, **kwargs):
    groups = db.DBSession.query(UserGroup).filter(UserGroup.organisation_id == organisation_id).all()
    return groups


def get_organisation_group(organisation_id, groupname, **kwargs):
    qfilter = (
        UserGroup.organisation_id == organisation_id,
        UserGroup.name == groupname
    )

    try:
        group = db.DBSession.query(UserGroup).filter(*qfilter).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No Group found with {organisation_id=} {groupname=}")

    return group


def get_all_organisations(**kwargs):
    orgs = db.DBSession.query(Organisation).all()
    return orgs


def add_user_to_organisation(uid, org_id, **kwargs):
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


def get_usergroup_by_id(group_id, **kwargs):
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    return group


def add_user_to_usergroup(uid, group_id, **kwargs):
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")

    if is_usergroup_member(uid, group_id):
        raise HydraError(f"User {uid=} is already a member of UserGroup {group_id=}")

    gm = GroupMembers(user_id=uid, group_id=group_id)
    group._members.append(gm)
    db.DBSession.flush()


def remove_user_from_usergroup(uid, group_id, **kwargs):
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


def add_users_to_usergroup(user_ids, group_id, **kwargs):
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")

    for uid in user_ids:
        gm = GroupMembers(user_id=uid, group_id=group_id)
        group._members.append(gm)

    db.DBSession.flush()


def transfer_user_between_usergroups(uid, from_gid, to_gid, **kwargs):
    if not is_usergroup_member(uid, from_gid):
        raise HydraError(f"User {uid=} is not a member of UserGroup {from_gid=}")

    if is_usergroup_member(uid, to_gid):
        raise HydraError(f"User {uid=} is already a member of UserGroup {to_gid=}")

    remove_user_from_usergroup(uid, from_gid)
    add_user_to_usergroup(uid, to_gid)


def get_usergroup_members(group_id, **kwargs):
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    members = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.members)).all()
    return members


def add_usergroup_administrator(uid, group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    ga = GroupAdmins(user_id=uid, group_id=group_id)
    group._admins.append(ga)
    db.DBSession.flush()


def remove_usergroup_administrator(uid, group_id, **kwargs):
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


def get_usergroup_administrators(group_id, **kwargs):
    try:
        group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No UserGroup found with id: {group_id}")
    admins = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.admins)).all()
    return admins


def is_usergroup_administrator(uid, group_id, **kwargs):
    return group_id in usergroups_administered_by_user(uid=uid)


def is_usergroup_member(uid, group_id, **kwargs):
    return group_id in usergroups_with_member_user(uid=uid)


def usergroups_administered_by_user(uid, **kwargs):
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return {g.group_id for g in user.administers}


def usergroups_with_member_user(uid, **kwargs):
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return {g.group_id for g in user.groups}


def is_organisation_member(uid, org_id, **kwargs):
    try:
        user = db.DBSession.query(User).filter(User.id == uid).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User found with id: {uid}")
    return org_id in {o.organisation_id for o in user.organisations}


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

def usergroup_can_read(usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Read

def usergroup_can_write(usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Write

def usergroup_can_share(usergroup_id, resource, resource_id, **kwargs):
    mask = get_resource_access_mask(resource, usergroup_id, resource_id)
    return mask & Perm.Share

def any_usergroup_can_read(usergroup_ids, resource, resource_id, **kwargs):
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Read, ugid, resource, resource_id):
            return True

    return False

def any_usergroup_can_write(usergroup_ids, resource, resource_id, **kwargs):
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Write, ugid, resource, resource_id):
            return True

    return False

def any_usergroup_can_share(usergroup_ids, resource, resource_id, **kwargs):
    for ugid in usergroup_ids:
        if _usergroup_has_perm(Perm.Share, ugid, resource, resource_id):
            return True

    return False

def user_has_permission_by_membership(uid, perm, resource, resource_id, **kwargs):
    groups = usergroups_with_member_user(uid)
    for group in groups:
        if _usergroup_has_perm(perm, group.id, resource, resource_id):
            return True

    return False

def get_permissions_map(**kwargs):
    return {p.name: p.value for p in Perm}

def get_default_organisation_usergroup_name(**kwargs):
    return Organisation.everyone

def _flatten_children(p):
    hier = [p.id]
    if getattr(p, "projects", None):  # null if empty or not present
        for pp in p.projects:
            hier += _flatten_children(pp)

    return hier

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

def get_all_usergroup_networks(group_id, **kwargs):
    visible_projects = get_all_usergroup_projects(group_id, include_children=True, user_id=kwargs["user_id"])

    net_ids = set()
    for project_id in visible_projects:
        networks = get_networks(project_id, user_id=kwargs["user_id"])
        net_ids.update({net.id for net in networks})

    return net_ids


if __name__ == "__main__":
    pass
