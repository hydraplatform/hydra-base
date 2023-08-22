from hydra_base.db.model.usergroups import (
    UserGroup,
    Organisation,
    GroupMembers,
    GroupAdmins,
    OrganisationMembers
)

from hydra_base.db.model import (
    User
)

from hydra_base import (
    db,
    config
)

__all__ = (
    "add_organisation",
    "add_usergroup",
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
    "is_organisation_member"
)


def add_organisation(name, **kwargs):
    org = Organisation(name=name)
    db.DBSession.add(org)
    db.DBSession.flush()

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
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
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


def get_groups_by_organisation(organisation, **kwargs):
    return get_groups_by_organisation_id(organisation.id)


def get_groups_by_organisation_id(organisation_id, **kwargs):
    groups = db.DBSession.query(UserGroup).filter(UserGroup.organisation_id == organisation_id)
    return groups


def get_usergroup_by_id(group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    return group


def add_user_to_organisation(uid, org_id, **kwargs):
    org = db.DBSession.query(Organisation).filter(Organisation.id == org_id).one()
    om = OrganisationMembers(user_id=uid, organisation_id=org_id)
    org._members.append(om)
    db.DBSession.flush()


def add_user_to_usergroup(uid, group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    gm = GroupMembers(user_id=uid, group_id=group_id)
    group._members.append(gm)
    db.DBSession.flush()


def remove_user_from_usergroup(uid, group_id, **kwargs):
    gm = db.DBSession.query(GroupMembers).filter(GroupMembers.user_id == uid, GroupMembers.group_id == group_id).one()
    db.DBSession.delete(gm)
    db.DBSession.flush()


def add_users_to_usergroup(user_ids, group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    for uid in user_ids:
        gm = GroupMembers(user_id=uid, group_id=group_id)
        group._members.append(gm)

    db.DBSession.flush()


def get_usergroup_members(group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    members = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.members)).all()
    return members


def add_usergroup_administrator(uid, group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    ga = GroupAdmins(user_id=uid, group_id=group_id)
    group._admins.append(ga)
    db.DBSession.flush()


def remove_usergroup_administrator(uid, group_id, **kwargs):
    ga = db.DBSession.query(GroupAdmins).filter(GroupAdmins.group_id == group_id, GroupAdmins.user_id == uid).one()
    db.DBSession.delete(ga)
    db.DBSession.flush()


def get_usergroup_administrators(group_id, **kwargs):
    group = db.DBSession.query(UserGroup).filter(UserGroup.id == group_id).one()
    admins = db.DBSession.query(User.id, User.username, User.display_name).filter(User.id.in_(group.admins)).all()
    return admins


def is_usergroup_administrator(uid, group_id, **kwargs):
    return group_id in usergroups_administered_by_user(uid=uid)


def is_usergroup_member(uid, group_id, **kwargs):
    return group_id in usergroups_with_member_user(uid=uid)


def usergroups_administered_by_user(uid, **kwargs):
    user = db.DBSession.query(User).filter(User.id == uid).one()
    return {g.group_id for g in user.administers}


def usergroups_with_member_user(uid, **kwargs):
    user = db.DBSession.query(User).filter(User.id == uid).one()
    return {g.group_id for g in user.groups}

def is_organisation_member(uid, org_id, **kwargs):
    user = db.DBSession.query(User).filter(User.id == uid).one()
    return org_id in {o.organisation_id for o in user.organisations}


if __name__ == "__main__":
    pass
