import enum
import sys

import pytest

from hydra_base.lib.objects import JSONObject


@pytest.fixture
def organisation(client):
    org =  client.add_organisation("PyTest fixture organisation")
    yield org
    client.delete_organisation(org_id=org.id)


@pytest.fixture
def usergroup(client):
    group = client.add_usergroup("PyTest fixture group")
    yield group
    client.delete_usergroup(group_id=group.id, purge=True)


@pytest.fixture(scope="module", autouse=True)
def Permissions_Map(client):
    perm = client.get_permissions_map()
    sys.modules[__name__].Perm = enum.IntEnum("Perm", perm)



class TestUserGroups():

    def test_organisation_instance(self, client):
        org = client.add_organisation("Main organisation")

        assert org.name == "Main organisation"
        assert isinstance(org.id, int)
        client.delete_organisation(org_id=org.id)

    def test_group_instance(self, client):
        group = client.add_usergroup("PyTest group")

        assert group.name == "PyTest group"
        assert isinstance(group.id, int)

    def test_delete_empty_group(self, client):
        group = client.add_usergroup("PyTest group")
        client.delete_usergroup(group.id, purge=False)

    def test_delete_populated_group(self, client):
        user_id = client.user_id
        group = client.add_usergroup("PyTest group")
        client.add_user_to_usergroup(uid=user_id, group_id=group.id)
        client.add_usergroup_administrator(uid=user_id, group_id=group.id)

        assert client.is_usergroup_administrator(uid=user_id, group_id=group.id)
        assert client.is_usergroup_member(uid=user_id, group_id=group.id)
        client.delete_usergroup(group.id, purge=True)
        assert not client.is_usergroup_administrator(uid=user_id, group_id=group.id)
        assert not client.is_usergroup_member(uid=user_id, group_id=group.id)

    def test_get_organisations(self, client):
        before_orgs = client.get_all_organisations()
        orgnames = ("First", "Second", "Third")
        for orgname in orgnames:
            client.add_organisation(f"{orgname} organisation")
        after_orgs = client.get_all_organisations()

        assert len(before_orgs) == 0
        assert len(after_orgs) == len(orgnames)

        for org in after_orgs:
            client.delete_organisation(org.id)

    def test_organisation_groups(self, client, organisation):
        group = client.add_usergroup("PyTest group", organisation)
        org_groups = client.get_groups_by_organisation_id(organisation_id=organisation.id)

        assert isinstance(org_groups, list)  # Return is a list...
        assert len(org_groups) == 2  # ...containing two elements (Everyone + group)...
        assert org_groups[0].organisation_id == organisation.id  # ...and has the correct parent org.

    def test_organisation_user_membership(self, client, organisation):
        user_id = client.user_id
        assert not client.is_organisation_member(uid=user_id, org_id=organisation.id)
        client.add_user_to_organisation(uid=user_id, org_id=organisation.id)
        assert client.is_organisation_member(uid=user_id, org_id=organisation.id)

    def test_get_all_organisation_members(self, client, organisation):
        user_id = client.user_id
        client.add_user_to_organisation(uid=user_id, org_id=organisation.id)
        members = client.get_all_organisation_members(org_id=organisation.id)
        assert len(members) == 1

    def test_add_group_member(self, client, usergroup):
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        members = client.get_usergroup_members(group_id=usergroup.id)

        # Does the UG now have that user as a member?
        assert len(members) == 1
        assert members[0].id == user_id

        # Test the reverse relationship: is the UG among those of which the user is a member?
        assert client.is_usergroup_member(uid=user_id, group_id=usergroup.id)

    def test_remove_group_member(self, client, usergroup):
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        client.remove_user_from_usergroup(uid=user_id, group_id=usergroup.id)

        assert not client.is_usergroup_member(uid=user_id, group_id=usergroup.id)

    def test_add_group_administrator(self, client, usergroup):
        user_id = client.user_id
        client.add_usergroup_administrator(uid=user_id, group_id=usergroup.id)
        admins = client.get_usergroup_administrators(group_id=usergroup.id)

        # Does the UG now have that user as an administrator?
        assert len(admins) == 1
        assert admins[0].id == user_id

        # Test the reverse relationship: is the UG among those the user administers?
        assert client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

    def test_remove_group_administrator(self, client, usergroup):
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        client.add_usergroup_administrator(uid=user_id, group_id=usergroup.id)
        assert client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

        client.remove_usergroup_administrator(uid=user_id, group_id=usergroup.id)
        assert not client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

    def test_everyone(self, client, usergroup, organisation):
        user_id = client.user_id
        client.add_user_to_organisation(uid=user_id, org_id=organisation.id)
        eo = client.get_organisation_group(organisation_id=organisation.id, groupname="Everyone")
        eom = client.get_usergroup_members(group_id=eo.id)

        assert len(eom) == 1  # User automatically in Everyone
        assert eom[0].id == user_id

    def test_permissions_map(self, client):
        required_permissions = ("Read", "Write", "Share")
        assert issubclass(Perm, enum.IntEnum)

        for perm in required_permissions:
            assert hasattr(Perm, perm)

    def test_check_resource_access_mask(self, client, usergroup, network_with_data):
        ref_key = "NETWORK"
        ra = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=Perm.Read)
        mask = client.get_resource_access_mask(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id)

        assert mask & Perm.Read
        assert not mask & Perm.Write

    def test_check_resource_access_helpers(self, client, usergroup, network_with_data):
        ref_key = "NETWORK"
        set_permission = Perm.Read | Perm.Share
        ra = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=set_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)

    def test_check_permission_change(self, client, usergroup, network_with_data):
        ref_key = "NETWORK"
        before_permission = Perm.Read | Perm.Share | Perm.Write
        after_permission = Perm.Read

        ra = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=before_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)

        ra = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=after_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
