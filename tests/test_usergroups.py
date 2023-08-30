import enum
import random
import sys

import pytest

from hydra_base.exceptions import HydraError
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
        everyone = client.get_default_organisation_usergroup_name()  # Org has default UserGroup
        assert isinstance(everyone, str)
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

    def test_disallow_repeat_addition(self, client, usergroup):
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        # Repeat addition of same User...
        with pytest.raises(HydraError):
            client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)

    def test_remove_group_member(self, client, usergroup):
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        client.remove_user_from_usergroup(uid=user_id, group_id=usergroup.id)

        assert not client.is_usergroup_member(uid=user_id, group_id=usergroup.id)

    def test_transfer_member_between_groups(self, client, usergroup):
        user_id = client.user_id
        from_group = usergroup
        to_group = client.add_usergroup("Destination group")

        client.add_user_to_usergroup(uid=user_id, group_id=from_group.id)
        assert client.is_usergroup_member(uid=user_id, group_id=from_group.id)
        assert not client.is_usergroup_member(uid=user_id, group_id=to_group.id)

        client.transfer_user_between_usergroups(uid=user_id, from_gid=from_group.id, to_gid=to_group.id)
        assert not client.is_usergroup_member(uid=user_id, group_id=from_group.id)
        assert client.is_usergroup_member(uid=user_id, group_id=to_group.id)

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
        _ = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=Perm.Read)
        mask = client.get_resource_access_mask(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id)

        assert mask & Perm.Read
        assert not mask & Perm.Write

    def test_check_resource_access_helpers(self, client, usergroup, network_with_data):
        ref_key = "NETWORK"
        set_permission = Perm.Read | Perm.Share
        _ = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=set_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)

    def test_check_permission_change(self, client, usergroup, network_with_data):
        ref_key = "NETWORK"
        before_permission = Perm.Read | Perm.Share | Perm.Write
        after_permission = Perm.Read

        _ = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=before_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)

        _ = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=after_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)

    def test_get_all_usergroup_projects_direct(self, client, usergroup):
        perm0 = Perm.Read | Perm.Write
        perm1 = Perm.Share
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=1234, access=perm0)
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=5678, access=perm1)
        visible = client.get_all_usergroup_projects(usergroup.id)

        assert len(visible) == 1
        assert visible[0] == 1234

    def test_get_all_usergroup_projects_children(self, client, usergroup):
        parent_proj = JSONObject({})
        parent_proj.name = "Parent"
        pproj = client.add_project(parent_proj)

        child_1 = JSONObject({})
        child_1.name = "Child Project 01"
        child_1.parent_id = pproj.id  # Parent -> child_1
        c1proj = client.add_project(child_1)

        child_2 = JSONObject({})
        child_2.name = "Child Project 02"
        child_2.parent_id = c1proj.id  # Parent -> child_1 -> child_2
        c2proj = client.add_project(child_2)

        child_3 = JSONObject({})
        child_3.name = "Child Project 03"
        child_3.parent_id = c2proj.id  # Parent -> child_1 -> child_2 -> child_3
        c3proj = client.add_project(child_3)

        child_4 = JSONObject({})
        child_4.name = "Parent Leaf Child Project"
        child_4.parent_id = pproj.id  # Parent -> child_4
        c4proj = client.add_project(child_4)

        child_5 = JSONObject({})
        child_5.name = "Floating Leaf Child Project"
        # Randomly associate a child with either a Child&Parent or Child&Leaf project
        floatleaf_parent = random.choice((c1proj, c2proj, c4proj))
        child_5.parent_id = floatleaf_parent.id  # <floatleaf_parent> -> child_5
        c5proj = client.add_project(child_5)

        # Set a permission including Read on the Parent only, should be inherited by all Children
        perm0 = Perm.Read | Perm.Write
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=pproj.id, access=perm0)
        visible = client.get_all_usergroup_projects(usergroup.id, include_children=True)

        for proj in (pproj, c1proj, c2proj, c3proj, c4proj, c5proj):
            assert proj.id in visible
        for proj in reversed((pproj, c1proj, c2proj, c3proj, c4proj, c5proj)):
            client.delete_project(proj.id)

    def test_get_all_usergroup_networks(self, client, usergroup, networkmaker):
        parent_proj = JSONObject({})
        parent_proj.name = f"Parent {id(parent_proj)}"
        pproj = client.add_project(parent_proj)

        child_1 = JSONObject({})
        child_1.name = f"Child Project 01 {id(child_1)}"
        child_1.parent_id = pproj.id  # Parent -> child_1
        c1proj = client.add_project(child_1)

        net1 = networkmaker.create(project_id=pproj.id)  # Parent -> net_1
        net2 = networkmaker.create(project_id=c1proj.id) # Parent -> child_1 -> net_2

        perm0 = Perm.Read | Perm.Write
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=pproj.id, access=perm0)

        nets = client.get_all_usergroup_networks(group_id=usergroup.id)
        for net in (net1, net2):
            assert net.id in nets

        client.delete_project(c1proj.id)
        client.delete_project(pproj.id)

    def test_get_user_projects(self, client, usergroup):
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)

        parent_proj = JSONObject({})
        parent_proj.name = "Parent"
        pproj = client.add_project(parent_proj)

        child_1 = JSONObject({})
        child_1.name = "Child Project 01"
        child_1.parent_id = pproj.id  # Parent -> child_1
        c1proj = client.add_project(child_1)

        perm0 = Perm.Read | Perm.Write
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=pproj.id, access=perm0)

        user_projects = client.get_all_user_projects(uid=user_id)
        for proj in (pproj, c1proj):
            assert proj.id in user_projects

        client.delete_project(c1proj.id)
        client.delete_project(pproj.id)
