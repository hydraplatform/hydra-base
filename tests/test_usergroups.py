import enum
import random
import sys

import pytest

from hydra_base.exceptions import HydraError, PermissionError
from hydra_base.lib.objects import JSONObject


@pytest.fixture
def organisation(client):
    org =  client.add_organisation("PyTest fixture organisation")
    yield org
    client.delete_organisation(org_id=org.id)


@pytest.fixture
def usergroup(client, organisation):
    group = client.add_usergroup("PyTest fixture group", organisation_id=organisation.id)
    yield group
    client.delete_usergroup(group_id=group.id, purge=True)

@pytest.fixture
def non_admin_user(client):
    user_data = JSONObject({
        "username": "non_admin_user",
        "password": "password",
        "display_name": "PyTest fixture non_admin_user"
    })
    user = client.add_user(user_data)
    user.plaintext_passwd = user_data.password  # Used to client.login this user
    yield user
    client.delete_user(user.id)
    client.login("root", "")  # Restore default test user


@pytest.fixture(scope="module", autouse=True)
def Permissions_Map(client):
    perm = client.get_permissions_map()
    sys.modules[__name__].Perm = enum.IntEnum("Perm", perm)



class TestUserGroups():

    def test_organisation_instance(self, client):
        """
          Can an Organisation be created and have the expected properties?
        """
        org = client.add_organisation("Main organisation")

        assert org.name == "Main organisation"
        assert isinstance(org.id, int)
        everyone = client.get_default_organisation_usergroup_name()  # Org has default UserGroup
        assert isinstance(everyone, str)
        client.delete_organisation(org_id=org.id)

    def test_group_instance(self, client, organisation):
        """
          Can a UserGroup be created and have the expected properties?
        """
        group = client.add_usergroup("PyTest group", organisation_id=organisation.id)

        assert group.name == "PyTest group"
        assert isinstance(group.id, int)

    def test_delete_empty_group(self, client, organisation):
        """
          Can a UserGroup without members be deleted?
        """
        group = client.add_usergroup("PyTest group", organisation_id=organisation.id)
        client.delete_usergroup(group.id, purge=False)

    def test_delete_populated_group(self, client, organisation, non_admin_user):
        """
          Can a UserGroup with members and an administrator be deleted?
        """
        user_id = non_admin_user.id
        group = client.add_usergroup("PyTest group", organisation_id=organisation.id)
        client.add_user_to_usergroup(uid=user_id, group_id=group.id)
        client.add_usergroup_administrator(uid=user_id, group_id=group.id)

        assert client.is_usergroup_administrator(uid=user_id, group_id=group.id)
        assert client.is_usergroup_member(uid=user_id, group_id=group.id)
        client.delete_usergroup(group.id, purge=True)
        assert not client.is_usergroup_administrator(uid=user_id, group_id=group.id)
        assert not client.is_usergroup_member(uid=user_id, group_id=group.id)

    def test_get_organisations(self, client):
        """
          Can the set of all defined Organisations be retrieved?
        """
        before_orgs = client.get_all_organisations()
        orgnames = ("First", "Second", "Third")
        for orgname in orgnames:
            client.add_organisation(f"{orgname} organisation")
        after_orgs = client.get_all_organisations()

        assert len(before_orgs) == 0
        assert len(after_orgs) == len(orgnames)

        for org in after_orgs:
            client.delete_organisation(org.id)

    def test_get_organisation_by_id(self, client, organisation):
        """
          Can a specific Organisation be retreived by its id attr?
        """
        org = client.get_organisation_by_id(organisation_id=organisation.id)
        assert isinstance(org, JSONObject)
        assert org.name == organisation.name
        assert org.id == organisation.id

    def test_get_organisation_by_name(self, client, organisation):
        """
          Can a specific Organisation be retreived by its name attr?
        """
        org = client.get_organisation_by_name(organisation_name=organisation.name)
        assert isinstance(org, JSONObject)
        assert org.name == organisation.name
        assert org.id == organisation.id


    def test_organisation_groups(self, client, organisation):
        """
          Are UserGroups created correctly within the specified Organisation?
        """
        group = client.add_usergroup("PyTest group", organisation_id=organisation.id)
        org_groups = client.get_groups_by_organisation_id(organisation_id=organisation.id)

        assert isinstance(org_groups, list)  # Return is a list...
        assert len(org_groups) == 2  # ...containing two elements (Everyone + group)...
        assert org_groups[0].organisation_id == organisation.id  # ...and has the correct parent org.

    def test_organisation_user_membership(self, client, organisation):
        """
          Can Users be added to Organisations?
        """
        user_id = client.user_id
        assert not client.is_organisation_member(uid=user_id, org_id=organisation.id)
        client.add_user_to_organisation(uid=user_id, organisation_id=organisation.id)
        assert client.is_organisation_member(uid=user_id, org_id=organisation.id)

    def test_get_all_organisation_members(self, client, organisation, non_admin_user):
        """
          Can the set of Users who are members of an Organisation be retrieved?
        """
        user_id = client.user_id
        client.add_user_to_organisation(uid=user_id, organisation_id=organisation.id)
        client.add_user_to_organisation(uid=non_admin_user.id, organisation_id=organisation.id)
        members = client.get_all_organisation_members(org_id=organisation.id)
        assert len(members) == 2

    def test_add_group_member(self, client, usergroup, non_admin_user):
        """
          Can Users be added to an Organisation?
        """
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        client.add_user_to_usergroup(uid=non_admin_user.id, group_id=usergroup.id)
        members = client.get_usergroup_members(group_id=usergroup.id)

        # Does the UG now have that user as a member?
        assert len(members) == 2
        assert members[0].id == user_id  # Should preserve addition order
        assert members[1].id == non_admin_user.id

        # Test the reverse relationship: is the UG among those of which the user is a member?
        assert client.is_usergroup_member(uid=user_id, group_id=usergroup.id)
        assert client.is_usergroup_member(uid=non_admin_user.id, group_id=usergroup.id)

    def test_disallow_repeat_addition(self, client, usergroup):
        """
          A User cannot be added more than once to the same UserGroup
        """
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        # Repeat addition of same User...
        with pytest.raises(HydraError):
            client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)

    def test_remove_group_member(self, client, usergroup):
        """
          Can a User be removed from a UserGroup?
        """
        user_id = client.user_id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        client.remove_user_from_usergroup(uid=user_id, group_id=usergroup.id)

        assert not client.is_usergroup_member(uid=user_id, group_id=usergroup.id)

    def test_add_group_administrator(self, client, usergroup):
        """
          Can an administrator be assigned to a UserGroup?
        """
        user_id = client.user_id
        client.add_usergroup_administrator(uid=user_id, group_id=usergroup.id)
        admins = client.get_usergroup_administrators(group_id=usergroup.id)

        # Does the UG now have that user as an administrator?
        assert len(admins) == 1
        assert admins[0].id == user_id

        # Test the reverse relationship: is the UG among those the user administers?
        assert client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

    def test_remove_group_administrator(self, client, usergroup, non_admin_user):
        """
          Can a User have the position of UserGroup administrator removed?
        """
        user_id = non_admin_user.id
        client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)
        client.add_usergroup_administrator(uid=user_id, group_id=usergroup.id)
        assert client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

        client.remove_usergroup_administrator(uid=user_id, group_id=usergroup.id)
        assert not client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

    def test_everyone(self, client, usergroup, organisation):
        """
          Does each organisation define an 'Everyone' group and are organisation
          members automatically added to it?
        """
        user_id = client.user_id
        client.add_user_to_organisation(uid=user_id, organisation_id=organisation.id)
        eo = client.get_organisation_group(organisation_id=organisation.id, groupname="Everyone")
        eom = client.get_usergroup_members(group_id=eo.id)

        assert len(eom) == 1  # User automatically in Everyone
        assert eom[0].id == user_id

    def test_permissions_map(self, client):
        """
          Does the Permissions map as exported over remote connections correctly
          define the required permissions?

          See the Permissions_Map fixture above.
        """
        required_permissions = ("Read", "Write", "Share")
        assert issubclass(Perm, enum.IntEnum)

        for perm in required_permissions:
            assert hasattr(Perm, perm)

    def test_check_resource_access_mask(self, client, usergroup, network_with_data):
        """
          Can the access mask for a resource be both set and returned correctly?
        """
        ref_key = "NETWORK"
        _ = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=Perm.Read)
        mask = client.get_resource_access_mask(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id)

        assert mask & Perm.Read
        assert not mask & Perm.Write

    def test_check_resource_access_helpers(self, client, usergroup, network_with_data):
        """
          Do the convenience functions for testing UserGroup permissions return
          the expected results?
        """
        ref_key = "NETWORK"
        set_permission = Perm.Read | Perm.Share
        _ = client.set_resource_access(res=ref_key, usergroup_id=usergroup.id, res_id=network_with_data.id, access=set_permission)
        assert client.usergroup_can_read(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert client.usergroup_can_share(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)
        assert not client.usergroup_can_write(usergroup_id=usergroup.id, resource=ref_key, resource_id=network_with_data.id)

    def test_check_permission_change(self, client, usergroup, network_with_data):
        """
          Are UserGroup permissions changes correctly detected by the helper functions?
        """
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
        """
          Are direct (no-parent, leaf) Projects with read permission visible to a UserGroup?
          Are such projects without read permission not visible?
        """
        perm0 = Perm.Read | Perm.Write
        perm1 = Perm.Share
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=1234, access=perm0)
        client.set_resource_access(res="project", usergroup_id=usergroup.id, res_id=5678, access=perm1)
        visible = client.get_all_usergroup_projects(usergroup.id)

        assert len(visible) == 1
        assert visible[0] == 1234

    def test_get_all_usergroup_projects_children(self, client, usergroup):
        """
          Does the Project visibility lookup produce the correct results for
          an arbitrarily-structured hierarchy?

          Permissions set on the root Project should be inherited by every
          child Project, irrespective of it being a direct child or indirect
          child and being a leaf or parent.
        """
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
        """
          The Networks visible to a UserGroup should include those in Projects
          which have been made visible directly and those in Projects which
          have inherited visibility.
        """
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
        """
          The Projects visible to a particular User should include the full
          hierarchy of Projects which inherit read permission.
        """
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

    def test_organisation_administrator(self, client, organisation, non_admin_user):
        """
          Are Users not Organisation administrators until added, and does this
          automatically confer Organisation.everyone UesrGroup admin status?
        """
        user_id = non_admin_user.id
        assert not client.is_organisation_administrator(uid=user_id, org_id=organisation.id)
        client.add_organisation_administrator(uid=user_id, organisation_id=organisation.id)
        assert client.is_organisation_administrator(uid=user_id, org_id=organisation.id)

        # Converse: Organisation 'Everyone' group should appear in groups administered by User...
        eo = client.get_organisation_group(organisation_id=organisation.id, groupname="Everyone")
        admin_groups = client.usergroups_administered_by_user(uid=user_id)
        assert eo.id in admin_groups

    def test_usergroup_operations_require_permissions(self, client, usergroup, non_admin_user):
        """
          Operations modifying UserGroup membership and admin permissions must
          require UserGroup administrator privilege.
        """
        user_id = non_admin_user.id
        assert not client.is_usergroup_administrator(uid=user_id, group_id=usergroup.id)

        uid, sid = client.login(non_admin_user.username, non_admin_user.plaintext_passwd)

        with pytest.raises(PermissionError):
            client.add_usergroup_administrator(uid=user_id, group_id=usergroup.id)

        with pytest.raises(PermissionError):
            client.remove_usergroup_administrator(uid=user_id, group_id=usergroup.id)

        with pytest.raises(PermissionError):
            client.add_user_to_usergroup(uid=user_id, group_id=usergroup.id)

        with pytest.raises(PermissionError):
            client.remove_user_from_usergroup(uid=user_id, group_id=usergroup.id)

    def test_organisation_operations_require_permissions(self, client, organisation, non_admin_user):
        """
          Operations modifying Organisation membership and admin permissions must
          require Organisation administrator privilege.
        """
        user_id = non_admin_user.id
        assert not client.is_organisation_administrator(uid=user_id, org_id=organisation.id)

        uid, sid = client.login(non_admin_user.username, non_admin_user.plaintext_passwd)

        with pytest.raises(PermissionError):
            client.add_user_to_organisation(uid=user_id, organisation_id=organisation.id)

        with pytest.raises(PermissionError):
            client.add_organisation_administrator(uid=user_id, organisation_id=organisation.id)

    def test_organisation_project_visibility(self, client, organisation):
        """
          Are projects which have been made visible to an Organisation among that
          Organisation's 'all projects'?  This should include inherited visibility.
        """
        proj = JSONObject({})
        proj.name = "Organisation Parent Project"
        hproj = client.add_project(proj)

        child_1 = JSONObject({})
        child_1.name = "Child Project 01"
        child_1.parent_id = hproj.id  # OPP -> child_1
        c1proj = client.add_project(child_1)

        orig_ids = client.get_all_organisation_projects(organisation_id=organisation.id)
        assert not orig_ids
        # Make only Parent project visible to Organisation...
        client.make_project_visible_to_organisation(organisation_id=organisation.id, project_id=hproj.id)
        project_ids = client.get_all_organisation_projects(organisation_id=organisation.id)

        # ...but visibility is inherited by children
        for p in (hproj, c1proj):
            assert p.id in project_ids

    def test_transfer_user_between_usergroups(self, client, organisation):
        from_group = client.add_usergroup("From UserGroup", organisation_id=organisation.id)
        to_group = client.add_usergroup("To UserGroup", organisation_id=organisation.id)
        user_id = client.user_id

        assert not client.is_usergroup_member(uid=user_id, group_id=from_group.id)
        client.add_user_to_usergroup(uid=user_id, group_id=from_group.id)
        assert client.is_usergroup_member(uid=user_id, group_id=from_group.id)

        assert not client.is_usergroup_member(uid=user_id, group_id=to_group.id)
        client.transfer_user_between_usergroups(uid=user_id, from_gid=from_group.id, to_gid=to_group.id)
        assert client.is_usergroup_member(uid=user_id, group_id=to_group.id)

    def test_delete_organisation_requires_hydra_admin(self, client, organisation, non_admin_user):
        uid, sid = client.login(non_admin_user.username, non_admin_user.plaintext_passwd)
        with pytest.raises(PermissionError):
            client.delete_organisation(org_id=organisation.id)
