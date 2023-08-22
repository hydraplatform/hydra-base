import pytest

from hydra_base.lib.objects import JSONObject


@pytest.fixture
def organisation(client):
    return client.add_organisation("PyTest fixture organisation")

@pytest.fixture
def usergroup(client):
    group = client.add_usergroup("PyTest fixture group")
    yield group
    client.delete_usergroup(group.id, purge=True)

@pytest.fixture
def administrator():
    pass


class TestUserGroups():

    def test_organisation_instance(self, client):
        org = client.add_organisation("Main organisation")

        assert org.name == "Main organisation"
        assert isinstance(org.id, int)

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

    def test_organisation_groups(self, client, organisation):
        group = client.add_usergroup("PyTest group", organisation)
        org_groups = client.get_groups_by_organisation_id(organisation_id=organisation.id)

        assert isinstance(org_groups, list)  # Return is a list...
        assert len(org_groups) == 1  # ...containing one element...
        assert org_groups[0].organisation_id == organisation.id  # ...and has the correct parent org.

    def test_organisation_user_membership(self, client, organisation):
        user_id = client.user_id
        assert not client.is_organisation_member(uid=user_id, org_id=organisation.id)
        client.add_user_to_organisation(uid=user_id, org_id=organisation.id)
        assert client.is_organisation_member(uid=user_id, org_id=organisation.id)

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
