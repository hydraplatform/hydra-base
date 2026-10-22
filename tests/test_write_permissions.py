"""
    Tests that functions which modify a network, its scenarios or its
    resource groups refuse a user who only has read access, and still
    permit a user with write access.

    UserD (role 'developer', so it holds the edit_* role permissions) is given
    view-only access to a network in a project which is shared with nobody,
    so any refusal below comes from the resource-level check.
"""
import pytest
import hydra_base as hb
from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import HydraError


@pytest.fixture()
def shared_net(client, projectmaker, networkmaker):
    """
        A network in a private project, shared view-only with UserD
        and with write access with UserC.
    """
    proj = projectmaker.create(share=False)
    net = networkmaker.create(project_id=proj.id)
    client.share_network(net.id, ["UserD"], 'Y', 'N')
    client.share_network(net.id, ["UserC"], 'N', 'N')
    return net


def _as(client, username):
    client.login(username, 'password')


@pytest.fixture()
def as_viewer(client):
    _as(client, 'UserD')
    yield
    client.login('root', '')


@pytest.fixture()
def as_writer(client):
    _as(client, 'UserC')
    yield
    client.login('root', '')


class TestViewOnlyRefused:

    def test_update_network(self, client, shared_net, as_viewer):
        net = JSONObject(client.get_network(shared_net.id))
        net.name = 'renamed by viewer'
        with pytest.raises(HydraError):
            client.update_network(net)
        client.login('root', '')
        assert client.get_network(shared_net.id, summary=True).name == shared_net.name

    def test_update_network_move_to_unwritable_project(self, client, shared_net, projectmaker):
        other = projectmaker.create(share=False)
        _as(client, 'UserC')
        try:
            net = JSONObject(client.get_network(shared_net.id))
            net.project_id = other.id
            with pytest.raises(HydraError):
                client.update_network(net)
        finally:
            client.login('root', '')

    def test_move_network_to_unwritable_project(self, client, shared_net, projectmaker):
        other = projectmaker.create(share=False)
        _as(client, 'UserC')
        try:
            with pytest.raises(HydraError):
                client.move_network(shared_net.id, other.id)
        finally:
            client.login('root', '')

    def test_update_scenario(self, client, shared_net, as_viewer):
        scenario = JSONObject(client.get_scenario(shared_net.scenarios[0].id))
        scenario.name = 'renamed by viewer'
        with pytest.raises(HydraError):
            client.update_scenario(scenario)

    def test_create_child_scenario(self, client, shared_net, as_viewer):
        with pytest.raises(HydraError):
            client.create_child_scenario(shared_net.scenarios[0].id, 'child')

    def test_clone_scenario(self, client, shared_net, as_viewer):
        with pytest.raises(HydraError):
            client.clone_scenario(shared_net.scenarios[0].id)

    def test_lock_and_unlock_scenario(self, client, shared_net, as_viewer):
        with pytest.raises(HydraError):
            client.lock_scenario(shared_net.scenarios[0].id)
        with pytest.raises(HydraError):
            client.unlock_scenario(shared_net.scenarios[0].id)

    def test_resourcegroup_functions(self, client, shared_net, as_viewer):
        group = shared_net.resourcegroups[0]
        scenario = shared_net.scenarios[0]
        item = scenario.resourcegroupitems[0]

        new_group = JSONObject(dict(name='viewer group', description='x', status='A'))
        with pytest.raises(HydraError):
            client.add_resourcegroup(new_group, shared_net.id)

        group.name = 'renamed by viewer'
        with pytest.raises(HydraError):
            client.update_resourcegroup(group)

        with pytest.raises(HydraError):
            client.delete_resourcegroup(group.id)

        with pytest.raises(HydraError):
            client.add_resourcegroupitem(
                JSONObject(dict(group_id=item.group_id, ref_key=item.ref_key,
                                ref_id=item.ref_id)), scenario.id)

        with pytest.raises(HydraError):
            client.delete_resourcegroupitem(item.id)

        with pytest.raises(HydraError):
            client.delete_resourcegroupitems(scenario.id, [item.id])

        with pytest.raises(HydraError):
            client.empty_group(group.id, scenario.id)


class TestWriteAccessPermitted:

    def test_owner_can_modify(self, client, shared_net):
        """The creator (root) still has full access"""
        net = JSONObject(client.get_network(shared_net.id))
        net.description = 'updated by owner'
        assert client.update_network(net).description == 'updated by owner'

        scenario = JSONObject(client.get_scenario(shared_net.scenarios[0].id))
        scenario.description = 'updated by owner'
        client.update_scenario(scenario)

        client.create_child_scenario(shared_net.scenarios[0].id, 'child')
        client.clone_scenario(shared_net.scenarios[0].id)

        group = shared_net.resourcegroups[0]
        group.description = 'updated by owner'
        assert client.update_resourcegroup(group).description == 'updated by owner'

        new_group = client.add_resourcegroup(
            JSONObject(dict(name='owner group', description='x', status='A')), shared_net.id)
        client.delete_resourcegroup(new_group.id)

    def test_write_share_can_modify(self, client, shared_net, as_writer):
        net = JSONObject(client.get_network(shared_net.id))
        net.description = 'updated by writer'
        assert client.update_network(net).description == 'updated by writer'

        scenario = JSONObject(client.get_scenario(shared_net.scenarios[0].id))
        scenario.description = 'updated by writer'
        client.update_scenario(scenario)

        client.create_child_scenario(shared_net.scenarios[0].id, 'writer child')
        client.clone_scenario(shared_net.scenarios[0].id)

        group = shared_net.resourcegroups[0]
        group.description = 'updated by writer'
        client.update_resourcegroup(group)
