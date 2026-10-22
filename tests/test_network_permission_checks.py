"""
    Tests for the helpers which let a caller (e.g. HWI) ask about edit access
    to a network, and find the network a resource belongs to, without
    querying the database itself.

    UserD is given view-only access to a network in a private project, and
    UserC write access, so any difference between them comes from the
    network-level check.
"""
import pytest
from hydra_base.lib.objects import JSONObject


@pytest.fixture()
def shared_net(client, projectmaker, networkmaker):
    proj = projectmaker.create(share=False)
    net = networkmaker.create(project_id=proj.id)
    client.share_network(net.id, ["UserD"], 'Y', 'N')
    client.share_network(net.id, ["UserC"], 'N', 'N')
    return net


@pytest.fixture()
def unshared_net(client, projectmaker, networkmaker):
    proj = projectmaker.create(share=False)
    return networkmaker.create(project_id=proj.id)


def _as(client, username):
    client.login(username, 'password')


class TestCheckNetworkWritePermission:

    def test_creator_can_edit(self, client, shared_net):
        assert client.check_network_write_permission(shared_net.id) is True

    def test_write_share_can_edit(self, client, shared_net):
        _as(client, 'UserC')
        try:
            assert client.check_network_write_permission(shared_net.id) is True
        finally:
            client.login('root', '')

    def test_view_only_cannot_edit(self, client, shared_net):
        _as(client, 'UserD')
        try:
            assert client.check_network_write_permission(shared_net.id) is False
        finally:
            client.login('root', '')

    def test_no_access_cannot_edit(self, client, unshared_net):
        _as(client, 'UserD')
        try:
            assert client.check_network_write_permission(unshared_net.id) is False
        finally:
            client.login('root', '')

    def test_unknown_network(self, client):
        assert client.check_network_write_permission(99999999) is False

    def test_is_admin_passed_through(self, client, unshared_net):
        _as(client, 'UserD')
        try:
            assert client.check_network_write_permission(unshared_net.id, is_admin=True) is True
        finally:
            client.login('root', '')


class TestGetResourceNetworkId:

    def test_network(self, client, shared_net):
        assert client.get_resource_network_id('NETWORK', shared_net.id) == shared_net.id

    def test_node_link_group_scenario(self, client, shared_net):
        net = JSONObject(client.get_network(shared_net.id))
        assert client.get_resource_network_id('node', net.nodes[0].id) == net.id
        assert client.get_resource_network_id('LINK', net.links[0].id) == net.id
        assert client.get_resource_network_id('SCENARIO', net.scenarios[0].id) == net.id

    def test_resource_attr(self, client, shared_net):
        net = JSONObject(client.get_network(shared_net.id))
        node_ra = net.nodes[0].attributes[0]
        assert client.get_resource_network_id('RESOURCEATTR', node_ra.id) == net.id
        if net.attributes:
            assert client.get_resource_network_id('resourceattr', net.attributes[0].id) == net.id

    def test_unknown_or_bad_input(self, client, shared_net):
        assert client.get_resource_network_id('NODE', 99999999) is None
        assert client.get_resource_network_id('NODE', 'abc') is None
        assert client.get_resource_network_id('NODE', None) is None
        assert client.get_resource_network_id('BOGUS', 1) is None
        assert client.get_resource_network_id('RESOURCEATTR', 99999999) is None

    def test_unreadable_network_not_revealed(self, client, unshared_net):
        net = JSONObject(client.get_network(unshared_net.id))
        node_id = net.nodes[0].id
        _as(client, 'UserD')
        try:
            assert client.get_resource_network_id('NODE', node_id) is None
            assert client.get_resource_network_id('NETWORK', net.id) is None
        finally:
            client.login('root', '')

    def test_view_only_user_still_resolves(self, client, shared_net):
        net = JSONObject(client.get_network(shared_net.id))
        _as(client, 'UserD')
        try:
            assert client.get_resource_network_id('NODE', net.nodes[0].id) == net.id
        finally:
            client.login('root', '')
