"""
    Rule code is run by job runners, so writing it needs the permission `author_rules`, which
    only the `rule_author` role (and admins) hold. The ordinary rule permissions are not enough.
"""
import pytest

import hydra_base as hb
from hydra_base.db.model import Role
from hydra_base.lib.objects import JSONObject


def _as(client, username):
    client.login(username, 'password')


def _rule(net, name="SEC08 rule", value="x = 1"):
    return JSONObject({'name': name, 'value': value, 'ref_key': 'NETWORK', 'ref_id': net.id, 'types': []})


@pytest.fixture()
def net(client, projectmaker, networkmaker):
    """ A network that the developer UserC and the author may both edit, so only the permission differs """
    proj = projectmaker.create(share=False)
    net = networkmaker.create(project_id=proj.id)
    client.share_network(net.id, ["UserC"], 'N', 'N')
    client.testutils.create_user("RuleAuthor", role='developer')
    client.share_network(net.id, ["RuleAuthor"], 'N', 'N')
    return net


@pytest.fixture()
def author(client):
    """ A developer who has also been given the rule_author role """
    user = client.testutils.create_user("RuleAuthor", role='developer')
    role = JSONObject(client.get_role_by_code('rule_author'))
    if role.id not in [r.id for r in map(JSONObject, client.get_user_roles(user.id))]:
        client.set_user_role(new_user_id=user.id, role_id=role.id)
    return user


class TestWritingRules:

    def test_a_developer_cannot_add_a_rule(self, client, net):
        _as(client, 'UserC')
        try:
            with pytest.raises(hb.exceptions.PermissionError, match="author_rules"):
                client.add_rule(rule=_rule(net))
        finally:
            client.login('root', '')

    def test_a_rule_author_can_add_and_update_a_rule(self, client, net, author):
        _as(client, 'RuleAuthor')
        try:
            rule = JSONObject(client.add_rule(rule=_rule(net)))
            rule.value = "x = 2"
            rule.ref_key = 'NETWORK'
            updated = JSONObject(client.update_rule(rule=rule))
            assert updated.value == "x = 2"
        finally:
            client.login('root', '')

    def test_an_admin_can_add_a_rule(self, client, net):
        assert client.add_rule(rule=_rule(net)).id

    def test_a_developer_cannot_change_a_rule_an_author_wrote(self, client, net, author):
        _as(client, 'RuleAuthor')
        try:
            rule = JSONObject(client.add_rule(rule=_rule(net)))
        finally:
            client.login('root', '')
        _as(client, 'UserC')
        try:
            rule.value = "import os"
            rule.ref_key = 'NETWORK'
            with pytest.raises(hb.exceptions.PermissionError, match="author_rules"):
                client.update_rule(rule=rule)
            with pytest.raises(hb.exceptions.PermissionError, match="author_rules"):
                client.activate_rule(rule.id)
        finally:
            client.login('root', '')

    def test_a_developer_can_clone_a_rule_an_author_wrote(self, client, net, author, projectmaker, networkmaker):
        """ A clone is a copy of code that an author wrote; it is not a way to write new code """
        _as(client, 'RuleAuthor')
        try:
            rule = JSONObject(client.add_rule(rule=_rule(net, value="x = 5")))
        finally:
            client.login('root', '')
        other = networkmaker.create(project_id=projectmaker.create(share=False).id)
        client.share_network(other.id, ["UserC"], 'N', 'N')
        _as(client, 'UserC')
        try:
            cloned = JSONObject(client.clone_rule(rule.id, target_ref_key='NETWORK', target_ref_id=other.id))
            assert cloned.value == "x = 5"
            cloned.ref_key = 'NETWORK'
            cloned.value = "x = 6"
            with pytest.raises(hb.exceptions.PermissionError, match="author_rules"):
                client.update_rule(rule=cloned)       # the copy cannot be edited either
        finally:
            client.login('root', '')

    def test_the_developer_role_does_not_carry_the_permission(self, client):
        for code in ('developer', 'dev', 'modeller', 'manager', 'grad', 'decision'):
            role = hb.db.DBSession.query(Role).filter(Role.code == code).one()
            assert 'author_rules' not in [p.code for p in role.permissions], code
        role = hb.db.DBSession.query(Role).filter(Role.code == 'rule_author').one()
        assert 'author_rules' in [p.code for p in role.permissions]

    def test_a_developer_can_still_read_rules(self, client, net):
        client.add_rule(rule=_rule(net))
        _as(client, 'UserC')
        try:
            assert len(client.get_resource_rules('NETWORK', net.id)) == 1
        finally:
            client.login('root', '')


class TestNoListOfAuthors:

    def test_there_is_no_call_that_lists_who_may_write_rules(self, client):
        for name in ('get_rule_authors', 'get_trusted_rule_authors'):
            assert not hasattr(hb.lib.rules, name)
