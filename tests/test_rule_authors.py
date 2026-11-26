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


class TestTrustedRuleAuthors:
    """ A caller learns about the authors of the rules it can read, and nothing else """

    def _rule_by(self, client, net, username):
        _as(client, username)
        try:
            return JSONObject(client.add_rule(rule=_rule(net, name=f"by {username}")))
        finally:
            client.login('root', '')

    def test_reports_whether_the_authors_of_the_rules_may_write_rules(self, client, net, author):
        by_author = self._rule_by(client, net, 'RuleAuthor')
        by_root = JSONObject(client.add_rule(rule=_rule(net, name="by root")))
        ids = client.get_trusted_rule_authors([by_author.id, by_root.id])
        assert set(ids) == {author.id, pytest.root_user_id}

    def test_an_author_who_lost_the_role_is_not_reported(self, client, net, author):
        by_author = self._rule_by(client, net, 'RuleAuthor')
        rule = JSONObject(client.add_rule(rule=_rule(net, name="by root")))
        assert author.id in client.get_trusted_rule_authors([by_author.id])
        # a rule whose only author is a developer without the role
        assert pytest.user_c.id not in client.get_trusted_rule_authors([rule.id])

    def test_only_the_authors_of_the_rules_asked_about_are_reported(self, client, net, author):
        by_root = JSONObject(client.add_rule(rule=_rule(net)))
        ids = client.get_trusted_rule_authors([by_root.id])
        assert author.id not in ids and ids == [pytest.root_user_id]

    def test_a_user_cannot_ask_about_a_rule_they_cannot_read(self, client, net, projectmaker, networkmaker):
        proj = projectmaker.create(share=False)
        private = networkmaker.create(project_id=proj.id)
        rule = JSONObject(client.add_rule(rule=_rule(private)))
        _as(client, 'UserC')
        try:
            with pytest.raises(hb.exceptions.PermissionError):
                client.get_trusted_rule_authors([rule.id])
        finally:
            client.login('root', '')

    def test_there_is_no_list_of_all_rule_authors(self, client):
        assert not hasattr(hb.lib.rules, 'get_rule_authors')
        with pytest.raises(Exception):
            client.get_rule_authors()

    def test_no_rules_asks_nothing(self, client):
        assert client.get_trusted_rule_authors([]) == []
