import pytest

from hydra_base.lib.objects import JSONObject


def _shape(projects):
    """Reduce a project list to what the summary form must reproduce"""
    return {
        p.id: (
            bool(p.get('nav_only')),
            sorted(n.id for n in p.networks),
            sorted(c.id for c in (p.projects or [])),
            sorted(o.user_id for o in p.owners),
        )
        for p in projects
    }


class TestProjectSummary:
    """
        get_projects(summary=True) must report the same projects, network
        counts and sub-project counts as the full form
    """

    def _assert_parity(self, client, user_id):
        full = client.get_projects(user_id)
        summary = client.get_projects(user_id, summary=True)
        assert _shape(summary) == _shape(full)
        return full, summary

    def test_summary_matches_full_for_owner(self, client, projectmaker, networkmaker):
        proj = projectmaker.create()
        subproj = projectmaker.create(parent_id=proj.id)
        subsubproj = projectmaker.create(parent_id=subproj.id)
        networkmaker.create(project_id=proj.id)
        networkmaker.create(project_id=proj.id)
        networkmaker.create(project_id=subproj.id)
        networkmaker.create(project_id=subsubproj.id)

        full, summary = self._assert_parity(client, client.user_id)

        top = next(p for p in summary if p.id == proj.id)
        assert len(top.networks) == 2
        assert [c.id for c in top.projects] == [subproj.id]
        assert all('id' in n and 'name' in n for n in top.networks)

    def test_summary_drops_heavy_contents(self, client, projectmaker, networkmaker):
        proj = projectmaker.create()
        projectmaker.create(parent_id=proj.id)
        networkmaker.create(project_id=proj.id)

        summary = client.get_projects(client.user_id, summary=True)
        top = next(p for p in summary if p.id == proj.id)
        assert 'scenarios' not in top.networks[0]
        assert 'owners' not in top.networks[0]
        assert 'projects' not in top.projects[0]

    def test_summary_matches_full_for_shared_user(self, client, projectmaker, networkmaker):
        """A user who is shared a sub-project only sees its ancestors as nav-only projects"""
        owner_id = client.user_id
        proj = projectmaker.create(share=False)
        subproj = projectmaker.create(share=False, parent_id=proj.id)
        sub2 = projectmaker.create(share=False, parent_id=proj.id)
        networkmaker.create(project_id=proj.id)
        networkmaker.create(project_id=subproj.id)
        networkmaker.create(project_id=sub2.id)

        client.share_project(subproj.id, ['UserC'], False, False)

        full, summary = self._assert_parity(client, pytest.user_c.id)
        assert any(p.get('nav_only') for p in summary)

        #the owner is unaffected by the sharing
        client.user_id = owner_id
        self._assert_parity(client, owner_id)

    def test_summary_matches_full_when_project_shared_directly(self, client, projectmaker, networkmaker):
        owner_id = client.user_id
        proj = projectmaker.create(share=False)
        subproj = projectmaker.create(share=False, parent_id=proj.id)
        networkmaker.create(project_id=proj.id)
        networkmaker.create(project_id=subproj.id)

        client.share_project(proj.id, ['UserC'], False, False)

        full, summary = self._assert_parity(client, pytest.user_c.id)
        shared = next(p for p in summary if p.id == proj.id)
        assert [c.id for c in shared.projects] == [subproj.id]

        client.user_id = owner_id
