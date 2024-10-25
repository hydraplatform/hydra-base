#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
import datetime
import logging
import copy
import pytest

from hydra_base import HydraError
from hydra_base.lib.objects import JSONObject

log = logging.getLogger(__name__)

class TestProjectInheritance:
    """
        Test for working with project inheritance in Hydra
    """

    def test_add_child_project(self, client):
        """
            Test adding a child project to a project
        """
        parentproject = JSONObject({})
        parentproject.name = 'Test Parent %s'%(datetime.datetime.now())

        new_project_i = client.add_project(parentproject)


        childproject = JSONObject({})
        childproject.name = 'Test Child %s'%(datetime.datetime.now())
        childproject.parent_id = new_project_i.id

        child_project_j = client.add_project(childproject)

        parentproject_j = client.get_project(new_project_i.id)

        assert len(parentproject_j.projects) == 1
        assert parentproject_j.projects[0].id == child_project_j.id

    def test_move_project_with_update(self, client):
        """
            Test adding a child project to a project
        """
        #Add another parent that the child will be moved to
        targetproject = JSONObject({})
        targetproject.name = 'Target Parent %s'%(datetime.datetime.now())
        target_project_i = client.add_project(targetproject)

        childproject = JSONObject({})
        childproject.name = 'Test Child %s'%(datetime.datetime.now())
        child_project_j = client.add_project(childproject)

        #make sure the child isn't in the parent
        sourceproject_j = client.get_project(target_project_i.id)
        assert len(sourceproject_j.projects) == 0


        #Update the child to say it's now in the parent.
        child_project_j.parent_id = target_project_i.id
        client.update_project(child_project_j)

        ##make sure the child is in the target
        sourceproject_j = client.get_project(target_project_i.id)
        assert len(sourceproject_j.projects) == 1
        assert sourceproject_j.projects[0].id == child_project_j.id

    def test_move_project(self, client):
        """
            Test adding a child project to a project
        """
        #Add a parent
        sourceproject = JSONObject({})
        sourceproject.name = 'Source Parent %s'%(datetime.datetime.now())
        source_project_i = client.add_project(sourceproject)

        #Add another parent that the child will be moved to
        targetproject = JSONObject({})
        targetproject.name = 'Target Parent %s'%(datetime.datetime.now())
        target_project_i = client.add_project(targetproject)

        childproject = JSONObject({})
        childproject.name = 'Test Child %s'%(datetime.datetime.now())
        childproject.parent_id = source_project_i.id
        child_project_j = client.add_project(childproject)

        #Make sure the child is added coorectly
        parentproject_j = client.get_project(source_project_i.id)
        assert len(parentproject_j.projects) == 1
        assert parentproject_j.projects[0].id == child_project_j.id

        #Move the project to another project
        client.move_project(child_project_j.id, target_project_i.id)

        ##make sure the child is no longer in the original parent
        sourceproject_j = client.get_project(source_project_i.id)
        assert len(sourceproject_j.projects) == 0

        ##check the child is in the destination project as requested
        targetproject_j = client.get_project(target_project_i.id)
        assert len(targetproject_j.projects) == 1
        assert targetproject_j.projects[0].id == child_project_j.id


    def test_share_project(self, client, projectmaker):
        """
            Test sharing a sub-project. This should result in the sharee having
            access to the full tree of projects until the shared projects, but not
            the projects which are siblings of any projects in that path through the tree
            For example, upon sharing p4, the sharee should not have access to p3
                                 p1
                                /  \
                               p2   p3
                              /
                             p4
        """

        proj_user = client.user_id
        proj1 = projectmaker.create(share=False)
        proj2 = projectmaker.create(share=False, parent_id=proj1.id)
        proj3 = projectmaker.create(share=False, parent_id=proj1.id)
        proj4 = projectmaker.create(share=False, parent_id=proj2.id)

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_project(proj4.id)

        #Now, as the main user, share P4 with user C
        client.user_id = proj_user
        client.share_project(proj4.id, ['UserC'], False, False)

        #Now as the sharee, try to get project c
        client.user_id = pytest.user_c.id
        userc_p4 = client.get_project(proj4.id)
        userc_p1 = client.get_project(proj1.id)
        userc_p2 = client.get_project(proj2.id)

        #Check user can't see p3 which is in p1, but doesn't have correct permissions
        p1_children = [p.id for p in userc_p1.projects]
        assert p1_children == [proj2.id]

        #Check user can't see p3 which is in p1, but doesn't have correct permissions
        p2_children = [p.id for p in userc_p2.projects]
        assert p2_children == [proj4.id]

        with pytest.raises(HydraError):
            client.get_project(proj3.id)


        client.user_id = proj_user
        client.unshare_project(proj4.id, ['UserC'])

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_project(proj4.id)
        client.user_id = proj_user

    def test_share_network(self, client, projectmaker, networkmaker):
        """
            Test sharing a network contained in a sub-project. This should result in the sharee having
            access to the full tree of projects until the shared projects, but not
            the projects which are siblings of any projects in that path through the tree
            For example, upon sharing p4, the sharee should not have access to p3
                                 p1
                                /  \
                               p2   p3
                              /
                             p4
                            /
                           n1
        """
        client.user_id = 1 # force current user to be 1 to avoid potential inconsistencies
        proj_user = client.user_id
        proj1 = projectmaker.create(share=False)
        proj2 = projectmaker.create(share=False, parent_id=proj1.id)
        proj3 = projectmaker.create(share=False, parent_id=proj1.id)
        proj4 = projectmaker.create(share=False, parent_id=proj2.id)

        net1 = networkmaker.create(project_id=proj4.id)

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_project(proj4.id)

        with pytest.raises(HydraError):
            client.get_network(net1.id)

        #Now, as the main user, share P4 with user C
        client.user_id = proj_user
        client.share_network(net1.id, ['UserC'], False, False)

        #Now as the sharee, try to get project c
        client.user_id = pytest.user_c.id
        userc_n1 = client.get_network(net1.id)
        userc_p4 = client.get_project(proj4.id)
        userc_p1 = client.get_project(proj1.id)
        userc_p2 = client.get_project(proj2.id)

        assert len(userc_p4.networks) == 1
        #Check user can't see p3 which is in p1, but doesn't have correct permissions
        p1_children = [p.id for p in userc_p1.projects]
        assert p1_children == [proj2.id]

        #Check user can't see p3 which is in p1, but doesn't have correct permissions
        p2_children = [p.id for p in userc_p2.projects]
        assert p2_children == [proj4.id]

        with pytest.raises(HydraError):
            client.get_project(proj3.id)

        client.user_id = proj_user
        client.unshare_network(net1.id, ['UserC'])

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_network(net1.id)

        #user no longer has access to P4 because there's no shared network in there
        with pytest.raises(HydraError):
            client.get_project(proj4.id)
        client.user_id = proj_user

    def test_access_to_shared_sub_project(self, client, projectmaker):
        """
            Test sharing a sub-project. This should result in the sharee having
            access to the full tree of projects until the shared projects, but not
            the projects which are siblings of any projects in that path through the tree
            For example, upon sharing p4, the sharee should not have access to p3
                                 p1
                                /  \
                               p2   p3
                              /
                             p4
            If userC wants to navigate to p4, they will also need read access on P1 and P2
            while not being able to see p3 in the list of sub-projects of P1
        """

        proj_user = client.user_id
        proj1 = projectmaker.create(share=False, name='Project1')
        proj2 = projectmaker.create(share=False, name='Project2', parent_id=proj1.id)
        proj3 = projectmaker.create(share=False, name='Project3', parent_id=proj1.id)
        proj4 = projectmaker.create(share=False, name='Project4', parent_id=proj2.id)

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_project(proj4.id)

        #Now, as the main user, share P4 with user C
        client.user_id = proj_user
        client.share_project(proj4.id, ['UserC'], False, False)

        #Now as the sharee, try to get project c
        client.user_id = pytest.user_c.id
        userc_projects = client.get_projects(pytest.user_c.id)
        assert proj1.id in [p.id for p in userc_projects]

        #User C doesn't have explicit read access on proj1 or proj2, but should
        #be abe to navigate to proj1 and 2 so they cna access proj4
        userc_proj1 = client.get_project(project_id=proj1.id)

        #User C should only see project 2, not project 3, as they don't hace access
        #to that branch
        assert len(userc_proj1.projects) == 1

        assert proj2.id in [p.id for p in userc_proj1.projects]

        #User C can't see project 3
        with pytest.raises(HydraError):
            client.get_project(proj3.id)
        client.user_id = proj_user

    def test_access_to_shared_network_in_sub_project(self, client, projectmaker, networkmaker):
        """
            Test sharing a network contained in sub-project. This should result in the sharee having
            access to the full tree of projects until the shared projects, but not
            the projects which are siblings of any projects in that path through the tree
            For example, upon sharing p4, the sharee should not have access to p3
                                 p1
                                /  \
                               p2   p3
                              /
                             N1
            If userC wants to navigate to N1, they will also need read access on P1 and P2
            while not being able to see p3 in the list of sub-projects of P1
        """

        proj_user = client.user_id
        proj1 = projectmaker.create(share=False, name='Project11')
        proj2 = projectmaker.create(share=False, name='Project22', parent_id=proj1.id)
        proj3 = projectmaker.create(share=False, name='Project33', parent_id=proj1.id)

        net1 = networkmaker.create(project_id=proj2.id)

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_network(net1.id)

        #Now, as the main user, share P4 with user C
        client.user_id = proj_user
        client.share_network(net1.id, ['UserC'], False, False)

        #Now as the sharee, try to get project c
        client.user_id = pytest.user_c.id
        userc_projects = client.get_projects(pytest.user_c.id)
        assert proj1.id in [p.id for p in userc_projects]

        #User C doesn't have explicit read access on proj1 or proj2, but should
        #be abe to navigate to proj1 and 2 so they cna access proj4
        userc_proj1 = client.get_project(project_id=proj1.id)

        #User C should only see project 2, not project 3, as they don't hace access
        #to that branch
        assert len(userc_proj1.projects) == 1

        assert proj2.id in [p.id for p in userc_proj1.projects]

        #User C can't see project 3
        with pytest.raises(HydraError):
            client.get_project(proj3.id)

        client.user_id = proj_user
        #Now unshare the network and check to make sure user C can no longer access the project tree.
        client.unshare_network(net1.id, ['UserC'])

        #Now as the sharee, try to get project c
        client.user_id = pytest.user_c.id
        userc_projects = client.get_projects(pytest.user_c.id)
        assert proj1.id not in [p.id for p in userc_projects]

        #User C no longer has access to project 1 as the network is no longer visible
        with pytest.raises(HydraError):
            client.get_project(project_id=proj1.id)

        #User C can't see project 3
        with pytest.raises(HydraError):
            client.get_project(project_id=proj3.id)

    def test_remove_project_parent(self, client, projectmaker, networkmaker):
        """
          Test two actions which should result in a project's parent_id
          being set to None:
            - Calling update_project() where the existing version has a not-None
              parent_id while the argument's parent_id has been set to None
            - Calling move_project() with a target parent project of None
        """
        orig_parent_proj = projectmaker.create()
        child_proj_update = projectmaker.create(parent_id=orig_parent_proj.id)
        child_proj_move = projectmaker.create(parent_id=orig_parent_proj.id)

        proj_i = client.get_project(child_proj_update.id)
        assert proj_i.parent_id == orig_parent_proj.id

        # Remove parent of child_proj
        proj_i.parent_id = None
        client.update_project(proj_i)

        # Verify retrieved project now has no parent
        proj_i = client.get_project(child_proj_update.id)
        assert proj_i.parent_id is None

        proj_i = client.get_project(child_proj_move.id)
        assert proj_i.parent_id == orig_parent_proj.id

        client.move_project(child_proj_move.id, None)

        # Verify moved project now has no parent
        proj_i = client.get_project(child_proj_move.id)
        assert proj_i.parent_id is None
