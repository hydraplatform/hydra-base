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
from .base import *

from .ownership import NetworkOwner, ProjectOwner
from .scenario import Scenario, ResourceScenario
from .permissions import User
from .network import Network, ResourceAttr
from .attributes import Attr

global project_cache_key
project_cache_key = config.get('cache', 'projectkey', 'userprojects')

__all__ = ['Project']


class Project(Base, Inspect, PermissionControlled):
    """
    """

    __tablename__='tProject'
    ref_key = 'PROJECT'

    __ownerclass__ = ProjectOwner

    __table_args__ = (
        UniqueConstraint('name', 'created_by', 'status', name="unique proj name"),
    )

    core_columns = ['name', 'description', 'status']

    attribute_data = []

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False, unique=False)
    description = Column(String(1000))
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)
    appdata = Column(JSON)
    user = relationship('User', backref=backref("projects", order_by=id))

    parent_id = Column(Integer(), ForeignKey('tProject.id'), nullable=True)
    parent = relationship('Project', remote_side=[id],
        backref=backref("children", order_by=id))

    _parents  = []
    _children = ['tNetwork']

    def get_networks(self, user_id, include_deleted_networks=False):
        """
        get all the networks, scenarios and owners of all the networks in the project.
        These are in 3 different sets:
        1: Networks of which user_id is the creator
        2: Networks of which user_id has read access but is not the creator
        3: Networks which user_id can see by virtue of being an owner of the project
           In this case, user_id can see all the networks. Ownership can be directly set
           on this project, or can be set at a higher level project
        """

        log.debug("Getting networks for project %s", self.id)

        networks = []
        networks_not_creator = []

        if self.is_owner(user_id):
            #all networks in the project, as the user owns the project
            networks_not_creator = get_session().query(Network)\
                .filter(Network.project_id == self.id).all()
        else:
            #all networks created by someone else, but which this user is an owner,
            #and this user can read this network
            networks_not_creator = get_session().query(Network).join(NetworkOwner)\
                .filter(Network.project_id == self.id)\
                .filter(Network.created_by != user_id)\
                .filter(NetworkOwner.user_id == user_id)\
                .filter(NetworkOwner.view == 'Y').all()

        all_network_ids = [n.id for n in networks_not_creator]

        #for efficiency, get all the owners in 1 query and sort them by network
        all_owners = get_session().query(NetworkOwner)\
            .filter(NetworkOwner.network_id.in_(all_network_ids)).all()

        owners_by_network = defaultdict(list)
        for owner in all_owners:
            owners_by_network[owner.network_id].append(JSONObject(owner))

        project_owners = self.get_owners()

        #for efficiency, get all the scenarios in 1 query and sort them by network
        all_scenarios = get_session().query(Scenario)\
            .filter(Scenario.network_id.in_(all_network_ids)).all()

        scenarios_by_network = defaultdict(list)
        for netscenario in all_scenarios:
            scenarios_by_network[netscenario.network_id].append(netscenario)

        for net_i in networks_not_creator:

            if include_deleted_networks is False and net_i.status.lower() == 'x':
                continue

            net_j = JSONObject(net_i)
            net_j.owners = owners_by_network[net_j.id]
            owner_ids = [no.user_id for no in owners_by_network[net_j.id]]
            #include inherited owners from the project ownership for this project
            for proj_owner in project_owners:
                if proj_owner.user_id not in owner_ids:
                    proj_owner.source = f'Inherited from: {self.name} (ID:{self.id})'
                    net_j.owners.append(proj_owner)
            net_j.scenarios = scenarios_by_network[net_j.id]
            networks.append(net_j)

        log.debug("%s networks retrieved", len(networks))

        return networks

    def get_child_projects(self, user_id, include_deleted_networks=False, levels=1):
        """
        Get all the direct child projects of a given project
        i.e. all projects which have this project specified in the 'parent_id' column
        """
        log.debug("Getting child projects of project %s", self.id)

        child_projects_i = get_session().query(Project).outerjoin(ProjectOwner)\
            .filter(Project.parent_id == self.id).all()

        projects_with_access = [] # projects to which the user has access
        for child_proj_i in child_projects_i:
            if child_proj_i.check_read_permission(user_id, do_raise=False) is True:
                projects_with_access.append(child_proj_i)

        project_lookup = {p.id:p for p in child_projects_i}

        owners = get_session().query(
            User.id.label('user_id'), User.display_name,
            ProjectOwner.project_id).filter(
                User.id==ProjectOwner.user_id).filter(
                    ProjectOwner.project_id.in_([p.id for p in child_projects_i])).all()

        creators = get_session().query(User.id.label('user_id'), User.display_name).filter(User.id.in_([p.created_by for p in child_projects_i])).all()
        creator_lookup = {u.user_id:JSONObject(u)  for u in creators}

        owner_lookup = defaultdict(list)
        for p in child_projects_i:
            owner_lookup[p.id] = [creator_lookup[p.created_by]]
        for o in owners:
            if o.user_id == project_lookup[o.project_id].created_by:
                continue
            owner_lookup[o.project_id].append(JSONObject(o))

        #Get the inherited owners of the child projects
        parentowners = self.get_owners()

        child_projects = []
        for child_proj_i in projects_with_access:
            project = JSONObject(child_proj_i)
            project.owners = owner_lookup.get(project.id, [])
            owner_ids = [o.user_id for o in project.owners]
            #add any inherited owners to the child projects.
            for parentowner in parentowners:
                if parentowner.user_id not in owner_ids:
                    parentowner.source = f"Inherited from {parentowner.project_name} (ID:{parentowner.project_id})"
                    project.owners.append(parentowner)

            project.networks = child_proj_i.get_networks(
                user_id,
                include_deleted_networks=include_deleted_networks)
            if levels > 0:
                project.projects = child_proj_i.get_child_projects(
                    user_id,
                    include_deleted_networks=include_deleted_networks, levels=(levels-1))
            else:
                project.projects = []
            child_projects.append(project)

        log.debug("%s child projects retrieved", len(child_projects))

        return child_projects

    def is_owner(self, user_id):
        """Check whether this user is an owner of this project, either directly
        #or by virtue of being an owner of a higher-up project"""

        if self.check_read_permission(user_id, nav=False, do_raise=False) is True:
            return True
        p = self.parent
        if p is not None:
            return p.is_owner(user_id)

        return False

    def get_owners(self):
        """
            Get all the owners of a project, both those which are applied directly
            to this project, but also who have been granted access via a parent project
        """

        owners = [JSONObject(o) for o in self.owners]
        owner_ids = [o.user_id for o in owners]

        for o in owners:
            o.project_name = self.name
            o.type = 'PROJECT'

        parent_owners = []
        if self.parent_id is not None:
            parent_owners = list(filter(lambda x:x.user_id not in owner_ids, self.parent.get_owners()))
            for po in parent_owners:
                po.source = f'Inherited from: {po.project_name} (ID:{po.project_id})'

        return owners + parent_owners

    """
    This map should look like:
     {'UID' :
         {
             None: [P1, P2],
             'P1': [P3, P4]
         }
     }
    Where UID is the user ID and the inner keys are project IDS, and the lists are
    projects the user can see within those projects. The 'None' key at the top is for
    top-level projects.
    """
    @classmethod
    def get_cache(cls, user_id=None):
        if user_id is None:
            return cache.get(project_cache_key, {})
        else:
            return cache.get(project_cache_key, {}).get(user_id, {})

    @classmethod
    def set_cache(cls, data):
        cache.set(project_cache_key, dict(data))

    @classmethod
    def clear_cache(cls, uid):
        projectcache = cache.get(project_cache_key, {})
        if projectcache.get(uid) is not None:
            del projectcache[uid]
        cls.set_cache(projectcache)

    def get_name(self):
        return self.project_name

    @classmethod
    def build_user_cache(cls, uid):
        """
            Build the cache of projects a user has access to either by direct Ownership
            or by indirect access required for navigating to a projct to which they own
        """
        if cls.get_cache().get(uid) is not None:
            return

        project_user_cache = defaultdict(lambda: defaultdict(list))
        ##Don't load the project's networks. Load them separately, as the networks
        #must be checked individually for ownership
        projects_qry = get_session().query(Project)
        network_project_qry = get_session().query(Project.id)

        log.info("Getting projects for user %s", uid)

        network_projects_i = network_project_qry.outerjoin(Network).outerjoin(NetworkOwner).filter(
            Network.status == 'A',
            NetworkOwner.user_id == uid,
            NetworkOwner.view == 'Y').distinct().all()

        #for some reason this outputs a list of tuples.
        projects_with_network_owner = [p[0] for p in network_projects_i]

        projects_qry = projects_qry.outerjoin(ProjectOwner).filter(
            Project.status == 'A', or_(
                and_(ProjectOwner.user_id == uid, ProjectOwner.view == 'Y'),
                Project.id.in_(projects_with_network_owner)
            )
        )

        projects_qry = projects_qry.options(noload(Project.networks)).order_by('id')

        projects_i = projects_qry.all()

        parent_project_ids = []
        for p in projects_i:
            project_user_cache[uid][p.parent_id].append(p.id)
            if p.parent_id is not None:
                parent_project_ids.append(p.parent_id)

        cls._build_user_cache_up_tree(uid, parent_project_ids, project_user_cache)

        cls._build_user_cache_down_tree(uid, [p.id for p in projects_i], project_user_cache)

        cls.set_cache(project_user_cache)

    @classmethod
    def _build_user_cache_down_tree(cls, uid, project_ids, project_user_cache):

        if len(project_ids) == 0:
            return

        projects = get_session().query(Project).filter(Project.parent_id.in_(project_ids)).all()

        child_project_ids = []
        for p in projects:
            project_user_cache[uid][p.parent_id].append(p.id)
            child_project_ids.append(p.id)

        cls._build_user_cache_down_tree(uid, child_project_ids, project_user_cache)

    @classmethod
    def _build_user_cache_up_tree(cls, uid, project_ids, project_user_cache):

        if len(project_ids) == 0:
            return

        projects = get_session().query(Project).filter(Project.id.in_(project_ids)).all()

        parent_project_ids = []
        for p in projects:
            project_user_cache[uid][p.parent_id].append(p.id)
            if p.parent_id is not None:
                parent_project_ids.append(p.parent_id)

        cls._build_user_cache_up_tree(uid, parent_project_ids, project_user_cache)

    def get_attribute_data(self):
        attribute_data_rs = get_session().query(ResourceScenario).join(ResourceAttr).filter(
            ResourceAttr.project_id==self.id).all()
        #lazy load datasets
        [rs.dataset.metadata for rs in attribute_data_rs]
        [rs.resourceattr.attr for rs in attribute_data_rs]
        self.attribute_data = attribute_data_rs
        return attribute_data_rs

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.project_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):

        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = ProjectOwner()
            owner.project_id = self.id
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view = read
        owner.edit = write
        owner.share = share

        Project.clear_cache(user_id)

        return owner

    def unset_owner(self, user_id):
        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the project", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break
        Project.clear_cache(user_id)

    def check_read_permission(self, user_id, do_raise=True, is_admin=None, nav=True):
        """
            Check whether this user can read this project
            nav: Indicates whether you can return true if the user is not an owner but
            is allowed to see the project for navigation purposes.
        """

        has_permission = super(Project, self).check_read_permission(user_id,
            is_admin=is_admin, do_raise=False)

        if nav is True:
            Project.build_user_cache(user_id)
            for v in Project.get_cache(user_id).values():
                if self.id in v:
                    return True

        if has_permission is False and self.parent_id is not None:
            """
                Permission check up the tree only applies to non 'nav'. i.e. i a parent which
                can be accessed for nav purposes does not count.
            """
            has_permission = self.parent.check_read_permission(user_id, nav=False, do_raise=False)

        if has_permission is False and do_raise is True:
            raise PermissionError("Permission denied. User %s does not have read"
                                  " access on project '%s'" % (user_id, self.name))

        return has_permission

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this project
        """
        has_permission = super(Project, self).check_write_permission(user_id,
            is_admin=is_admin, do_raise=False)

        if has_permission is False and self.parent_id is not None:
            has_permission = self.parent.check_write_permission(user_id)

        if has_permission is False and do_raise is True:
            raise PermissionError("Permission denied. User %s does not have edit"
                                  " access on project %s" % (user_id, self.id))

        return has_permission

    def get_scoped_attributes(self, include_hierarchy=False, name_match=None, return_json=True):
        """
            Get all the attributes scoped to this project, and to all projects above
            it in the project hierarchy (including global attributes if requested)
            args:
                include_hierarchy (Bool): Include attribtues from projects higher up in the
                    project hierarchy
        """

        scoped_attrs_qry = get_session().query(Attr).filter(Attr.project_id==self.id)

        if name_match is not None:
            name_match = name_match.lower()
            scoped_attrs_qry = scoped_attrs_qry.filter(
                func.lower(Attr.name).like(f'%{name_match}%'))

        scoped_attrs = scoped_attrs_qry.all()

        if self.parent_id is not None and include_hierarchy is True:
            scoped_attrs.extend(self.parent.get_scoped_attributes(
                include_hierarchy=True, name_match=name_match))

        if return_json is True:
            scoped_attrs_j = [JSONObject(a) for a in scoped_attrs]
            #This is for convenience to avoid having to do extra calls to get the project name
            for a in scoped_attrs_j:
                a.project_name = self.name

            return scoped_attrs_j
        else:
            return scoped_attrs

    def get_hierarchy(self, user_id):

        project_hierarchy = [JSONObject(self)]
        if self.parent_id:
            project_hierarchy = project_hierarchy + self.parent.get_hierarchy(user_id)

        return project_hierarchy