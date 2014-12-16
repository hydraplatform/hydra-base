from HydraServer.db.model import Network, Scenario, Project, User, Role, Perm, RolePerm, RoleUser, ResourceAttr, ResourceType
from sqlalchemy.orm.exc import NoResultFound
from HydraServer.db import DBSession
import datetime
import random
import bcrypt
from HydraLib.HydraException import HydraError
import transaction
import logging
log = logging.getLogger(__name__)


def add_resource_types(resource_i, types):
    """
    Save a reference to the types used for this resource.

    @returns a list of type_ids representing the type ids
    on the resource.

    """
    if types is None:
        return []

    existing_type_ids = []
    if resource_i.types:
        for t in resource_i.types:
            existing_type_ids.append(t.type_id)

    new_type_ids = []
    for templatetype in types:

        if templatetype.id in existing_type_ids:
            continue

        rt_i = ResourceType()
        rt_i.type_id     = templatetype.id
        rt_i.ref_key     = resource_i.ref_key
        if resource_i.ref_key == 'NODE':
            rt_i.node_id      = resource_i.node_id
        elif resource_i.ref_key == 'LINK':
            rt_i.link_id      = resource_i.link_id
        elif resource_i.ref_key == 'GROUP':
            rt_i.group_id     = resource_i.group_id
        resource_i.types.append(rt_i)
        new_type_ids.append(templatetype.id)

    return new_type_ids

def add_attributes(resource_i, attributes):
    if attributes is None:
        return {}
    resource_attrs = {}
    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
        else:
            ra_i = DBSession.query(ResourceAttr).filter(ResourceAttr.resource_attr_id==ra.id).one()
            ra_i.attr_is_var = ra.attr_is_var

        resource_attrs[ra.id] = ra_i

    return resource_attrs

def make_root_user():

    try:
        user = DBSession.query(User).filter(User.username=='root').one()
    except NoResultFound:
        user = User(username='root',
                    password=bcrypt.hashpw('', bcrypt.gensalt()),
                    display_name='Root User')
        DBSession.add(user)

    try:
        role = DBSession.query(Role).filter(Role.role_code=='admin').one()
    except NoResultFound:
        raise HydraError("Admin role not found.")

    try:
        userrole = DBSession.query(RoleUser).filter(RoleUser.role_id==role.role_id,
                                                   RoleUser.user_id==user.user_id).one()
    except NoResultFound:
        userrole = RoleUser(role_id=role.role_id,user_id=user.user_id) 
        user.roleusers.append(userrole)
        DBSession.add(userrole)
    DBSession.flush()
    transaction.commit()


def login_user(username, password):
    try:
        user_i = DBSession.query(User).filter(User.username==username).one()
    except NoResultFound:
       raise HydraError(username)

    if bcrypt.hashpw(password.encode('utf-8'), user_i.password.encode('utf-8')) == user_i.password.encode('utf-8'):
        session_id = '%x' % random.randint(1<<124, (1<<128)-1)
    else:
       raise HydraError(username)

    user_i.last_login = datetime.datetime.now()
    return user_i.user_id, session_id


def create_default_net():
    try:
        net = DBSession.query(Network).filter(Network.network_id==1).one()
    except NoResultFound:
        project = Project(project_name="Project network")
        net = Network(network_name="Default network")
        scen = Scenario(scenario_name="Default network")
        project.networks.append(net)
        net.scenarios.append(scen)
        DBSession.add(net)
    DBSession.flush()
    return net
        

def create_default_users_and_perms():
   
    perms = DBSession.query(Perm).all()
    if len(perms) > 0:
        return

    default_perms = ( ("add_user",   "Add User"),
                    ("edit_user",  "Edit User"),
                    ("add_role",   "Add Role"),
                    ("edit_role",  "Edit Role"),
                    ("add_perm",   "Add Permission"),
                    ("edit_perm",  "Edit Permission"),

                    ("add_network",    "Add network"),
                    ("edit_network",   "Edit network"),
                    ("delete_network", "Delete network"),
                    ("share_network",  "Share network"),
                    ("edit_topology",  "Edit network topology"),

                    ("add_project",    "Add Project"),
                    ("edit_project",   "Edit Project"),
                    ("delete_project", "Delete Project"),
                    ("share_project",  "Share Project"),

                    ("edit_data", "Edit network data"),
                    ("view_data", "View network data"),

                    ("add_template", "Add Template"),
                    ("edit_template", "Edit Template"))

    default_roles = (
                    ("admin",    "Administrator"),
                    ("dev",      "Developer"),
                    ("modeller", "Modeller / Analyst"),
                    ("manager",  "Manager"),
                    ("grad",     "Graduate"),
                    ("developer", "Developer"),
                    ("decision", "Decision Maker"),
                )

    roleperms = (
            ('admin', "add_user"),
            ('admin', "edit_user"),
            ('admin', "add_role"),
            ('admin', "edit_role"),
            ('admin', "add_perm"),
            ('admin', "edit_perm"),
            ('admin', "add_network"),
            ('admin', "edit_network"),
            ('admin', "delete_network"),
            ('admin', "share_network"),
            ('admin', "add_project"),
            ('admin', "edit_project"),
            ('admin', "delete_project"),
            ('admin', "share_project"),
            ('admin', "edit_topology"),
            ('admin', "edit_data"),
            ('admin', "view_data"),
            ('admin', "add_template"),
            ('admin', "edit_template"),

            ("developer", "add_network"),
            ("developer", "edit_network"),
            ("developer", "delete_network"),
            ("developer", "share_network"),
            ("developer", "add_project"),
            ("developer", "edit_project"),
            ("developer", "delete_project"),
            ("developer", "share_project"),
            ("developer", "edit_topology"), 
            ("developer", "edit_data"),
            ("developer", "view_data"),
            ("developer", "add_template"),
            ("developer", "edit_template"),

            ("modeller", "add_network"),
            ("modeller", "edit_network"),
            ("modeller", "delete_network"),
            ("modeller", "share_network"),
            ("modeller", "edit_topology"), 
            ("modeller", "add_project"),
            ("modeller", "edit_project"),
            ("modeller", "delete_project"),
            ("modeller", "share_project"),
            ("modeller", "edit_data"),
            ("modeller", "view_data"),

            ("manager", "edit_data"),
            ("manager", "view_data"),
    )
   
    perm_dict = {}
    for code, name in default_perms:
        perm = Perm(perm_code=code, perm_name=name)
        perm_dict[code] = perm
        DBSession.add(perm)
    role_dict = {}
    for code, name in default_roles:
        role = Role(role_code=code, role_name=name)
        role_dict[code] = role
        DBSession.add(role)

    for role_code, perm_code in roleperms:
        roleperm = RolePerm()
        roleperm.role = role_dict[role_code]
        roleperm.perm = perm_dict[perm_code]
        DBSession.add(roleperm)

    DBSession.flush()


