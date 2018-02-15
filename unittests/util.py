# (c) Copyright 2013, 2014, University of Manchester

#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import hydra_base
from hydra_base.util.hydra_dateutil import get_datetime
from hydra_base.lib.objects import JSONObject, Dataset
from hydra_base import config

import json

import datetime

global user_id
user_id = config.get('DEFAULT', 'root_user_id', 2)

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)

def connect(db_url=None):
    hydra_base.connect()

def login(username, password):
    hydra_base.util.hdb.login(username, password)

def logout(username):
    hydra_base.util.hdb.logout(username)

def create_user(name):

    existing_user = hydra_base.get_user_by_name(name)
    
    if existing_user is not None:
        return existing_user

    user = JSONObject(dict(
        username = name,
        password = "password",
        display_name = "test useer",
    ))

    new_user = JSONObject(hydra_base.add_user(user, user_id=user_id))

    #make the user an admin user by default
    role =  JSONObject(hydra_base.get_role_by_code('admin', user_id=user_id))

    hydra_base.set_user_role(new_user.id, role.role_id, user_id=user_id)

    return new_user

def update_template(template_id):

    template = JSONObject(hydra_base.get_template(template_id, user_id=user_id))
    new_net_attr    = create_attr("net_attr_d", dimension='Monetary Value')

    for tmpltype in template.templatetypes:
        if tmpltype.resource_type == 'NETWORK':
            typeattr_1 = dict(
                attr_id = new_net_attr.id,
                data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1},
                unit = 'USD',
            )
            tmpltype.typeattrs.append(typeattr_1)
            break

    template = hydra_base.update_template(template, user_id=user_id)


def create_template(use_existing_template=True):
    if use_existing_template is True:
        template_i = hydra_base.get_template_by_name('Default Template', user_id=user_id)

        if template_i is not None:
            template = JSONObject(template_i)
            return template

    net_attr1    = create_attr("net_attr_a", dimension='Volume')
    net_attr2    = create_attr("net_attr_c", dimension=None)
    link_attr_1  = create_attr("link_attr_a", dimension='Pressure')
    link_attr_2  = create_attr("link_attr_b", dimension='Speed')
    link_attr_3  = create_attr("link_attr_c", dimension='Length')
    node_attr_1  = create_attr("node_attr_a", dimension='Volume')
    node_attr_2  = create_attr("node_attr_b", dimension='Speed')
    node_attr_3  = create_attr("node_attr_c", dimension='Cost')
    group_attr_1 = create_attr("grp_attr_1", dimension='Cost')
    group_attr_2 = create_attr("grp_attr_2", dimension='Displacement')

    template = JSONObject()
    if use_existing_template is True:
        template['name'] = 'Default Template'
    else:
        template['name'] = 'Default Template ' + str(datetime.datetime.now())


    types = []
    #**********************
    #network type         #
    #**********************
    net_type = JSONObject()
    net_type.name = "Default Network"
    net_type.alias = "Test type alias"
    net_type.resource_type='NETWORK'

    typeattrs = []

    typeattr_1 = JSONObject()
    typeattr_1.attr_id = net_attr1.id
    typeattr_1.data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1}
    typeattr_1.unit = 'm^3'
    typeattrs.append(typeattr_1)

    typeattr_2 = JSONObject()
    typeattr_2.attr_id = net_attr2.id
    typeattrs.append(typeattr_2)

    net_type.typeattrs = typeattrs

    types.append(net_type)
    #**********************
    # node type           #
    #**********************
    node_type = JSONObject()
    node_type.name = "Default Node"
    node_type.alias = "Test type alias"
    node_type.resource_type='NODE'

    typeattrs = []

    typeattr_1 = JSONObject()
    typeattr_1.attr_id = node_attr_1.id
    typeattr_1.data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1}
    typeattr_1.unit = 'm^3'
    typeattrs.append(typeattr_1)

    typeattr_2 = JSONObject()
    typeattr_2.attr_id = node_attr_2.id
    typeattr_2.data_restriction = {'INCREASING': None}
    typeattrs.append(typeattr_2)

    typeattr_3 = JSONObject()
    typeattr_3.attr_id = node_attr_3.id
    typeattrs.append(typeattr_3)

    node_type.typeattrs = typeattrs

    types.append(node_type)
    #**********************
    #link type            #
    #**********************
    link_type = JSONObject()
    link_type.name = "Default Link"
    link_type.resource_type='LINK'

    typeattrs = []

    typeattr_1 = JSONObject()
    typeattr_1.attr_id = link_attr_1.id
    typeattrs.append(typeattr_1)

    typeattr_2 = JSONObject() 
    typeattr_2.attr_id = link_attr_2.id
    typeattrs.append(typeattr_2)

    typeattr_3 = JSONObject()
    typeattr_3.attr_id = link_attr_3.id
    typeattrs.append(typeattr_3)

    link_type.typeattrs = typeattrs

    types.append(link_type)

    #**********************
    #group type           #
    #**********************
    group_type = JSONObject()
    group_type.name = "Default Group"
    group_type.resource_type='GROUP'

    typeattrs = []

    typeattr_1 = JSONObject() 
    typeattr_1.attr_id = group_attr_1.id
    typeattrs.append(typeattr_1)

    typeattr_2 = JSONObject()
    typeattr_2.attr_id = group_attr_2.id
    typeattrs.append(typeattr_2)

    group_type.typeattrs = typeattrs

    types.append(group_type)

    template.types = types

    new_template_i = hydra_base.add_template(template, user_id=user_id)
    new_template = JSONObject(new_template_i)
    
    assert new_template.name == template.name, "Names are not the same!"
    assert new_template.id is not None, "New Template has no ID!"
    assert new_template.id > 0, "New Template has incorrect ID!"

    assert len(new_template.templatetypes) == len(types), "Resource types did not add correctly"
    for t in new_template.templatetypes[1].typeattrs:
        assert t.attr_id in (node_attr_1.id, node_attr_2.id, node_attr_3.id);
        "Node types were not added correctly!"

    for t in new_template.templatetypes[2].typeattrs:
        assert t.attr_id in (link_attr_1.id, link_attr_2.id, link_attr_3.id);
        "Link types were not added correctly!"

    return new_template

def create_project(name=None):

    if name is None:
        name = "Unittest Project"

    try:
        p = JSONObject(hydra_base.get_project_by_name(name), user_id=user_id)
        return p
    except:
        project = JSONObject()
        project.name = name
        project.description = "Project which contains all unit test networks"
        project = JSONObject(hydra_base.add_project(project, user_id=user_id))
        hydra_base.share_project(project.project_id,
                                 ["UserA", "UserB", "UserC"],
                                 'N',
                                 'Y',
                                 user_id=user_id)

        return project


def create_link(link_id, node_1_name, node_2_name, node_1_id, node_2_id):

    ra_array = []

    link = JSONObject({
        'id'          : link_id,
        'name'        : "%s_to_%s"%(node_1_name, node_2_name),
        'description' : 'A test link between two nodes.',
        'layout'      : None,
        'node_1_id'   : node_1_id,
        'node_2_id'   : node_2_id,
        'attributes'  : ra_array,
    })

    return link

def create_node(node_id, attributes=None, node_name="Test Node Name"):

    if attributes is None:
        attributes = []
    #turn 0 into 1, -1 into 2, -2 into 3 etc..
    coord = (node_id * -1) + 1
    node = JSONObject({
        'id' : node_id,
        'name' : node_name,
        'description' : "A node representing a water resource",
        'layout'      : None,
        'x' : 10 * coord,
        'y' : 10 * coord -1,
        'attributes' : attributes,
    })

    return node

def create_attr(name="Test attribute", dimension="dimensionless"):
    attr_i = hydra_base.get_attribute_by_name_and_dimension(name, dimension, user_id=user_id)
    if attr_i is None:
        attr = JSONObject({'name'  : name,
                'dimen' : dimension
               })
        attr = JSONObject(hydra_base.add_attribute(attr, user_id=user_id))
    else:
        attr = JSONObject(attr_i)
    return attr

def build_network(project_id=None, num_nodes=10, new_proj=True,
                  map_projection='EPSG:4326', use_existing_template=True):

    start = datetime.datetime.now()
    if project_id is None:
        proj_name = None
        if new_proj is True:
            proj_name = "Test Project @ %s"%(datetime.datetime.now())
            project_id = create_project(name=proj_name).id
        else:
            project_id = project_id

    log.debug("Project creation took: %s"%(datetime.datetime.now()-start))
    start = datetime.datetime.now()

    template = create_template(use_existing_template=use_existing_template)

    log.debug("Attribute creation took: %s"%(datetime.datetime.now()-start))
    start = datetime.datetime.now()


    #Put an attribute on a group
    group_ra = JSONObject(dict(
        ref_id  = None,
        ref_key = 'GROUP',
        attr_is_var = 'N',
        attr_id = template.templatetypes[2].typeattrs[0].attr_id,
        id      = -1
    ))
    group_attrs = [group_ra]

    nodes = []
    links = []

    prev_node = None
    ra_index = 2

    network_type = template.templatetypes[0]
    node_type = template.templatetypes[1]
    link_type = template.templatetypes[2]
    for n in range(num_nodes):
        node = create_node(n*-1, node_name="Node %s"%(n))

        #From our attributes, create a resource attr for our node
        #We don't assign data directly to these resource attributes. This
        #is done when creating the scenario -- a scenario is just a set of
        #data for a given list of resource attributes.
        node_ra1         = JSONObject(dict(
            ref_key = 'NODE',
            ref_id  = None,
            attr_id = node_type.typeattrs[0].attr_id,
            id      = ra_index * -1,
            attr_is_var = 'N',
        ))
        ra_index = ra_index + 1
        node_ra2         = JSONObject(dict(
            ref_key = 'NODE',
            ref_id  = None,
            attr_id = node_type.typeattrs[1].attr_id,
            id      = ra_index * -1,
            attr_is_var = 'Y',
        ))
        ra_index = ra_index + 1
        node_ra3         = JSONObject(dict(
            ref_key = 'NODE',
            ref_id  = None,
            attr_id = node_type.typeattrs[2].attr_id,
            id      = ra_index * -1,
            attr_is_var = 'N',
        ))
        ra_index = ra_index + 1

        node.attributes = [node_ra1, node_ra2, node_ra3]

        type_summary = JSONObject(dict(
            template_id = template.id,
            template_name = template.name,
            id = node_type.type_id,
            name = node_type.name
        ))

        type_summary_arr = [type_summary]

        node.types = type_summary_arr

        nodes.append(node)

        if prev_node is not None:
            #Connect the two nodes with a link
            link = create_link(
                n*-1,
                node['name'],
                prev_node['name'],
                node['id'],
                prev_node['id'])

            link_ra1         = JSONObject(dict(
                ref_id  = None,
                ref_key = 'LINK',
                id     = ra_index * -1,
                attr_id = link_type.typeattrs[0].attr_id,
                attr_is_var = 'N',
            ))
            ra_index = ra_index + 1
            link_ra2         = JSONObject(dict(
                ref_id  = None,
                ref_key = 'LINK',
                attr_id = link_type.typeattrs[1].attr_id,
                id      = ra_index * -1,
                attr_is_var = 'N',
            ))
            ra_index = ra_index + 1
            link_ra3         = JSONObject(dict(
                ref_id  = None,
                ref_key = 'LINK',
                attr_id = link_type.typeattrs[2].attr_id,
                id      = ra_index * -1,
                attr_is_var = 'N',
            ))
            ra_index = ra_index + 1

            link.attributes = [link_ra1, link_ra2, link_ra3]
            if link['id'] % 2 == 0:
                type_summary_arr = []
                type_summary = JSONObject()
                type_summary.template_id = template.id
                type_summary.template_name = template.name
                type_summary.id = link_type.type_id
                type_summary.name = link_type.name

                type_summary_arr.append(type_summary)

                link.types = type_summary_arr
            links.append(link)

        prev_node = node

    #A network must contain an array of links. In this case, the array

    log.debug("Making nodes & links took: %s"%(datetime.datetime.now()-start))
    start = datetime.datetime.now()

    #Create the scenario
    scenario = JSONObject()
    scenario.id = -1
    scenario.name        = 'Scenario 1'
    scenario.description = 'Scenario Description'
    scenario.layout      = json.dumps({'app': ["Unit Test1", "Unit Test2"]})
    scenario.start_time  = datetime.datetime.now()
    scenario.end_time    = scenario.start_time + datetime.timedelta(hours=1)
    scenario.time_step   = 1 # one second intervals.

    #Multiple data (Called ResourceScenario) means an array.
    scenario_data = []

    group_array       = []
    group             = JSONObject(dict(
        id          = -1,
        name        = "Test Group",
        description = "Test group description"
    ))

    group.attributes = group_attrs

    group_array.append(group)

    group_item_array      = []
    group_item_1 = JSONObject(dict(
        ref_key  = 'NODE',
        ref_id   = nodes[0]['id'],
        group_id = group['id'],
    ))
    group_item_2  = JSONObject(dict(
        ref_key  = 'NODE',
        ref_id   = nodes[1]['id'],
        group_id = group['id'],
    ))
    group_item_array = [group_item_1, group_item_2]

    scenario.resourcegroupitems = group_item_array

    #This is an example of 3 diffent kinds of data

    #For Links use the following:
    #A simple string (Descriptor)
    #A multi-dimensional array.

    #For nodes, use the following:
    #A time series, where the value may be a 1-D array
    
    nodes[0].attributes
    for n in nodes:
        for na in n.attributes:
            if na.get('attr_is_var', 'N') == 'N':
                if na['attr_id'] == node_type.typeattrs[0].attr_id:
                    timeseries = create_timeseries(na)
                    scenario_data.append(timeseries)
                elif na['attr_id'] == node_type.typeattrs[2].attr_id:
                    scalar = create_scalar(na)
                    scenario_data.append(scalar)

    for l in links:
        for na in l.attributes:
            if na['attr_id'] == link_type.typeattrs[0].attr_id:
                array      = create_array(na)
                scenario_data.append(array)
            elif na['attr_id'] == link_type.typeattrs[1].attr_id:
                descriptor = create_descriptor(na)
                scenario_data.append(descriptor)


    grp_timeseries = create_timeseries(group_attrs[0])

    scenario_data.append(grp_timeseries)

    #Set the scenario's data to the array we have just populated
    scenario.resourcescenarios = scenario_data

    #A network can have multiple scenarios, so they are contained in
    #a scenario array
    scenario_array = []
    scenario_array.append(scenario)

    log.debug("Scenario definition took: %s"%(datetime.datetime.now()-start))
    #This can also be defined as a simple dictionary, but I do it this
    #way so I can check the value is correct after the network is created.
    layout = dict(
        color = 'red',
        shapefile = 'blah.shp'
    )

    node_array = nodes
    link_array = links

    net_attr = create_attr("net_attr_b", dimension='Pressure')

    ra_index = ra_index + 1
    net_ra_notmpl = JSONObject(dict(
        ref_id  = None,
        ref_key = 'NETWORK',
        attr_is_var = 'N',
        attr_id = net_attr.id,
        id      = ra_index*-1
    ))
    ra_index = ra_index + 1
    net_ra_tmpl = JSONObject(dict(
        ref_id  = None,
        ref_key = 'NETWORK',
        attr_is_var = 'N',
        attr_id = network_type.typeattrs[0].attr_id,
        id      = ra_index*-1
    ))
    net_attrs = [net_ra_notmpl, net_ra_tmpl]

    net_type_summary_arr = []
    net_type_summary = JSONObject(dict(
        template_id = template.id,
        template_name = template.name,
        id = network_type.type_id,
        name = network_type.name,
    ))
    net_type_summary_arr.append(net_type_summary)

    network = JSONObject(dict(
        name        = 'Network @ %s'%datetime.datetime.now(),
        description = 'Test network with 2 nodes and 1 link',
        project_id  = project_id,
        links       = link_array,
        nodes       = node_array,
        layout      = layout,
        scenarios   = scenario_array,
        resourcegroups = group_array,
        projection  = map_projection,
        attributes  = net_attrs,
        types       = net_type_summary_arr,
    ))

    return network

def create_network_with_data(project_id=None, num_nodes=10,
                             ret_full_net=True, new_proj=False,
                             map_projection='EPSG:4326',
                            use_existing_template=True):
    """
        Test adding data to a network through a scenario.
        This test adds attributes to one node and then assignes data to them.
        It assigns a descriptor, array and timeseries to the
        attributes node.
    """
    network=build_network(project_id, num_nodes, new_proj=new_proj,
                               map_projection=map_projection,
                                use_existing_template=use_existing_template)

    #log.debug(network)
    start = datetime.datetime.now()
    log.info("Creating network...")
    response_network_summary = JSONObject(hydra_base.add_network(network, user_id=user_id))
    log.info("Network Creation took: %s"%(datetime.datetime.now()-start))
    if ret_full_net is True:
        log.info("Fetching new network...:")
        start = datetime.datetime.now()
        response_net = JSONObject(hydra_base.get_network(response_network_summary.id, user_id=user_id))
        log.info("Network Retrieval took: %s"%(datetime.datetime.now()-start))
        check_network(network, response_net)
        return response_net
    else:
       return response_network_summary

def get_network(network_id=None):
    """
        Get a network with all data.
    """
    network=JSONObject(hydra_base.get_network(network_id, user_id=user_id))
    return network



def check_network(request_net, response_net):

    assert json.loads(response_net.layout) == request_net['layout']


    assert response_net.scenarios[0].created_by is not None

    for n in response_net.nodes:
        assert n.node_x is not None
        assert n.node_y is not None
        assert len(n.attributes) > 0

    before_times = []

    s = request_net['scenarios'][0]
    for rs0 in s['resourcescenarios']:
        if rs0['value']['type'] == 'timeseries':
            val = json.loads(rs0['value']['value'])
            before_ts_times = val.values()[0].keys()
            before_times = []
            for t in before_ts_times:
                try:
                    before_times.append(get_datetime(t))
                except:
                    before_times.append(t)

    after_times = []
    s = response_net.scenarios[0]
    for rs0 in s.resourcescenarios:
        if rs0.value.type == 'timeseries':
            val = json.loads(rs0.value.value)
            after_ts_times = val.values()[0].keys()
            after_times = []
            for t in after_ts_times:
                try:
                    after_times.append(get_datetime(t))
                except:
                    after_times.append(t)

    for d in after_times:
        assert d in before_times, "%s is incorrect"%(d)


def create_scalar(resource_attr, val=1.234):
    #with a resource attribute.

    dataset = Dataset(dict(
        id=None,
        type = 'scalar',
        name = 'Flow speed',
        unit = 'm s^-1',
        dimension = 'Speed',
        hidden = 'N',
        value = val,
    ))

    scenario_attr = JSONObject(dict(
        attr_id = resource_attr.attr_id,
        resource_attr_id = resource_attr.id,
        value = dataset,
    ))

    return scenario_attr

def create_descriptor(resource_attr, val="test"):
    #A scenario attribute is a piece of data associated
    #with a resource attribute.

    dataset = Dataset(dict(
        id=None,
        type = 'descriptor',
        name = 'Flow speed',
        unit = 'm s^-1',
        dimension = 'Speed',
        hidden = 'N',
        value = val,
    ))

    scenario_attr = JSONObject(dict(
        attr_id = resource_attr.attr_id,
        resource_attr_id = resource_attr.id,
        value = dataset,
    ))

    return scenario_attr


def create_timeseries(resource_attr):
    #A scenario attribute is a piece of data associated
    #with a resource attribute.
    #[[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]]]

    fmt = hydra_base.config.get('DEFAULT', 'datetime_format', "%Y-%m-%dT%H:%M:%S.%f000Z")

    t1 = datetime.datetime.now()
    t2 = t1+datetime.timedelta(hours=1)
    t3 = t1+datetime.timedelta(hours=2)

    val_1 = [[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]], [[9, 8, 7],[6, 5, 4]]]
    val_2 = [1.0, 2.0, 3.0]

    val_3 = [3.0, None, None]

    ts_val = {"index": {t1.strftime(fmt): val_1,
                  t2.strftime(fmt): val_2,
                  t3.strftime(fmt): val_3}}

    metadata = {'created_by': 'Test user'}

    dataset = Dataset(dict(
        id=None,
        type = 'timeseries',
        name = 'my time series',
        unit = 'cm^3',
        dimension = 'Volume',
        hidden = 'N',
        value = json.dumps(ts_val),
        metadata = metadata
    ))

    scenario_attr = JSONObject(dict(
        attr_id = resource_attr.attr_id,
        resource_attr_id = resource_attr.id,
        value = dataset,
    ))

    return scenario_attr

def create_array(resource_attr):
    #A scenario attribute is a piece of data associated
    #with a resource attribute.
    #[[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    arr = json.dumps([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])

    metadata_array = json.dumps({'created_by': 'Test user'})

    dataset = Dataset(dict(
        id=None,
        type = 'array',
        name = 'my array',
        unit = 'bar',
        dimension = 'Pressure',
        hidden = 'N',
        value = arr,
        metadata = metadata_array,
    ))

    scenario_attr = JSONObject(dict(
        attr_id = resource_attr.attr_id,
        resource_attr_id = resource_attr.id,
        value = dataset,
    ))

    return scenario_attr
