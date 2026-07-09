# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2020 University of Manchester
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

import logging
import json
import pytest
from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import HydraError
log = logging.getLogger(__name__)


class TestTemplateInheritance:
    """
        TEMPLATES Functions
    """

    def test_add_child_template(self, client):
        """
            Test a child template of a template, then retrieve that
            and ensure that it is consistent with the parent.
        """

        template_j = client.testutils.create_template()

        child_template_j = client.testutils.create_child_template(template_j.id)

        child_template_received = client.get_template(child_template_j.id)

        assert template_j.id == child_template_j.parent_id == child_template_received.parent_id
        assert template_j.description == child_template_j.description
        assert child_template_j.description == child_template_received.description

    def test_add_type_to_child(self, client):
        """
            When adding a type to a child template, it should appear in the child
            but not the parent.
        """

        parent_template_j = client.testutils.create_template()

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        original_templatetypes = parent_template_j.templatetypes

        #Add a type to the child
        templatetype_j = client.testutils.create_templatetype(child_template_j.id)
        client.add_templatetype(templatetype_j)

        parent_template_received = client.get_template(parent_template_j.id)
        child_template_received = client.get_template(child_template_j.id)

        assert len(child_template_received.templatetypes)\
                == len(parent_template_received.templatetypes) + 1

        #Add a type to the parent
        templatetype_j = client.testutils.create_templatetype(parent_template_j.id)
        new_type_j = client.add_templatetype(templatetype_j)

        parent_template_received = client.get_template(parent_template_j.id)
        child_template_received = client.get_template(child_template_j.id)

        assert len(parent_template_received.templatetypes)\
                == len(original_templatetypes) + 1
        #check that one has been added to both parent and child
        #so we now have 2 more than originally
        assert len(child_template_received.templatetypes)\
                == len(original_templatetypes) + 2
        assert len(child_template_received.templatetypes)\
                == len(parent_template_received.templatetypes) + 1

    def test_update_type_in_child(self, client):
        """
            Test updating a template type in a child, which will result in creating
            a new template type object in the child template, which has a parent of the type
            in the parent template
        """

        parent_template_j = client.testutils.create_template()

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        child_template_received = client.get_template(child_template_j.id)

        #Store the types from the child for comparison later
        original_child_types = child_template_received.templatetypes


        #Get a type which inherits from the parent, and update it
        type_to_update = None
        for templatetype in child_template_received.templatetypes:
            if templatetype.template_id == parent_template_j.id:
                type_to_update = templatetype
                break


        child_type = client.add_child_templatetype(type_to_update.id, child_template_j.id)

        child_type.status = 'X'

        #hack to make sure the child attrs don't get added
        child_type['typeattrs'] = []

        client.update_templatetype(child_type)

        updated_child_template = client.get_template(child_template_j.id)

        #This should be one less than before
        assert len(updated_child_template.templatetypes) == len(original_child_types)

        for child_type in updated_child_template.templatetypes:
            if child_type.parent_id == type_to_update.id:
                assert child_type.status == 'X'


    def test_add_typeattr_to_child(self, client):
        """
            Test updating a template type in a child, by adding a type attribute
            to a type in the child template
        """

        parent_template_j = client.testutils.create_template()

        un_updated_parent_template = client.get_template(parent_template_j.id)

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        child_template_received = client.get_template(child_template_j.id)


        #Get a type which inherits from the parent, and update it
        type_to_update = None
        for templatetype in child_template_received.templatetypes:
            if templatetype.template_id == parent_template_j.id:
                type_to_update = templatetype
                break

        #Create a child of a parent *type*, so we can add a typeattr to it.
        #WHen the child template is requested, this type will contain all the
        #parent type attributes, as well as the newly added one to the child
        child_type = client.add_child_templatetype(type_to_update.id, child_template_j.id)

        #create some random attribute
        newattr = client.testutils.create_attribute('extra_attr', None)

        #Link this new attribute to the child type using a type attribute
        newtypeattr = JSONObject({
            'attr_id': newattr.id,
            'type_id': child_type.id
        })
        client.add_typeattr(newtypeattr)

        #Save the original types attributes for comparison later
        original_type_attrs = type_to_update.typeattrs

        #Fetch the child and parent templates to verify that the child has
        #been updated (with one extra type attr on one of its types) but the
        #parent has not been updated.
        updated_child_template = client.get_template(child_template_j.id)
        un_updated_parent_template = client.get_template(parent_template_j.id)

        updated_child_type_attrs = None
        updated_parent_type_attrs = None
        for templatetype in updated_child_template.templatetypes:
            if templatetype.parent_id == type_to_update.id:
                updated_child_type_attrs = templatetype.typeattrs
                break
        for templatetype in un_updated_parent_template.templatetypes:
            if templatetype.id == type_to_update.id:
                un_updated_parent_type_attrs = templatetype.typeattrs
                break

        assert len(updated_child_type_attrs) == len(original_type_attrs)+1
        assert len(updated_child_type_attrs) == len(un_updated_parent_type_attrs)+1

    def test_add_typeattr_to_child_then_parent(self, client):
        """
        Test for the situation where a typeattr is added to a child, then added to the parent.
        This is a valid case, and the behaviour should be that the parent typeattr should be ignored,
        rather than returning both or returning some merged typeattr.
        """

        parent_template_j = client.testutils.create_template()

        un_updated_parent_template = client.get_template(parent_template_j.id)

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        child_template_received = client.get_template(child_template_j.id)


        #Get a type which inherits from the parent, and update it
        type_to_update = None
        for templatetype in child_template_received.templatetypes:
            if templatetype.template_id == parent_template_j.id:
                type_to_update = templatetype
                break

        #Create a child of a parent *type*, so we can add a typeattr to it.
        #WHen the child template is requested, this type will contain all the
        #parent type attributes, as well as the newly added one to the child
        child_type = client.add_child_templatetype(type_to_update.id, child_template_j.id)

        #create some random attribute
        newattr = client.testutils.create_attribute('extra_attr', None)

        #Link this new attribute to the child type using a type attribute
        new_child_typeattr = JSONObject({
            'attr_id': newattr.id,
            'type_id': child_type.id
        })
        client.add_typeattr(new_child_typeattr)

        #Save the original types attributes for comparison later
        original_type_attrs = type_to_update.typeattrs

        #Fetch the child and parent templates to verify that the child has
        #been updated (with one extra type attr on one of its types) but the
        #parent has not been updated.
        updated_child_template = client.get_template(child_template_j.id)
        un_updated_parent_template = client.get_template(parent_template_j.id)

        updated_child_type_attrs = None
        updated_parent_type_attrs = None
        for templatetype in updated_child_template.templatetypes:
            if templatetype.parent_id == type_to_update.id:
                updated_child_type_attrs = templatetype.typeattrs
                break
        for templatetype in un_updated_parent_template.templatetypes:
            if templatetype.id == type_to_update.id:
                un_updated_parent_type_attrs = templatetype.typeattrs
                break

        assert len(updated_child_type_attrs) == len(original_type_attrs)+1
        assert len(updated_child_type_attrs) == len(un_updated_parent_type_attrs)+1


        #now add *the same* typeattr to the parent type. This should result in the child type returing its own typeattr
        #and the parent type returning a different typeattr, i.e. a different type_id with the same attr_id
        new_parent_typeattr = JSONObject({
            'attr_id': newattr.id,
            'type_id': type_to_update.id
        })
        client.add_typeattr(new_parent_typeattr)

        #Fetch both the parent and child to verify that the child and parent types
        #have typeattrs with the same attr_id but different type_ids
        updated_child_template = client.get_template(child_template_j.id)
        updated_parent_template = client.get_template(parent_template_j.id)

        updated_child_type_attrs = None
        updated_parent_type_attrs = None
        for templatetype in updated_child_template.templatetypes:
            if templatetype.parent_id == type_to_update.id:
                updated_child_type_attrs = templatetype.typeattrs
                break
        for templatetype in updated_parent_template.templatetypes:
            if templatetype.id == type_to_update.id:
                updated_parent_type_attrs = templatetype.typeattrs
                break

        #check the attr_ids are there
        assert newattr.id in [ta.attr_id for ta in updated_child_type_attrs]
        assert newattr.id in [ta.attr_id for ta in updated_parent_type_attrs]
        #check the type IDs are there
        assert child_type.id in [ta.type_id for ta in updated_child_type_attrs]
        assert type_to_update.id in [ta.type_id for ta in updated_parent_type_attrs]

        #check the type IDs are NOT in the other one's list of typeattrs. Specifically
        #we want to ensure that there are no duplicate typeattrs being returned
        assert child_type.id not in [ta.type_id for ta in updated_parent_type_attrs]
        assert type_to_update.id in [ta.type_id for ta in updated_child_type_attrs]

        #Make sure there are no duplicates returned explicitly
        assert len(set([ta.attr_id for ta in updated_child_type_attrs])) == len([ta.attr_id for ta in updated_child_type_attrs])
        assert len(set([ta.attr_id for ta in updated_parent_type_attrs])) == len([ta.attr_id for ta in updated_parent_type_attrs])




    def test_update_child_typeattr(self, client):
        """
            Test updating a template type attribute in a child, by adding a type attribute
            to a type in the child template, which has a parent in the parent template,
            and then setting the status of that to 'X'
        """

        parent_template_j = client.testutils.create_template()

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        child_template_received = client.get_template(child_template_j.id)

        #Get a type which inherits from the parent, and update it
        typeattr_to_update = None
        original_type = None
        for templatetype in child_template_received.templatetypes:
            if templatetype.template_id == parent_template_j.id:
                original_type = templatetype
                typeattr_to_update = templatetype.typeattrs[0]
                break

        #Create a child of a parent *typeattr*, so we can update it.
        #When the child template is requested, this typeattr will contain the combined
        #data from itself and from its parent.
        child_typeattr = client.add_child_typeattr(typeattr_to_update.id, child_template_j.id)

        child_typeattr.status = 'X'

        client.update_typeattr(child_typeattr)

        #Fetch the child and parent templates to verify that the child has
        #been updated (with one extra type attr on one of its types) but the
        #parent has not been updated.
        updated_child_template = client.get_template(child_template_j.id)
        un_updated_parent_template = client.get_template(parent_template_j.id)


        updated_child_type_attr = None
        update_child_type = None
        un_updated_parent_type_attr = None
        for templatetype in updated_child_template.templatetypes:
            for typeattr in templatetype.typeattrs:
                if typeattr.parent_id == typeattr_to_update.id:
                    updated_child_type = templatetype
                    updated_child_type_attr = typeattr
                    break
        for templatetype in un_updated_parent_template.templatetypes:
            for typeattr in templatetype.typeattrs:
                if typeattr.id == typeattr_to_update.id:
                    un_updated_parent_type_attr = typeattr
                    break

        #The parent and child type should have the same number of type attrs, but
        #one of the children's ones will be set to 'X'
        assert len(original_type.typeattrs) == len(updated_child_type.typeattrs)
        assert updated_child_type_attr.status == 'X'
        assert un_updated_parent_type_attr.status == 'A'

    def test_get_updated_type(self, client):
        """
            Test the individual 'get_type' functionality by
            updating a template type attribute in a child, by adding a type attribute
            to a type in the child template, which has a perent in the parent template,
            and then setting the status of that to 'X'
        """

        parent_template_j = client.testutils.create_template()

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        child_template_received = client.get_template(child_template_j.id)

        #Get a type which inherits from the parent, and update it
        typeattr_to_update = None
        original_type = None
        for templatetype in child_template_received.templatetypes:
            if templatetype.template_id == parent_template_j.id:
                original_type = templatetype
                typeattr_to_update = templatetype.typeattrs[0]
                break

        #Create a child of a parent *typeattr*, so we can update it.
        #When the child template is requested, this typeattr will contain the combined
        #data from itself and from its parent.
        child_typeattr = client.add_child_typeattr(typeattr_to_update.id, child_template_j.id)

        child_typeattr.status = 'X'

        client.update_typeattr(child_typeattr)

        #Fetch the child and parent templates to verify that the child has
        #been updated (with one extra type attr on one of its types) but the
        #parent has not been updated.
        updated_child_type = client.get_templatetype(child_typeattr.type_id)
        un_updated_parent_type = client.get_templatetype(typeattr_to_update.type_id)


        updated_child_type_attr = None
        update_child_type = None
        un_updated_parent_type_attr = None
        for typeattr in updated_child_type.typeattrs:
            if typeattr.parent_id == typeattr_to_update.id:
                updated_child_type = templatetype
                updated_child_type_attr = typeattr
                break
        for typeattr in un_updated_parent_type.typeattrs:
            if typeattr.id == typeattr_to_update.id:
                un_updated_parent_type_attr = typeattr
                break

        #The parent and child type should have the same number of type attrs, but
        #one of the children's ones will be set to 'X'
        assert len(original_type.typeattrs) == len(updated_child_type.typeattrs)
        assert updated_child_type_attr.status == 'X'
        assert un_updated_parent_type_attr.status == 'A'

    def test_get_updated_typeattr(self, client):
        """
            Test the individual 'get_type' functionality by
            updating a template type attribute in a child, by adding a type attribute
            to a type in the child template, which has a perent in the parent template,
            and then setting the status of that to 'X'
        """

        parent_template_j = client.testutils.create_template()

        child_template_j = client.testutils.create_child_template(parent_template_j.id)

        child_template_received = client.get_template(child_template_j.id)

        #Get a type which inherits from the parent, and update it
        typeattr_to_update = None
        original_type = None
        for templatetype in child_template_received.templatetypes:
            if templatetype.template_id == parent_template_j.id:
                original_type = templatetype
                typeattr_to_update = templatetype.typeattrs[0]
                break

        #Create a child of a parent *typeattr*, so we can update it.
        #When the child template is requested, this typeattr will contain the combined
        #data from itself and from its parent.
        child_typeattr = client.add_child_typeattr(typeattr_to_update.id, child_template_j.id)

        child_typeattr.status = 'X'

        client.update_typeattr(child_typeattr)

        updated_child_type_attr = client.get_typeattr(child_typeattr.id)
        un_updated_parent_type_attr = client.get_typeattr(typeattr_to_update.id)

        assert updated_child_type_attr.status == 'X'
        assert un_updated_parent_type_attr.status == 'A'


    def test_create_network_with_child_template(self, client):
        """
            This tests that a network is correctly linked with its template upon
            creation. The issues is:
            Upon creating using a template, a network is linked to a template with
            the template's 'network' template type. In an inherited template, this
            template type may belong to the parent, and so there is no direct means
            of linking a network to the correct child template, as the type ID of the 'network'
            template id will come from the parent. To mitigate this, a 'child template id' column
            exists on the tResourceType table which allows us to explicitly say which template was
            used to create the network.

            When creating a network using a child template, it's possible that
            there is no TemplateType entry for the network within the child template
            i.e. there's no entry in the DB for that template type.

            This means that when you request the template, the template type
            which is returned is that of the parent, and the type_id points to
            the template type entry in the parent template.

            If we store only this type_id, then we have lost the connection to
            the child template, as Hydra will only store the link to the type
            which is in the parent, and therefore hydra will think that the
            network was created using the parent template.

        """

        #first create a template
        parent_template_j = client.testutils.create_template()
        parent_network_type = list(filter(lambda x: x.resource_type=='NETWORK', parent_template_j.templatetypes))[0]

        #and a child
        child_template_j = client.testutils.create_child_template(parent_template_j.id)
        child_template_j = client.get_template(child_template_j.id)

        child_network_type = list(filter(lambda x: x.resource_type=='NETWORK', child_template_j.templatetypes))[0]

        #check that the child has a network type of its own
        assert child_network_type.id != parent_network_type.id
        assert child_network_type.parent_id == parent_network_type.id


        node_type = list(filter(lambda x: x.resource_type=='NODE', child_template_j.templatetypes))[0]
        #now create a network using the child's network type
        #The network type needs to be linked to the template ID of the
        #child so we can find the correct template

        project_j = client.add_project(JSONObject({'name': 'Template Inheritance Project'}))

        network = JSONObject({
            'project_id': project_j.id,
            'name': 'Test Network with Child Template',
            'types' : [{'id': child_network_type.id,
                        'child_template_id': child_template_j.id}],
            'nodes' : [{'name': 'Node1', 'x':0, 'y':0, 'types':[{'id':node_type.id}]}]
        })

        new_network = client.add_network(network)
        #Now do a get network to verify the network's there
        requested_network = client.get_network(new_network.id)

        assert requested_network.nodes[0].types[0].child_template_id == child_template_j.id

        #Make sure that the correct attributes have been added from the child (inherihted
        #from the parent)
        attr_ids_a = set(a.attr_id for a in requested_network.attributes)
        attr_ids_b = set(ta.attr_id for ta in parent_network_type.typeattrs)
        assert len(attr_ids_a.difference(attr_ids_b)) == 0


    def test_network_child_template_with_subclassed_type(self, client):
        """
        This tests a few operations involving making a derived type in a child template
        and verifies that the hydra_base.lib functions return a complete view of
        the resulting hierarchy.

          1. Create a Parent Template with 'Parent Node Type'
          2. Create a Child Template
          3. Create a Network using the Child Template
          4. Create a Child Template Node Type with parent type 'Parent Node Type'
          5. Create a Node instance of this type in the Network and verify correctness
          6. Create a Grandchild Template
          7. Create a Grandchild Template Node Type with parent type 'Child Template Node Type'
          8. Retrieve type information and verify the parent_ids attr contains both
             'Child Template Node Type' and 'Parent Node Type'
          9. Verify that the get_all_parent_types(ttype_id) lib function returns the
             correct Parent and GrandParent TemplateTypes

        """
        # Create Parent Template
        parent_template = client.testutils.create_template()

        # Create Child Template
        child_template = client.testutils.create_child_template(parent_template.id)
        child_template = client.get_template(child_template.id)

        child_network_type = list(filter(lambda x: x.resource_type=="NETWORK", child_template.templatetypes))[0]

        parent_node_type = list(filter(lambda x: x.resource_type=="NODE", child_template.templatetypes))[0]

        # Create Network using Child Template
        parent_node_name = "Original Parent Type Node"
        project_j = client.add_project(JSONObject({"name": "Child Template SubType Test Project"}))

        network = JSONObject({
            "project_id": project_j.id,
            "name": "Test Network with Child Template",
            "types" : [{"id": child_network_type.id,
                        "child_template_id": child_template.id}],
            "nodes" : [{"name": parent_node_name, "x": 0, "y": 0, "types":[{"id": parent_node_type.id}]}]
        })

        new_network = client.add_network(network)
        ret_network = client.get_network(new_network.id)

        # Add a subclassed Node type to the Child Template

        # 1. Make a Child Template Type
        templatetype = JSONObject()
        templatetype.name = "Child Template TT"
        templatetype.alias = "Child Template TT alias"
        templatetype.resource_type = "NODE"
        templatetype.template_id = child_template.id
        templatetype.parent_id = parent_node_type.id
        templatetype.layout = {"color": "green", "shapefile": "child_node.shp"}

        templatetype.typeattrs = []
        client.add_templatetype(templatetype)
        ret_tt = client.get_templatetype_by_name(child_template.id, "Child Template TT")

        # 2. Make a Node using the Child TemplateType
        node = JSONObject()
        node.id = -1
        node.name = "Child Template Added Node"
        node.description = "Child Template Node desc"
        node.x = 2
        node.y = 3

        node_types = []
        node_type = JSONObject()
        node_type.id  = ret_tt.id
        node_type.name = ret_tt.name

        node_types.append(node_type)
        node.types = node_types

        new_node = client.add_node(ret_network.id, node)

        # Verify Node has expected Type
        ret_child_node = client.get_node(new_node.id)
        ret_orig_node = client.get_node(ret_network.nodes[0].id)
        ret_child_template = client.get_template(child_template.id)
        ret_network = client.get_network(new_network.id)

        """
        ret_network now has Node with type.id==2, but
        ret_child_template.templatetypes has only Node type.id==6

        E.g. in the following, the original Parent Node Type (2) appears
        only in the parent_types, not in child_types even though these
        are the types of a Child Template.

        user_res_types = ("NODE", "LINK", "GROUP")
        ret_parent_template = client.get_template(parent_template.id)

        parent_types = set(tt.id for tt in ret_parent_template.templatetypes
                           if tt.resource_type.upper() in user_res_types)
        child_types = set(tt.id for tt in ret_child_template.templatetypes
                          if tt.resource_type.upper() in user_res_types)


        Now verify that the templates' parent_ids attr is complete...
        """

        # Make Grandchild Template with Node type derived from Child Node Type
        grandchild_template = client.testutils.create_child_template(ret_child_template.id)
        grandchild_template = client.get_template(grandchild_template.id)

        templatetype = JSONObject()
        templatetype.name = "Grandchild Template TT"
        templatetype.alias = "Grandchild Template TT alias"
        templatetype.resource_type = "NODE"
        templatetype.template_id = grandchild_template.id
        templatetype.parent_id = ret_child_node.types[0].type_id
        templatetype.layout = {"color": "blue", "shapefile": "grandchild_node.shp"}

        templatetype.typeattrs = []
        client.add_templatetype(templatetype)
        ret_tt = client.get_templatetype_by_name(grandchild_template.id, "Grandchild Template TT")

        # Retrieve the Grandchild template and verify both
        # 'Parent Node Type' and 'Child Node Type' are included
        # as parents of the 'Grandchild Node Type'
        ret_grandchild_template = client.get_template(grandchild_template.id)
        assert ret_orig_node.types[0].type_id in ret_grandchild_template.templatetypes[1].parent_ids
        assert ret_child_node.types[0].type_id in ret_grandchild_template.templatetypes[1].parent_ids
        # ...and no others...
        assert len(ret_grandchild_template.templatetypes[1].parent_ids) == 2

        # Verify that get_all_parent_types(ttype_id) returns the expected TemplateTypes...
        parents = client.get_all_parent_types(ret_grandchild_template.templatetypes[1].id)
        assert parents[0].id == ret_orig_node.types[0].type_id
        assert parents[1].id == ret_child_node.types[0].type_id
        # ...and no others...
        assert len(parents) == 2


    def test_delete_parent_type(self, client):
        """
            Test to ensure that when you delete a parent type, its child types
            are also deleted (or not depoending on the 'delete_children' flag)
        """
        #first create a template
        parent_template_j = client.testutils.create_template()
        parent_template_j = client.get_template(parent_template_j.id)
        parent_type = list(filter(lambda x: x.resource_type=='NODE', parent_template_j.templatetypes))[0]

        #and a child
        child_template_j = client.testutils.create_child_template(parent_template_j.id)
        child_template_j = client.get_template(child_template_j.id)

        child_type = client.add_child_templatetype(parent_type.id, child_template_j.id)


        #now delete the parent type, without force. This should error because there is a reference to it
        #from the child
        with pytest.raises(HydraError):
            client.delete_templatetype(parent_type.id)

        #now force the deletion
        client.delete_templatetype(parent_type.id, delete_children=True)


        updated_parent_template_j = client.get_template(parent_template_j.id)
        assert len(updated_parent_template_j.templatetypes) == len(parent_template_j.templatetypes) - 1
        assert child_type.id not in [t.id for t in updated_parent_template_j.templatetypes]


        updated_child_template_j = client.get_template(child_template_j.id)
        assert len(updated_child_template_j.templatetypes) == len(child_template_j.templatetypes) - 1
        assert child_type.id not in [t.id for t in updated_child_template_j.templatetypes]
        #all types in the child should now belong to the parent. The chid one is gone.
        for tt in updated_child_template_j.templatetypes:
            #there should always be a network type for the child
            if tt.resource_type == 'NETWORK':
                continue
            assert tt.resource_type is not None
            assert tt.name is not None
            assert tt.template_id==parent_template_j.id

    def test_get_child_template_by_name(self, client):

        #first create a template
        parent_template_j = client.testutils.create_template()
        parent_network_type = list(filter(lambda x: x.resource_type=='NETWORK', parent_template_j.templatetypes))[0]

        #and a child
        child_template_j = client.testutils.create_child_template(parent_template_j.id)
        child_template_j = client.get_template(child_template_j.id)


        child_template_2_j = client.get_template_by_name(child_template_j.name)

        assert len(parent_template_j.templatetypes) == len(child_template_j.templatetypes) == len(child_template_2_j.templatetypes)


