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


    def test_add_scoped_child_type_to_project(self, client):
        """
            This tests that a child templatetype can be added to a template type but in
            the same template. THis is for when a template can be expanded, but in the context
            of a project, so when accessing a template through the context of a project, it can be
            extended without creating a whole new child template. Instead scoped child attributes
            within the child template are used, so changes from the main template are
            only applied in this project.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        templatetype_to_scope = template_j.templatetypes[0]

        #Add a type to the child
        _ = client.testutils.create_scoped_templatetype(
            template_j.id,
            templatetype_to_scope.id,
            project.id)

        template_no_scoping = client.get_template(template_j.id)
        for templatetype in template_no_scoping.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                assert templatetype.layout == templatetype_to_scope.layout
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

        template_project_scoped = client.get_template(template_j.id, project.id)
        for templatetype in template_project_scoped.templatetypes:
            if templatetype.name == templatetype_to_scope.name:
                #Check that the scoped temoplate data takes precedence over
                #the higher level parent data.
                assert templatetype.layout == {"color": "red", "shapefile": "blah.shp"}
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

    def test_add_scope_type_to_project(self, client):
        """
            This tests that a project-specific non-child templatetype can be added to a
            template.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        #Add a type to the child
        templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            project.id)

        template_no_scoping = client.get_template(template_j.id)
        assert templatetype_j.id not in [t.id for t in  template_no_scoping.templatetypes]

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        assert templatetype_j.id in [t.id for t in  template_project_scoped.templatetypes]

    def test_add_scope_type_to_network(self, client):
        """
            This tests that a project-specific non-child templatetype can be added to a
            template.
        """

        template_j = client.testutils.create_template()
        network = client.testutils.create_network_with_data(new_proj=True)

        #Add a type to the child
        templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            network_id=network.id)

        template_no_scoping = client.get_template(template_j.id)
        assert templatetype_j.id not in [t.id for t in  template_no_scoping.templatetypes]

        template_project_scoped = client.get_template(template_j.id, network_id=network.id)
        assert templatetype_j.id in [t.id for t in  template_project_scoped.templatetypes]


    def test_scope_type_to_network(self, client):
        """
            This tests that a child templatetype can be added to a template type but in
            the same template. THis is for when a template can be expanded, but in the context
            of a network, so when accessing a template through the context of a network, it can be
            extended without creating a whole new child template. Instead scoped child attributes
            within the child template are used, so changes from the main template are
            only applied in this network.
        """

        template_j = client.testutils.create_template()

        network = client.testutils.create_network_with_data(new_proj=True)

        templatetype_to_scope = template_j.templatetypes[0]

        #Add a type to the child
        templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            templatetype_to_scope.id,
            network.id)

        template_no_scoping = client.get_template(template_j.id)
        for templatetype in template_no_scoping.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                assert templatetype.layout == templatetype_to_scope.layout
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

        template_network_scoped = client.get_template(template_j.id, network.id)
        for templatetype in template_network_scoped.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                #Check that the scoped template data takes precdence over
                #the higher level parent data.
                assert templatetype.layout == {"color": "red", "shapefile": "blah.shp"}
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

    def test_scope_type_to_project_and_network(self, client):
        """
            This tests that a child templatetype can be added to a template type but in
            the same template.
            This tests that the inheritance works correctly for a tempaltetype which is
            scoped first to a project, then to a network, which should prioritise changes
            as the scope becomes closer to the network.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        network = client.testutils.create_network_with_data(project_id=project.id)

        templatetype_to_scope = template_j.templatetypes[0]

        with pytest.raises(HydraError):
            client.testutils.create_scoped_templatetype(
                template_j.id,
                templatetype_to_scope.id,
                project_id=project.id, #can't scope to both a project and network
                network_id=network.id)


        #Add a type to the child
        project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            templatetype_to_scope.id,
            project_id=project.id,
            layout={"color": "blue", "shapefile": "blah.shp"})

        #Add a type to the child
        network_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            templatetype_to_scope.id,
            network_id=network.id,
            layout={"color": "green"})

        template_no_scoping = client.get_template(template_j.id)
        for templatetype in template_no_scoping.templatetypes:
            if templatetype.name == templatetype_to_scope.name:
                assert templatetype.layout == templatetype_to_scope.layout
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        for templatetype in template_project_scoped.templatetypes:
            if templatetype.id == project_scoped_templatetype_j.parent_id:
                #Check that the scoped temoplate data takes precdence over
                #the higher level parent data.
                assert templatetype.layout == {"color": "blue", "shapefile": "blah.shp"}
                break
        else:
            raise Exception(f"Unable to find project scoped templatetype {project_scoped_templatetype_j.id}")

        template_network_scoped = client.get_template(template_j.id, network_id=network.id)
        for templatetype in template_network_scoped.templatetypes:
            if templatetype.id == network_scoped_templatetype_j.parent_id:
                #Check that the scoped temoplate data takes precdence over
                #the higher level parent data.
                assert templatetype.layout == {"color": "green"}
                break
        else:
            raise Exception(f"Unable to find network scoped templatetype {network_scoped_templatetype_j.id}")

    def test_add_new_scope_type_to_project_and_network(self, client):
        """
            This tests that a new templatetype can be added to a template in the scope of a project,
            and a network in that project, and that the template will contain both the project scoped
            type and the network scoped type.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        network = client.testutils.create_network_with_data(project_id=project.id)

        #Add a type to the child
        project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            project_id=project.id,
            layout={"color": "blue", "shapefile": "blah.shp"})

        #Add a type to the child
        network_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            network_id=network.id,
            layout={"color": "green"})

        template_no_scoping = client.get_template(template_j.id)
        assert project_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_project_scoped.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_project_scoped.templatetypes]

        template_network_scoped = client.get_template(template_j.id, project_id=project.id, network_id=network.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_network_scoped.templatetypes]
        assert network_scoped_templatetype_j.id in [t.id for t in template_network_scoped.templatetypes]

    def test_add_new_scope_type_with_typeattrs_to_project_and_network(self, client):
        """
            This tests that a new templatetype can be added to a template in the scope of a project,
            and a network in that project, and that the template will contain both the project scoped
            type and the network scoped type.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        network = client.testutils.create_network_with_data(project_id=project.id)

        #Add a type to the child
        project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            project_id=project.id,
            layout={"color": "blue", "shapefile": "blah.shp"})

        #Add a type to the child
        project_scoped_typeattr_j = client.testutils.create_typeattr(
            project_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            project_id=project.id)

        #Add a type attribute to the child
        network_scoped_typeattr_on_project_type_j = client.testutils.create_typeattr(
            project_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            network_id=network.id)

        #Add a new type, scoped to the network
        network_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            network_id=network.id,
            layout={"color": "green"})

        #Add a typeattribute to the new network-scoped type
        network_scoped_typeattr_j = client.testutils.create_typeattr(
            network_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            network_id=network.id)

        #The scoped template type shoul dhave the typeattr
        network_scoped_templatetype_j = client.get_templatetype(type_id=network_scoped_templatetype_j.id,
                                                                project_id=project.id,
                                                                network_id=network.id)

        template_no_scoping = client.get_template(template_j.id)
        assert project_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_project_scoped.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_project_scoped.templatetypes]
        assert project_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==project_scoped_templatetype_j.id, template_project_scoped.templatetypes))[0].typeattrs]
        assert network_scoped_typeattr_on_project_type_j.id not in [ta.id for ta in list(filter(lambda x:x.id==project_scoped_templatetype_j.id, template_project_scoped.templatetypes))[0].typeattrs]

        template_network_scoped = client.get_template(template_j.id, project_id=project.id, network_id=network.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_network_scoped.templatetypes]
        assert network_scoped_templatetype_j.id in [t.id for t in template_network_scoped.templatetypes]
        assert project_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==project_scoped_templatetype_j.id, template_network_scoped.templatetypes))[0].typeattrs]
        assert network_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==network_scoped_templatetype_j.id, template_network_scoped.templatetypes))[0].typeattrs]
        assert network_scoped_typeattr_j.id in [ta.id for ta in network_scoped_templatetype_j.typeattrs]

    def test_add_child_type_with_typeattrs_to_project_child_project_and_network(self, client):
        """
            This tests that a child templatetype can be added to a template in the scope of a project,
            a child project in that project and a network in that child project, and that the template returned for a network
            will contain the correct type modifications from that hierarchy.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()
        child_project = client.testutils.create_project(name="child project", parent_id=project.id)

        network = client.testutils.create_network_with_data(project_id=child_project.id)

        type_to_scope = template_j.templatetypes[0].id

        #Add a type scoped to the project
        project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            type_id=type_to_scope, # we're creating the child of an existing type scoped to the project
            project_id=project.id,
            layout={"color": "magenta", "shapefile": "parenscoped.shp", "scope": "1"})

        #Add a typeattr scopedto the project type
        project_scoped_typeattr_j = client.testutils.create_typeattr(
            project_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            project_id=project.id)

        #add type scoped to the child project
        child_project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            type_id=type_to_scope,# we're creating the child of an existing type scoped to the child project
            project_id=child_project.id,
            layout={"color": "purple", "shapefile": "scopedshapefile.shp"})

        #Add a type attr to the child-project-scoped type
        child_project_scoped_typeattr_j = client.testutils.create_typeattr(
            child_project_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            project_id=child_project.id)

        #Add a type scoped to the network
        network_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            type_id=type_to_scope,# we're creating the child of an existing type scoped to the network
            network_id=network.id,
            layout={"color": "green"})

        #Add a typeattr to the network-scoped type
        network_scoped_typeattr_j = client.testutils.create_typeattr(
            network_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            network_id=network.id)

        template_no_scoping = client.get_template(template_j.id)
        assert project_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        scoped_type = list(filter(lambda x : x.id==type_to_scope, template_project_scoped.templatetypes))[0]
        assert scoped_type.layout['color'] == 'magenta'
        assert child_project_scoped_templatetype_j.id not in [t.id for t in template_project_scoped.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_project_scoped.templatetypes]
        assert project_scoped_typeattr_j.id in  [ta.id for ta in scoped_type.typeattrs]

        template_child_project_scoped = client.get_template(template_j.id, project_id=child_project.id)
        scoped_type = list(filter(lambda x : x.id==type_to_scope, template_child_project_scoped.templatetypes))[0]
        assert scoped_type.layout['color'] == 'purple'
        assert scoped_type.layout.get('scope') is None
        assert project_scoped_templatetype_j.id not in [t.id for t in template_child_project_scoped.templatetypes]
        assert child_project_scoped_templatetype_j.id not in [t.id for t in template_child_project_scoped.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_child_project_scoped.templatetypes]
        assert child_project_scoped_typeattr_j.id in  [ta.id for ta in scoped_type.typeattrs]

        template_network_scoped = client.get_template(template_j.id, project_id=child_project.id, network_id=network.id)
        scoped_type = list(filter(lambda x : x.id==type_to_scope, template_network_scoped.templatetypes))[0]
        assert scoped_type.layout['color'] == 'green'
        assert scoped_type.layout.get('shapefile') is None
        assert project_scoped_typeattr_j.id in  [ta.id for ta in scoped_type.typeattrs]
        assert network_scoped_typeattr_j.id in  [ta.id for ta in scoped_type.typeattrs]
        assert child_project_scoped_typeattr_j.id in  [ta.id for ta in scoped_type.typeattrs]

    def test_add_new_scope_type_with_typeattrs_to_project_child_project_and_network(self, client):
        """
            This tests that a new templatetype can be added to a template in the scope of a project,
            and a network in that project, and that the template will contain both the project scoped
            type and the network scoped type.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()
        child_project = client.testutils.create_project(name="child project", parent_id=project.id)

        network = client.testutils.create_network_with_data(project_id=child_project.id)

        #Add a type scoped to the project
        project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            project_id=project.id,
            layout={"color": "blue", "shapefile": "blah.shp"})

        #Add a typeattr scopedto the project type
        project_scoped_typeattr_j = client.testutils.create_typeattr(
            project_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            project_id=project.id)

        #add type scoped to the child project
        child_project_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            project_id=child_project.id,
            layout={"color": "purple", "shapefile": "blah.shp"})

        #Add a type attr to the child-project-scoped type
        child_project_scoped_typeattr_j = client.testutils.create_typeattr(
            child_project_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            project_id=child_project.id)

        #Add a type scoped to the network
        network_scoped_templatetype_j = client.testutils.create_scoped_templatetype(
            template_j.id,
            None,
            network_id=network.id,
            layout={"color": "green"})

        #Add a typeattr to the network-scoped type
        network_scoped_typeattr_j = client.testutils.create_typeattr(
            network_scoped_templatetype_j.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            network_id=network.id)

        template_no_scoping = client.get_template(template_j.id)
        assert project_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_no_scoping.templatetypes]

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_project_scoped.templatetypes]
        assert child_project_scoped_templatetype_j.id not in [t.id for t in template_project_scoped.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_project_scoped.templatetypes]
        assert project_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==project_scoped_templatetype_j.id, template_project_scoped.templatetypes))[0].typeattrs]


        template_child_project_scoped = client.get_template(template_j.id, project_id=child_project.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_child_project_scoped.templatetypes]
        assert child_project_scoped_templatetype_j.id in [t.id for t in template_child_project_scoped.templatetypes]
        assert network_scoped_templatetype_j.id not in [t.id for t in template_child_project_scoped.templatetypes]
        assert child_project_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==child_project_scoped_templatetype_j.id, template_child_project_scoped.templatetypes))[0].typeattrs]

        template_network_scoped = client.get_template(template_j.id, project_id=project.id, network_id=network.id)
        assert project_scoped_templatetype_j.id in [t.id for t in template_network_scoped.templatetypes]
        assert network_scoped_templatetype_j.id in [t.id for t in template_network_scoped.templatetypes]
        assert project_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==project_scoped_templatetype_j.id, template_network_scoped.templatetypes))[0].typeattrs]
        assert network_scoped_typeattr_j.id in  [ta.id for ta in list(filter(lambda x:x.id==network_scoped_templatetype_j.id, template_network_scoped.templatetypes))[0].typeattrs]

    def test_add_scoped_typeattr_to_project(self, client):
        """
            This tests that a child type attribute can be added to a template type but in
            the same template. This is for when a template can be expanded, but in the context
            of a project, so when accessing a template through the context of a project, it can be
            extended without creating a whole new child template. Instead scoped child attributes
            within the child template are used, so changes from the main template are
            only applied in this project.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        templatetype_to_scope = template_j.templatetypes[0]

        #Add a type to the child
        scoped_typeattr_j = client.testutils.create_typeattr(
            templatetype_to_scope.id,
            typeattr_id=None, # we're creating a new typeattr, not extending an existing one.
            project_id=project.id)

        template_no_scoping = client.get_template(template_j.id)
        for templatetype in template_no_scoping.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                assert scoped_typeattr_j.id not in [ta.id for ta in templatetype.typeattrs]
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        for templatetype in template_project_scoped.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                assert scoped_typeattr_j.id in [ta.id for ta in templatetype.typeattrs]
                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

    def test_scope_typeattr_to_project(self, client):
        """
            This tests that a child type attribute can be added to a template type as the child of another type attribute
            but in the same template. This is for when a template can be expanded, but in the context
            of a project, so when accessing a template through the context of a project, it can be
            extended without creating a whole new child template. Instead scoped child attributes
            within the child template are used, so changes from the main template are
            only applied in this project.
        """

        template_j = client.testutils.create_template()

        project = client.testutils.create_project()

        templatetype_to_scope = template_j.templatetypes[0]
        typeattr_to_scope = templatetype_to_scope.typeattrs[0]

        #Add a child type type, scoped to the project
        scoped_typeattr_j = client.testutils.create_typeattr(
            templatetype_to_scope.id,
            typeattr_to_scope.id,
            project.id)

        template_no_scoping = client.get_template(template_j.id)
        for templatetype in template_no_scoping.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                for ta in templatetype.typeattrs:
                    if ta.id == typeattr_to_scope.id:
                        assert ta.unit_id == typeattr_to_scope.unit_id
                        break
                else:
                    raise Exception(f"Unable to find typeattr {typeattr_to_scope.id}")

                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

        #find the scoped typeattr and verify that it's got a unit ID
        template_project_scoped = client.get_template(template_j.id, project_id=project.id)
        for templatetype in template_project_scoped.templatetypes:
            if templatetype.id == templatetype_to_scope.id:
                for ta in templatetype.typeattrs:
                    if ta.id == typeattr_to_scope.id:
                        assert ta.unit_id == scoped_typeattr_j.unit_id
                        break
                else:
                    raise Exception(f"Unable to find scoped typeattr {typeattr_to_scope.id}")

                break
        else:
            raise Exception(f"Unable to find templatetype {templatetype_to_scope.id}")

    def test_move_network_to_compatible_project(self, client):
        """
            Test that a network created in a project which contains
            custom node types or custom attributes can be moved
            to another project so long as the target project has
            access ot the same custom types and attributes.
        """
        #create template A
        template_a = client.testutils.create_template(name="Template A")

        #create project A
        project_a = client.testutils.create_project(name="Project A")

        #create project B -- child of A
        project_b = client.testutils.create_project(name="Project B", parent_id=project_a.id)
        #create project C -- child of A
        project_c = client.testutils.create_project(name="Project C", parent_id=project_a.id)
        """
        CHECK TYPE COMPATIBILITY
        """
        #create a custom node type from template A in project A
        scoped_templatetype = client.testutils.create_scoped_templatetype(template_a.id, project_id=project_a.id)

        #Create a network in project B using the custom node type
        scoped_template = client.get_template(template_a.id, project_id=project_b.id)
        network_with_scoped_type = client.testutils.create_network_with_data(project_id=project_b.id, template=scoped_template)

        network_with_scoped_type = client.get_network(network_with_scoped_type.id)

        found_node_with_scoped_type = False
        for node in network_with_scoped_type.nodes:
            if scoped_templatetype.id == node.types[0].id:
                found_node_with_scoped_type = True
        assert found_node_with_scoped_type == True
        #Move the network to project C. This is allowed as they both

        client.move_network(network_with_scoped_type.id, project_c.id)
        #have access to the same types.

        """
        CHECK TYPE ATTRIBUTE COMPATIBILITY
        """
        node_type = list(filter(lambda x:x.resource_type=='NODE', template_a.templatetypes))[0]
        #create a new type attribute from template A in project A
        scopedtypeattr = client.testutils.create_typeattr(type_id=node_type.id, project_id=project_a.id)
        #Create a network in project B using the custom node type
        scoped_template = client.get_template(template_a.id, project_id=project_b.id)
        network_with_scoped_typeattr = client.testutils.create_network_with_data(project_id=project_b.id, template=scoped_template)

        network_with_scoped_typeattr = client.get_network(network_with_scoped_typeattr.id)
        found_node_with_scoped_typeattr = False
        for node in network_with_scoped_typeattr.nodes:
            if node_type.id == node.types[0].id:
                if scopedtypeattr.attr_id in [ra.attr_id for ra in node.attributes]:
                    found_node_with_scoped_typeattr = True
        assert found_node_with_scoped_typeattr == True

        #Move thhe network to project C. This is allowed as they both
        #have access to the same types.
        client.move_network(network_with_scoped_typeattr.id, project_c.id)

    def test_move_network_to_incompatible_project(self, client):
        """
            Test that a network created in a project which contains
            custom node types or custom attributes cannot be moved
            to another project which does not contain them.
        """

        #create template A
        template_a = client.testutils.create_template(name="Template For network Incompatibiltiy Testing")
        #create project A
        project_a = client.testutils.create_project(name="Project A for network incompatibility testing")
        #create project B
        project_b = client.testutils.create_project(name="Project B for network incompatibility testing")

        """
        CHECK TYPE INCOMPATIBILITY
        """

        #create a custom node type from template A in project A
        scoped_templatetype = client.testutils.create_scoped_templatetype(template_a.id, project_id=project_a.id)

        #Create a network in project A using the custom node type
        scoped_template = client.get_template(template_a.id, project_id=project_a.id)
        network_with_scoped_type = client.testutils.create_network_with_data(project_id=project_a.id, template=scoped_template)
        network_with_scoped_type = client.get_network(network_with_scoped_type.id)

        #Move the network to project B. This should fail as they both
        #have access to the same types.
        with pytest.raises(HydraError):
            client.move_network(network_with_scoped_type.id, project_b.id)

        """
        CHECK TYPE ATTRIBUTE INCOMPATIBILITY
        """
        node_type = list(filter(lambda x:x.resource_type=='NODE', template_a.templatetypes))[0]
        #create a custom type attribute from template A in project A
        scopedtypeattr = client.testutils.create_typeattr(type_id=node_type.id, project_id=project_a.id)

        #Create a network in project A using the custom node type
        scoped_template = client.get_template(template_a.id, project_id=project_a.id)
        network_with_scoped_typeattr = client.testutils.create_network_with_data(project_id=project_a.id, template=scoped_template)
        network_with_scoped_typeattr = client.get_network(network_with_scoped_typeattr.id)

        #Move thhe network to project B. This should fail as they both
        #have access to the same types.
        with pytest.raises(HydraError):
            client.move_network(network_with_scoped_typeattr.id, project_b.id)

    def test_move_project_to_compatible_project(self, client):
        """
            Test that a project created in a project which contains
            custom node types or custom attributes can be moved
            to another project so long as the target project has
            access ot the same custom types and attributes.
        """

        #create template A
        template_a = client.testutils.create_template(name="Template For project compatibiltiy Testing")
        #create project A
        project_a = client.testutils.create_project(name="Project A for project compatibility testing")
        #create project B -- child of A
        project_b = client.testutils.create_project(name="Project B for project compatibility testing", parent_id=project_a.id)
        #create project C -- child of A
        project_c = client.testutils.create_project(name="Project C for project compatibility testing", parent_id=project_a.id)

        """
        CHECK TYPE COMPATIBILITY
        """

        #create a custom node type from template A in project A
        parent_scoped_templatetype = client.testutils.create_scoped_templatetype(template_a.id, project_id=project_a.id)

        #Create a network in project B using the custom node type
        projb_network = client.testutils.create_network_with_data(project_id=project_b.id, template=template_a)

        #Move project B to project C. This is allowed as they both
        #have access to the same types.
        client.move_network(projb_network.id, project_c.id)

        """
        CHECK TYPE ATTRIBUTE COMPATIBILITY
        """
        type_to_scope = template_a.templatetypes[0]
        #create a custom type attribute from template A in project A
        parent_scoped_typeattr = client.testutils.create_typeattr(
            type_id=type_to_scope.id,
            project_id=project_a.id
        )

        #Create a network in project B using the custom node type
        projb_network2 = client.testutils.create_network_with_data(project_id=project_b.id, template=template_a)


        #Move project B to project C. This is allowed as they both
        #have access to the same types.
        client.move_network(projb_network2.id, project_c.id)

    def test_move_project_to_incompatible_project(self, client):
        """
            Test that a project created in a project which contains
            custom node types or custom attributes cannot be moved
            to another project which does not contain them.
        """


        #create template A
        template_a = client.testutils.create_template(name="Template For project Incompatibiltiy Testing")
        #create project A
        project_a = client.testutils.create_project(name="Project A for project incompatibility testing")
        #create project B -- child of A
        project_b = client.testutils.create_project(name="Project B for project incompatibility testing", parent_id=project_a.id)
        #create project C -- not a child of A
        project_c = client.testutils.create_project(name="Project C for project incompatibility testing")

        """
        CHECK TYPE INCOMPATIBILITY
        """

        #create a custom node type from template A in project A
        parent_scoped_templatetype = client.testutils.create_scoped_templatetype(template_a.id, project_id=project_a.id)

        #Create a network in project B using the custom node type
        projb_network = client.testutils.create_network_with_data(project_id=project_b.id, template=template_a)

        #Move project B to project C. This is not allowed
        # as project C does not have access to the same types.
        with pytest.raises(HydraError):
            client.move_network(projb_network.id, project_c.id)

        """
        CHECK TYPE ATTRIBUTE INCOMPATIBILITY
        """
        type_to_scope = template_a.templatetypes[0]
        #create a custom type attribute from template A in project A
        parent_scoped_typeattr = client.testutils.create_typeattr(
            type_id=type_to_scope.id,
            project_id=project_a.id
        )

        #Create a network in project B using the custom type attribute
        projb_network2 = client.testutils.create_network_with_data(project_id=project_b.id, template=template_a)

        #Move project B to project C. This is not allowed
        # as they both have access to the same types.
        with pytest.raises(HydraError):
            client.move_network(projb_network2.id, project_c.id)
