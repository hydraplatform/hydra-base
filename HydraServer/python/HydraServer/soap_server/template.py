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
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import Integer, Integer32, Unicode
from spyne.decorator import rpc
from hydra_complexmodels import Template,\
TemplateType,\
TypeAttr,\
TypeSummary,\
ResourceTypeDef,\
ValidationError

from hydra_base import HydraService
from HydraServer.lib import template

class TemplateService(HydraService):
    """
        The template SOAP service
    """

    @rpc(Unicode, _returns=Template)
    def upload_template_xml(ctx, template_xml):
        """
            Add the template, type and typeattrs described
            in an XML file.

            Delete type, typeattr entries in the DB that are not in the XML file
            The assumption is that they have been deleted and are no longer required.
        """
        tmpl_i = template.upload_template_xml(template_xml,
                                              **ctx.in_header.__dict__)

        return Template(tmpl_i)

    @rpc(Unicode, Integer, _returns=SpyneArray(TypeSummary))
    def get_matching_resource_types(ctx, resource_type, resource_id):
        """
            Get the possible types of a resource by checking its attributes
            against all available types.

            @returns A list of TypeSummary objects.
        """
        types = template.get_matching_resource_types(resource_type,
                                                     resource_id,
                                                     **ctx.in_header.__dict__)
        ret_types = [TypeSummary(ts) for ts in types]
        return ret_types

    @rpc(SpyneArray(ResourceTypeDef),
         _returns=SpyneArray(TemplateType))
    def assign_types_to_resources(ctx, resource_types):
        """Assign new types to list of resources.
        This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
        """
        types = template.assign_types_to_resources(resource_types,
                                                    **ctx.in_header.__dict__)
        ret_val = [TemplateType(t) for t in types]
        return ret_val


    @rpc(Integer, Unicode, Integer, _returns=TypeSummary)
    def assign_type_to_resource(ctx, type_id, resource_type, resource_id):
        """Assign new type to a resource. This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
        """
        templatetype = template.assign_type_to_resource(type_id,
                                                       resource_type,
                                                       resource_id,
                                                       **ctx.in_header.__dict__)
        ret_type = TypeSummary()
        ret_type.name = templatetype.type_name
        ret_type.id   = templatetype.type_id
        ret_type.template_name = templatetype.template.template_name 
        ret_type.template_id = templatetype.template.template_id 

        return ret_type 

    @rpc(Integer, Integer, _returns=Unicode)
    def apply_template_to_network(ctx, template_id, network_id):
        """
            Given a template and a network, try to match up and assign
            all the nodes & links in the network to the types in the template
        """
        template.apply_template_to_network(template_id,
                                           network_id,
                                           **ctx.in_header.__dict__)
        return 'OK'
    
    @rpc(Integer, Integer, Unicode(pattern="[YN]", default='N'), _returns=Unicode)
    def remove_template_from_network(ctx, network_id, template_id, remove_attrs):
        """
            Given a template and a network, try to match up and assign
            all the nodes & links in the network to the types in the template
        """
        template.remove_template_from_network(network_id,
                                           template_id,
                                           remove_attrs,
                                           **ctx.in_header.__dict__)
        return 'OK'


    @rpc(Integer, Unicode, Integer, _returns=Unicode)
    def remove_type_from_resource(ctx,  type_id, resource_type, resource_id):
        """

            Remove a resource type trom a resource
        """
        template.remove_type_from_resource(type_id,
                                           resource_type,
                                           resource_id,
                                           **ctx.in_header.__dict__)
        
        return 'OK'

    @rpc(Template, _returns=Template)
    def add_template(ctx, tmpl):
        """
            Add template and a type and typeattrs.
        """
        tmpl_i = template.add_template(tmpl,
                                      **ctx.in_header.__dict__)

        return Template(tmpl_i)

    @rpc(Template, _returns=Template)
    def update_template(ctx, tmpl):
        """
            Update template and a type and typeattrs.
        """
        tmpl_i = template.update_template(tmpl,
                                           **ctx.in_header.__dict__)
        return Template(tmpl_i)


    @rpc(Integer, _returns=Template)
    def delete_template(ctx, template_id):
        """
            Update template and a type and typeattrs.
        """
        template.delete_template(template_id,
                                           **ctx.in_header.__dict__)
        return 'OK'

    @rpc(_returns=SpyneArray(Template))
    def get_templates(ctx):
        """
            Get all resource template templates.
        """
        tmpls = template.get_templates(**ctx.in_header.__dict__)
        ret_templates = [Template(t) for t in tmpls]

        return ret_templates

    @rpc(Integer, Integer, _returns=Unicode)
    def remove_attr_from_type(ctx, type_id, attr_id):
        """

            Remove an attribute from a type
        """
        success = 'OK'
        template.remove_attr_from_type(type_id,
                                       attr_id,
                                       **ctx.in_header.__dict__)
        return success

    @rpc(Integer, _returns=Template)
    def get_template(ctx, template_id):
        """
            Get a specific resource template template, either by ID or name.
        """
        tmpl_i = template.get_template(template_id,
                                      **ctx.in_header.__dict__)
        tmpl = Template(tmpl_i)

        return tmpl

    @rpc(Unicode, _returns=Template)
    def get_template_by_name(ctx, template_name):
        """
            Get a specific resource template, either by ID or name.
        """
        tmpl_i = template.get_template_by_name(template_name,
                                              **ctx.in_header.__dict__)
        if tmpl_i is not None:
            tmpl = Template(tmpl_i)

            return tmpl
        else:
            return None

    @rpc(TemplateType, _returns=TemplateType)
    def add_templatetype(ctx, templatetype):
        """
            Add a template type with typeattrs.
        """

        tmpl_type = template.add_templatetype(templatetype,
                                              **ctx.in_header.__dict__)

        return TemplateType(tmpl_type)

    @rpc(TemplateType, _returns=TemplateType)
    def update_templatetype(ctx, templatetype):
        """
            Update a resource type and its typeattrs.
            New typeattrs will be added. typeattrs not sent will be ignored.
            To delete typeattrs, call delete_typeattr
        """
        type_i = template.update_templatetype(templatetype,
                                              **ctx.in_header.__dict__)
        return TemplateType(type_i)

    @rpc(Integer, _returns=Template)
    def delete_templatetype(ctx, type_id):
        """
            Update template and a type and typeattrs.
        """
        template.delete_templatetype(type_id,
                                           **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=TemplateType)
    def get_templatetype(ctx, type_id):
        """
            Get a specific resource type by ID.
        """
        type_i = template.get_templatetype(type_id,
                                           **ctx.in_header.__dict__)
        templatetype = TemplateType(type_i)
        return templatetype

    @rpc(Integer, Unicode, _returns=TemplateType)
    def get_templatetype_by_name(ctx, template_id, type_name):
        """
            Get a specific resource type by name.
        """

        type_i = template.get_templatetype_by_name(template_id, 
                                                   type_name,
                                                   **ctx.in_header.__dict__)
        tmpltype = TemplateType(type_i)

        return tmpltype

    @rpc(TypeAttr, _returns=TemplateType)
    def add_typeattr(ctx, typeattr):
        """
            Add an typeattr to an existing type.
        """
        updated_template_type = template.add_typeattr(typeattr,
                                           **ctx.in_header.__dict__)

        ta = TemplateType(updated_template_type)
        return ta


    @rpc(TypeAttr, _returns=Unicode)
    def delete_typeattr(ctx, typeattr):
        """
            Remove an typeattr from an existing type
        """
        success = 'OK'
        template.delete_typeattr(typeattr,
                                 **ctx.in_header.__dict__)
        return success

    @rpc(Integer, _returns=Unicode)
    def get_network_as_xml_template(ctx, network_id):
        """
            Turn an existing network into an xml template
            using its attributes.
            If an optional scenario ID is passed in, default
            values will be populated from that scenario.
        """
        template_xml = template.get_network_as_xml_template(network_id,
                                                            **ctx.in_header.__dict__)

        return template_xml

    @rpc(Integer, Integer, Integer, _returns=ValidationError)
    def validate_attr(ctx, resource_attr_id, scenario_id, template_id):
        """
            Validate that the value of a specified resource attribute is valid
            relative to the data restrictions specified on the template. If no
            template is specified, (set as null), then validation will be made
            against every template on the network.
        """
        error_dict = template.validate_attr(resource_attr_id, scenario_id, template_id)
        if error_dict is None:
            return None

        error = ValidationError(
                ref_key = error_dict.get('ref_key'),
                 ref_id  = error_dict.get('ref_id'), 
                 ref_name = error_dict.get('ref_name'),
                 resource_attr_id = error_dict.get('resource_attr_id'),
                 attr_id          = error_dict.get('attr_id'),
                 attr_name        = error_dict.get('attr_name'),
                 dataset_id       = error_dict.get('dataset_id'),
                 scenario_id=error_dict.get('scenario_id'),
                 template_id=error_dict.get('template_id'),
                 error_text=error_dict.get('error_text')
        )
        return error

    @rpc(SpyneArray(Integer32), Integer, Integer, _returns=SpyneArray(ValidationError))
    def validate_attrs(ctx, resource_attr_ids, scenario_id, template_id):
        errors = []
        error_dicts = template.validate_attrs(resource_attr_ids, scenario_id, template_id)
        for error_dict in error_dicts:
            error = ValidationError(
                    ref_key = error_dict.get('ref_key'),
                     ref_id  = error_dict.get('ref_id'), 
                     ref_name = error_dict.get('ref_name'),
                     resource_attr_id = error_dict.get('resource_attr_id'),
                     attr_id          = error_dict.get('attr_id'),
                     attr_name        = error_dict.get('attr_name'),
                     dataset_id       = error_dict.get('dataset_id'),
                     scenario_id=error_dict.get('scenario_id'),
                     template_id=error_dict.get('template_id'),
                     error_text=error_dict.get('error_text')
            )
            errors.append(error)
            
        return errors

    @rpc(Integer, Integer, _returns=SpyneArray(ValidationError))
    def validate_scenario(ctx, scenario_id, template_id):
        errors = []
        error_dicts = template.validate_scenario(scenario_id, template_id)
        for error_dict in error_dicts:
            error = ValidationError(
                    ref_key = error_dict.get('ref_key'),
                     ref_id  = error_dict.get('ref_id'), 
                     ref_name = error_dict.get('ref_name'),
                     resource_attr_id = error_dict.get('resource_attr_id'),
                     attr_id          = error_dict.get('attr_id'),
                     attr_name        = error_dict.get('attr_name'),
                     dataset_id       = error_dict.get('dataset_id'),
                     scenario_id=error_dict.get('scenario_id'),
                     template_id=error_dict.get('template_id'),
                     error_text=error_dict.get('error_text')
            )
            errors.append(error)
            
        return errors

    @rpc(Integer, Integer, Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(Unicode))
    def validate_network(ctx, network_id, template_id, scenario_id):
        errors = template.validate_network(network_id, template_id, scenario_id)
        return errors

    @rpc(Integer, Integer, _returns=SpyneArray(Unicode))
    def check_type_compatibility(ctx, type_1_id, type_2_id): 
        errors = template.check_type_compatibility(type_1_id, type_2_id)
        return errors
