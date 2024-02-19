import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import noload

from hydra_base.exceptions import HydraError
from hydra_base.util.permissions import required_perms
from hydra_base import db
from hydra_base.db.model import (
    Project,
    Template,
    TemplateType,
    TypeAttr,
    ProjectTemplate,
    ResourceType,
    Network
)
from hydra_base.exceptions import ResourceNotFoundError
from hydra_base.lib.objects import JSONObject

LOG = logging.getLogger(__name__)

def _get_project(project_id, user_id, check_write=False):
    try:
        project_i = db.DBSession.query(Project).filter(
            Project.id == project_id).options(noload(Project.children)).one()

        if check_write is True:
            project_i.check_write_permission(user_id)
        else:
            ## to avoid doing 2 checks, only check this if the check write is not set
            project_i.check_read_permission(user_id)

        return project_i
    except NoResultFound:
        raise ResourceNotFoundError("Project %s not found"%(project_id))

def _get_template(template_id, user_id):
    try:
        template_i = db.DBSession.query(Template).filter(
            Template.id == template_id).one()

        return template_i
    except NoResultFound:
        raise ResourceNotFoundError("Template %s not found"%(template_id))


@required_perms('edit_project')
def add_project_template(template_id, project_id, **kwargs):
    """
      Add a link between a project and a template.
      A project can be scoped to multiple templates.
      Args:
          template_id (int): The ID of the template to which the project will be scoped
          project_id (int): The ID of the project to scope
      Returns:
          JSONObject of the ProjectTemplate entry:
              {'template_id': 123, 'project_id': 234}
    """

    user_id = kwargs.get('user_id')
    project_i  = _get_project(project_id, user_id, check_write=True)

    template_i = _get_template(template_id, user_id)

    project_template_i = ProjectTemplate(project_id=project_id, template_id=template_id)

    db.DBSession.add(project_template_i)

    db.DBSession.flush()

    return JSONObject(project_template_i)

@required_perms('get_project')
def get_project_templates(project_id, template_id=None, **kwargs):
    """
    Get the ProjectTemplate objects associated to a project id.
    Args:
        project_id (int): The ID of the project
        template_id (int) (optional) : The ID of the template by which to filter.
    Returns:
        ProjectTemplate object
    Raises:
        Hydra Error if no ProjectTemplate entry exists
    """

    user_id = kwargs.get('user_id')

    project_templates_qry = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id==project_id)

    if template_id is not None:
        project_templates_qry = project_templates_qry.filter(ProjectTemplate.template_id==template_id)

    project_templates_i = project_templates_qry.all()

    return [JSONObject(project_template_i) for project_template_i in project_templates_i]

@required_perms('edit_project')
def delete_project_template(template_id, project_id, **kwargs):
    """
      Delete a link between a project and a template.
      This can only be done if there are no networks in the project which use the template.
      If there are, then it will not be allowed, as this will create an inconsistency.

      Args:
          template_id: The template_id whose scope will be removed
          project_id: The project from which the template scope is being removed
      Returns:
         'OK'
      Raises:
          HydraError If a network exists in the project which uses the template to remove.
    """

    user_id = kwargs.get('user_id')
    project_i  = _get_project(project_id, user_id, check_write=True)
    networktype_id = db.DBSession.query(TemplateType.id).filter(
        TemplateType.template_id==template_id, TemplateType.resource_type=='NETWORK').one().id

    can_delete = _check_can_remove_project_template(networktype_id, project_id, user_id, project=project_i)

    if can_delete:
        project_template_i = db.DBSession.query(ProjectTemplate).filter(
            ProjectTemplate.project_id == project_id,
            ProjectTemplate.template_id == template_id
        ).first()

        if project_template_i is not None:
            db.DBSession.delete(project_template_i)
            Project.clear_cache(project_i.id)
    else:
        template_i = _get_template(template_id, user_id)
        raise HydraError(f"Cannot remove the template '{template_i.name}' ({template_i.id}) "
                         f"from project '{project_i.name} ({project_i.id})' because there"
                         "are networks in the project which rely on this template.")

    db.DBSession.flush()

    return 'OK'

def _check_can_remove_project_template(networktype_id, project_id, user_id, project=None):
    """
    Check whether a network exists in this project which uses this template. If so, return false.
    """

    if project is None:
        project = _get_project(project_id, user_id)

    networktypes = db.DBSession.query(ResourceType).join(Network,
        ResourceType.network_id==Network.id).filter(
            Network.status != 'X',
            Network.project_id==project_id).all()

    for networktype in networktypes:
        #A network which uses the template in question has been found
        #meaning the template cannot be dis-associated with the template
        if networktype.type_id == networktype_id:
            return False

    children = db.DBSession.query(Project).filter(Project.parent_id==project_id).all()
    can_delete = True
    for child in children:
        can_delete_child = _check_can_remove_project_template(networktype_id, child.id, user_id, project=child)
        can_delete = can_delete and can_delete_child

    return can_delete

def check_can_move_network(network_id, target_project_id):
    """
        This checks whether a network can be moved to another project.
        The criteria for this is that the correct templatetypes and typeattrs must be available in the
        target project. If the source project defines custom types not in the target, then the network
        cannot be moved there because it would mean the network is using types and typeattrs
        defined outide the source project's context.

        Args:
            network_id: The network being moved

            target_project_ide: The project which the network is being moved to
        Returns
            'OK'
        Raises:
            HydraError if:
                1: The target project is not scoped to the template which the network uses
                2: The source project has modifications to the template, making the templates incompatible.
                3: The network has modifications to the template, making the templates incompatible.
    """

    from hydra_base.lib.project import get_project_hierarchy

    network_i = db.DBSession.query(Network).filter(
        Network.id==network_id).one()

    target_project_i = db.DBSession.query(Project).filter(
        Project.id==target_project_id).one()
    #Identify the template associated to the target project
    target_project_hierarchy = get_project_hierarchy(target_project_i.id, 1)
    target_project_hierarchy_ids = [p.id for p in target_project_hierarchy]
    target_project_templates = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id.in_(target_project_hierarchy_ids)
    ).all()

    if len(target_project_templates) > 0:
        for target_project_template in target_project_templates:
            #get the type of the network to check if it's compatible with the
            #target project template
            networktype = db.DBSession.query(TemplateType).join(
                ResourceType, ResourceType.network_id==network_id).filter(
                TemplateType.id==ResourceType.type_id).one()
            target_template_i = db.DBSession.query(Template).join(
                Template.id==target_project_template.template_id
            ).one()
            source_template_i = db.DBSession.query(Template).join(
                Template.id==networktype.template_id
            ).one()
            if networktype.template_id != target_project_template.id:
                raise HydraError(f"Unable to move network '{network_i.name}' ({network_i.id}) "
                                 f"to project '{target_project_i.name} ({target_project_i.id})'"
                                 f"The target project uses template {target_template_i.name}"
                                 f"while the network uses template {source_template_i.name}")

    #Identify the template associated to the source project
    source_project_i = db.DBSession.query(Project).filter(
        Project.id==network_i.project_id).one()
    source_project_hierarchy = get_project_hierarchy(source_project_i.id, 1)
    source_project_hierarchy_ids = [p.id for p in source_project_hierarchy]
    source_project_scoped_types = db.DBSession.query(TemplateType).filter(
        TemplateType.project_id.in_(source_project_hierarchy_ids)
    ).all()

    target_project_scoped_types = db.DBSession.query(TemplateType).filter(
        TemplateType.project_id.in_(target_project_hierarchy_ids)
    ).all()

    if len(source_project_scoped_types) > len(target_project_scoped_types) or\
        len(set([tt.id for tt in source_project_scoped_types]) - set([tt.id for tt in target_project_scoped_types])) > 0:
        raise HydraError(f"Cannot move network '{network_i.name}'"
                         f" to project '{target_project_i.name}'."
                         f"The current project contains template modifications"
                         f"required for the network to operate correctly.")

    source_project_scoped_typeattrs = db.DBSession.query(TypeAttr).filter(
        TypeAttr.project_id.in_(source_project_hierarchy_ids)
    ).all()
    if len(source_project_scoped_typeattrs) > 0:
        target_project_scoped_typeattrs = db.DBSession.query(TypeAttr).filter(
            TypeAttr.project_id.in_(target_project_hierarchy_ids)
        ).all()

        source_typeattrs = set([ta.attr_id for ta in source_project_scoped_typeattrs])
        target_typeattrs = set([ta.attr_id for ta in target_project_scoped_typeattrs])
        #if there are attributes on a type defined in the source project that don't
        #exist on a type defined in the target project, then the projects are incompatible
        if len(source_typeattrs - target_typeattrs) > 0:

            raise HydraError(f"Cannot move network '{network_i.name}'"
                         f" to project '{target_project_i.name}'."
                         f"The current project contains template modifications"
                         f"required for the network to operate correctly.")
    return 'OK'

def check_can_move_project(project_id, target_project_id):
    """
        This checks whether a project can be moved to another project.
        The criteria for this is that the correct templatetypes and typeattrs for all networks in the proejct being moved
        must be available in the target project. If the source parent project defines custom types not in the target, then the network
        cannot be moved there because it would mean the network is using types and typeattrs
        defined outide the source project's context.
        Args:
            project_id : The project being moved

            target_project_id: The project which the project is being moved to
        Returns
            'OK'
        Raises:
            HydraError if:
                1: The target project is not scoped to the template which the moved project uses
                2: The moved project has modifications to the template, making the templates incompatible.
                3: The moved project contains one or more networks with modifications to the template, making the templates incompatible.

    """
    from hydra_base.lib.project import get_project_hierarchy, get_all_networks

    moved_project_i = db.DBSession.query(Project).filter(
        Project.id==project_id).one()

    #identify the template associated to the target project
    target_project_i = db.DBSession.query(Project).filter(
        Project.id==target_project_id).one()
    target_project_hierarchy = get_project_hierarchy(target_project_i.id, 1)
    target_project_hierarchy_ids = [p.id for p in target_project_hierarchy]
    target_project_template = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id.in_(target_project_hierarchy_ids)
    ).first()

    #Identify the template associated to the source project
    source_project_i = db.DBSession.query(Project).filter(
        Project.id==project_id).one()
    source_project_hierarchy = get_project_hierarchy(source_project_i.id, 1)
    source_project_hierarchy_ids = [p.id for p in source_project_hierarchy]
    source_project_template = db.DBSession.query(ProjectTemplate).filter(
        TemplateType.project_id.in_(source_project_hierarchy_ids)
    ).first()

    if target_project_template is not None:
        if source_project_template is None:
            #find all the networks in this project and check if any of them don't match
            #the template on the source
            all_project_networks = get_all_networks(project_id)
            all_source_network_ids = [n.id for n in all_project_networks]
            #get the type of the network to check if it's compatible with the
            #target project template
            networktypes = db.DBSession.query(TemplateType).join(
                ResourceType, ResourceType.network_id.in_(all_source_network_ids)).filter(
                TemplateType.id==ResourceType.type_id).all()

            target_template_i = db.DBSession.query(Template).join(
                Template.id==target_project_template.template_id
            ).one()

            source_template_i = db.DBSession.query(Template).join(
                Template.id.in_([nt.template_id for nt in networktypes])
            ).all()
            if set([t.id for t in source_template_i]) != 1:
                templatenames = [t.name for t in source_template_i]
                raise HydraError(f"Unable to move project '{moved_project_i.name}' ({moved_project_i.id}) "
                    f"to project '{target_project_i.name} ({target_project_i.id})'"
                    f"The target project requires only template {target_template_i.name}"
                    f"while this project uses templates {templatenames.join(',')}")

            elif target_template_i.id != source_template_i[0].id:
                raise HydraError(f"Unable to move project '{moved_project_i.name}' ({moved_project_i.id}) "
                    f"to project '{target_project_i.name} ({target_project_i.id})'"
                    f"The target project requires only template {target_template_i.name}"
                    f"while this project uses template {source_template_i[0]}")


    #Identify the template associated to the source project
    source_project_i = db.DBSession.query(Project).filter(
        Project.id==moved_project_i.id).one()
    source_project_hierarchy = get_project_hierarchy(source_project_i.id, 1)
    source_project_hierarchy_ids = [p.id for p in source_project_hierarchy]
    source_project_scoped_types = db.DBSession.query(TemplateType).filter(
        TemplateType.project_id.in_(source_project_hierarchy_ids)
    ).all()

    project_scoped_types = db.DBSession.query(TemplateType).filter(
        TemplateType.project_id.in_(source_project_hierarchy_ids)
    ).all()

    if len(project_scoped_types) > 0:
        raise HydraError(f"Cannot move project '{moved_project_i.name}'"
                         f" to project '{target_project_i.name}'."
                         f"The project being moved contains template modifications"
                         f"required for the networks in it to operate correctly. "
                         f"These would be lost when moving the project.")

    source_project_scoped_typeattrs = db.DBSession.query(TypeAttr).filter(
        TypeAttr.project_id.in_(source_project_hierarchy_ids)
    ).all()

    if len(source_project_scoped_typeattrs) > 0:
        target_project_hierarchy = get_project_hierarchy(target_project_i.id, 1)
        targert_project_hierarchy_ids = [p.id for p in target_project_hierarchy]
        target_project_scoped_typeattrs = db.DBSession.query(TypeAttr).filter(
            TypeAttr.project_id.in_(targert_project_hierarchy_ids)
        ).all()

        source_typeattrs = set([ta.attr_id for ta in source_project_scoped_typeattrs])
        target_typeattrs = set([ta.attr_id for ta in target_project_scoped_typeattrs])
        #if there are attributes on a type defined in the source project that don't
        #exist on a type defined in the target project, then the projects are incompatible
        if len(source_typeattrs - target_typeattrs) > 0:

            raise HydraError(f"Cannot move project '{moved_project_i.name}'"
                         f" to project '{target_project_i.name}'."
                         f"The project being moved contains template modifications"
                         f"required for the networks in it to operate correctly."
                         f"These would be lost when moving the project.")
    'OK'
