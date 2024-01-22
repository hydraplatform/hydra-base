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
      A project can be scoped to one template only.
    """

    user_id = kwargs.get('user_id')
    project_i  = _get_project(project_id, user_id, check_write=True)

    template_i = _get_template(template_id, user_id)

    project_template_i = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id==project_id,
        ProjectTemplate.template_id==template_id).first()

    if project_template_i is not None:
        return HydraError("Template %s already exists on project %s", template_i.name, project_i.name)

    project_template_i = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id==project_id).first()

    if project_template_i is not None:
        raise HydraError(f"Project {project_i.name} is already scoped to a template "
                         f"{template_i.name}. A project cannot be scoped to more than one template.")


    project_template_i = ProjectTemplate(project_id=project_id, template_id=template_id)

    db.DBSession.add(project_template_i)

    db.DBSession.flush()

    return JSONObject(project_template_i)

@required_perms('get_project')
def get_project_template(template_id, project_id, **kwargs):
    """
      Get a template, with any additions added by scoping to a project
    """

    user_id = kwargs.get('user_id')

    project_template_i = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id==project_id,
        ProjectTemplate.template_id==template_id).first()


    if project_template_i is None:
        template_i = _get_template(template_id, user_id)
        raise HydraError(f"Unable to find template '{template_i.name}' ({template_id})"
                         f" on project {project_id}")

    return JSONObject(project_template_i)

@required_perms('edit_project')
def delete_project_template(template_id, project_id, **kwargs):
    """
      Delete a link between a project and a template.
      This can only be done if there are no networks in the project which use the template.
      If there are, then it will not be allowed, as this will create an inconsistency.
    """

    user_id = kwargs.get('user_id')
    project_i  = _get_project(project_id, user_id, check_write=True)
    networktype_id = db.DBSession.query(TemplateType.id).filter(
        TemplateType.template_id==template_id, TemplateType.resource_type=='NETWORK').one().id

    networks_using_template = _check_can_remove_project_template(networktype_id, project_id, user_id, project=project_i)

    if len(networks_using_template) == 0:
        project_template_i = db.DBSession.query(ProjectTemplate).filter(
            ProjectTemplate.project_id == project_id,
            ProjectTemplate.template_id == template_id
        ).first()

        if project_template_i is not None:
            db.DBSession.delete(project_template_i)
            Project.clear_cache(project_i.id)
    else:
        template_i = _get_template(template_id, user_id)
        networks = db.DBSession.query(Network.name).filter(Network.id.in_(networks_using_template)).all()
        raise HydraError(f"Cannot remove the template '{template_i.name}' ({template_i.id}) "
                         f"from project '{project_i.name} ({project_i.id})' because there"
                         "are networks in the project which rely on this template."
                         f"They are: {[n.name for n in networks]}")

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

    networks_using_template = [n.network_id for n in networktypes]

    children = db.DBSession.query(Project).filter(Project.parent_id==project_id).all()
    for child in children:
        networks_using_template.extend(_check_can_remove_project_template(networktype_id, child.id, user_id, project=child))

    return networks_using_template

def _check_project_is_compatible_with_network(network_i, target_project_i):
    """
    When moving a network, check that the target project is compatible/
    """
    #Identify the template associated to the target project
    target_project_hierarchy_ids = [p.id for p in target_project_i.get_hierarchy()]
    target_project_template = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id.in_(target_project_hierarchy_ids)
    ).first()

    if target_project_template is not None:
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

def _check_two_projects_are_compatible(network_i,
                                       target_project_i):
    """
    Check that a network can be moved from one project to another, ensuring
    that the projects are compatible in terms of the types that they use.
    """

    #Identify the template associated to the source project
    source_project_i = db.DBSession.query(Project).filter(
        Project.id==network_i.project_id).one()

    source_project_hierarchy = source_project_i.get_hierarchy()
    source_project_hierarchy_ids = [p.id for p in source_project_hierarchy]

    source_project_scoped_types = db.DBSession.query(TemplateType).filter(
        TemplateType.project_id.in_(source_project_hierarchy_ids)
    ).all()

    target_project_hierarchy_ids = [p.id for p in target_project_i.get_hierarchy()]
    target_project_scoped_types = db.DBSession.query(TemplateType).filter(
        TemplateType.project_id.in_(target_project_hierarchy_ids)
    ).all()

    if len(source_project_scoped_types) > len(target_project_scoped_types) or\
        len(set([tt.id for tt in source_project_scoped_types]) - set([tt.id for tt in target_project_scoped_types])) > 0:
        diff = set([tt.id for tt in source_project_scoped_types]) - set([tt.id for tt in target_project_scoped_types])
        difftypes = db.DBSession.query(TemplateType.name).filter(TemplateType.id.in_(diff)).all()
        raise HydraError(f"Cannot move network '{network_i.name}'"
                         f" to project '{target_project_i.name}'."
                         f"The current project contains template modifications"
                         f"required for the network to operate correctly."
                         f"The differences are the following types: {[t.name for t in difftypes]}")


    #Check if there are diffent type attribtues defined on the different projects
    source_project_scoped_typeattrs = db.DBSession.query(TypeAttr).filter(
        TypeAttr.project_id.in_(source_project_hierarchy_ids)
    ).all()

    if len(source_project_scoped_typeattrs) > 0:
        target_project_scoped_typeattrs = db.DBSession.query(TypeAttr).filter(
            TypeAttr.project_id.in_(target_project_hierarchy_ids)).all()

        source_typeattrs = set([ta.attr_id for ta in source_project_scoped_typeattrs])
        target_typeattrs = set([ta.attr_id for ta in target_project_scoped_typeattrs])
        #if there are attributes on a type defined in the source project that don't
        #exist on a type defined in the target project, then the projects are incompatible
        if len(source_typeattrs - target_typeattrs) > 0:

            raise HydraError(f"Cannot move network '{network_i.name}'"
                         f" to project '{target_project_i.name}'."
                         f"The current project contains template modifications"
                         f"required for the network to operate correctly.")


def check_can_move_network(network_id, target_project_id):
    """
        This checks whether a network can be moved to another project.
        The criteria for this is that the correct templatetypes and typeattrs must be available in the
        target project. If the source project defines custom types not in the target, then the network
        cannot be moved there because it would mean the network is using types and typeattrs
        defined outide the source project's context.
    """


    network_i = db.DBSession.query(Network).filter(
        Network.id==network_id).one()

    #identify the template associated to the target project
    target_project_i = db.DBSession.query(Project).filter(
        Project.id==target_project_id).one()

    _check_project_is_compatible_with_network(network_i, target_project_i)

    _check_two_projects_are_compatible(network_i, target_project_i)


def check_can_move_project(project_id, target_project_i):
    """
        This checks whether a project can be moved to another project.
        The criteria for this is that the correct templatetypes and typeattrs for all networks in the proejct being moved
        must be available in the target project. If the source parent project defines custom types not in the target, then the network
        cannot be moved there because it would mean the network is using types and typeattrs
        defined outide the source project's context.
    """

    moved_project_i = db.DBSession.query(Project).filter(
        Project.id==project_id).one()



    target_project_hierarchy = target_project_i.get_hierarchy()
    moved_project_hierarchy = moved_project_i.get_hierarchy()

    target_project_hierarchy_ids = [p.id for p in target_project_hierarchy]

    target_project_template = db.DBSession.query(ProjectTemplate).filter(
        ProjectTemplate.project_id.in_(target_project_hierarchy_ids)
    ).first()

    moved_project_hierarchy_ids = [p.id for p in moved_project_i.get_hierarchy()]

    moved_project_template = db.DBSession.query(ProjectTemplate).filter(
        TemplateType.project_id.in_(moved_project_hierarchy_ids)
    ).first()

    #check the moved project is compatible with the target project
    _check_project_types_compatible_with_target(moved_project_i,
                                          target_project_i,
                                          moved_project_template,
                                          target_project_template)

    _check_project_typeattrs_compatible_with_target(moved_project_i,
                                          target_project_i,
                                          moved_project_template,
                                          target_project_template)



def _check_project_types_compatible_with_target(moved_project_i,
                                                target_project_i,
                                                moved_project_template,
                                                target_project_template):
    """
    Check that project can be moved to another project by checking that they both
    use the same set of types.
    """

    if target_project_template is not None:
        if moved_project_template is None:
            #find all the networks in this project and check if any of them don't match
            #the template on the moved
            all_project_networks = moved_project_i.get_all_networks()
            all_moved_network_ids = [n.id for n in all_project_networks]
            #get the type of the network to check if it's compatible with the
            #target project template
            networktypes = db.DBSession.query(TemplateType).join(
                RemovedType, RemovedType.network_id.in_(all_moved_network_ids)).filter(
                TemplateType.id==RemovedType.type_id).all()

            target_template_i = db.DBSession.query(Template).join(
                Template.id==target_project_template.template_id
            ).one()

            moved_template_i = db.DBSession.query(Template).join(
                Template.id.in_([nt.template_id for nt in networktypes])
            ).all()
            if set([t.id for t in moved_template_i]) != 1:
                templatenames = [t.name for t in moved_template_i]
                raise HydraError(f"Unable to move project '{moved_project_i.name}' ({moved_project_i.id}) "
                    f"to project '{target_project_i.name} ({target_project_i.id})'"
                    f"The target project requires only template {target_template_i.name}"
                    f"while this project uses templates {templatenames.join(',')}")

            elif target_template_i.id != moved_template_i[0].id:
                raise HydraError(f"Unable to move project '{moved_project_i.name}' ({moved_project_i.id}) "
                    f"to project '{target_project_i.name} ({target_project_i.id})'"
                    f"The target project requires only template {target_template_i.name}"
                    f"while this project uses template {moved_template_i[0]}")


def _check_project_typeattrs_compatible_with_target(moved_project_i,
                                                    target_project_i,
                                                    moved_project_template,
                                                    target_project_template):

    """
    Check that project can be moved to another project by checking that they both
    use the same set of type attributes.
    """
    if target_project_template is not None:
        if moved_project_template is None:
            #find all the networks in this project and check if any of them don't match
            #the template on the moved
            all_project_networks = moved_project_i.get_all_networks()
            all_moved_network_ids = [n.id for n in all_project_networks]
            #get the type of the network to check if it's compatible with the
            #target project template
            networktypes = db.DBSession.query(TemplateType).join(
                RemovedType, RemovedType.network_id.in_(all_moved_network_ids)).filter(
                TemplateType.id==RemovedType.type_id).all()

            target_template_i = db.DBSession.query(Template).join(
                Template.id==target_project_template.template_id
            ).one()

            moved_template_i = db.DBSession.query(Template).join(
                Template.id.in_([nt.template_id for nt in networktypes])
            ).all()
            if set([t.id for t in moved_template_i]) != 1:
                templatenames = [t.name for t in moved_template_i]
                raise HydraError(f"Unable to move project '{moved_project_i.name}' ({moved_project_i.id}) "
                    f"to project '{target_project_i.name} ({target_project_i.id})'"
                    f"The target project requires only template {target_template_i.name}"
                    f"while this project uses templates {templatenames.join(',')}")

            elif target_template_i.id != moved_template_i[0].id:
                raise HydraError(f"Unable to move project '{moved_project_i.name}' ({moved_project_i.id}) "
                    f"to project '{target_project_i.name} ({target_project_i.id})'"
                    f"The target project requires only template {target_template_i.name}"
                    f"while this project uses template {moved_template_i[0]}")


