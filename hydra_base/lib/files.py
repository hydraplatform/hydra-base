import tempfile
import os
import datetime
import json

from .. import config
from werkzeug.utils import secure_filename
from . import project
from ..exceptions import HydraError

"""
+------------------+
| FILES - GET |
+------------------+
"""
global USER_FILE_ROOT_DIR
USER_FILE_ROOT_DIR = config.get("FILES", 'USER_FILE_ROOT_DIR', '/tmp')
global USER_FILE_UPLOAD_DIR
USER_FILE_UPLOAD_DIR = config.get("FILES", 'USER_FILE_UPLOAD_DIR', '/tmp')

def allowed_file(filename, allowed_extensions):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

def add_project_files(project_id:int, files:list, user_id:int) -> list:
    """
        Add the list of supplied files to the relevant project folder.
    """

    if project_id is None:
        raise Exception("No project ID specified.")

    allowed_extensions = config.get("FILES", 'USER_FILE_ALLOWED_EXTENSIONS', ['csv', 'xlsx'])
    
    project_i = project._get_project(project_id, user_id, check_write=True)

    #Files uploaded to the USER_FILE_UPLOAD_DIR are synchronized with the USER_FILE_ROOT_DIR
    #So they are then downloaded from the USER_FILE_ROOT_DIR
    os.makedirs(os.path.join(USER_FILE_UPLOAD_DIR, str(project_id)), exist_ok=True)
    
    #assume files have been checked with 'secure_filename' from werkzeug
    full_paths = []
    for filetag in files:
        f = files[filetag]
        filename = secure_filename(f.filename)
        if allowed_file(filename, allowed_extensions) is False:
            raise HydraError(f"File {filename} not allowed. Only files with the following extensions are allowed: {allowed_extensions}")
        full_path = os.path.join(USER_FILE_UPLOAD_DIR, project_id, filename)
        f.save(full_path)
        full_paths.append(full_path)

    return full_paths

def get_project_files(project_id, user_id):
    """
        Get the files available within the specified project.
        The files available in this project also includes files available in parent projects.
        If there are files with the same name at a lower level, then the lower level one is used.
        Files from projects higher in the project tree are not editable or removeable.
    """

    if project_id is None:
        raise Exception("No project ID specified.")

    project_i = project._get_project(project_id, user_id)

    #Get a list of projects, starting with this one, looking
    # up the hierarchy, and ending with the root project ID
    project_hierarchy = project.get_project_hierarchy(project_id, user_id)

    #start from the top down. Files with the same neame, at a lower level
    #take precedence.
    project_hierarchy.reverse()

    filedict = {}
    fileprojects  = {}
    for proj_in_hierarchy in project_hierarchy:
        #Files uploaded to the USER_FILE_UPLOAD_DIR are synchronized with the USER_FILE_ROOT_DIR
        #So they are then downloaded from the USER_FILE_ROOT_DIR
        project_data_path = os.path.join(USER_FILE_ROOT_DIR, str(proj_in_hierarchy.id))

        try:
            projectfiles = os.listdir(project_data_path)
            for f in projectfiles:
                name = f.split(os.sep)[-1]
                filedict[name] = os.path.join(project_data_path, f)
                fileprojects[name] = proj_in_hierarchy
        except FileNotFoundError:
            pass

    projectfiles = []
    for filename in sorted(filedict):
        filepath = filedict[filename]
        filesize = os.path.getsize(filepath)
        projectfiles.append(
            {
                'name':filename,
                'project_id': fileprojects[filename]['id'],
                'project_name': fileprojects[filename]['name'],
                'size': sizeof_fmt(filesize)
            }
        )

    return projectfiles

def sizeof_fmt(num, suffix="B"):
    """
        Given a numbner in bytes, format it into a human-
        readable size, such as Ki etc'
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def delete_project_file(project_id, filename, user_id):
    """
        Delete a file from a project. 
        Delete a file which is stored within a project's folder, something
        like `rm /PATH_TO_PROJECT_DATA/<project_id>/<filename>`

        Assume the 'filename' has been through the werkzeug ('secure_filename')
    """
    if project_id is None:
        raise Exception("No project ID specified.")
    if filename is None:
        raise Exception("No file specified.")
    
    project_i = project._get_project(project_id, user_id, check_write=True)

    file_path = os.path.join(USER_FILE_ROOT_DIR, str(project_id), filename)

    if not os.path.exists(file_path):
        return 'OK'
    
    try:
        os.remove(os.path.join(USER_FILE_ROOT_DIR, str(project_id), filename))
    except FileNotFoundError:
        return 'OK'

    return 'OK'