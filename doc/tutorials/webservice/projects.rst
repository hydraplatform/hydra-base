.. _projects:

Working with projects
=====================
A project is the most fundamental structure in Hydra. Without
a project, users cannot create networks. A project acts as a
container for multiple projects. Users can create as many projects
as they wish, and within a project can create as many networks as they wish.

Projects do not perform any special functions other than acting as a container.

A project must be created before a network, so this is commonly the very first
thing one would do in Hydra.

Creating a project
------------------
Creating an empty project is very simple..

.. code-block:: python

    #create a new project object
    proj      = self.client.factory.create('hyd:Project')
    #give your project a name
    proj.name = 'SOAP test %s'%(datetime.datetime.now())
    #add the project
    project   = self.client.service.add_project(proj)

Accessing projects
------------------
Once a project has been created, a user can access it using ``get_project`` and
passing the project ID. The user must be logged in in order to access their
project.

.. code-block:: python

    #...connect..
    project_id = 1232
    proj = client.service.get_project(project_id)

    #All a user's projects can be accessed also
    my_user_id = 1
    proj = client.service.get_projects(my_user_id)

Deleting projects
-----------------
If a user no longer has any need for their project, they can delete it.
``deleting`` does not remove the project from the databaset. To do this, 
the user must ``purge`` the database. **WARNING** As a project is the top of the
hierarchy, purging it will purge ALL its networks!

.. code-block:: python

    #...connect...

    project_id = 1232
    my_projects = client.service.delete_project(my_user_id)

    #..or..
    #WARNING: This will remove a project and ALL the networks inside it!
    my_projects = client.service.purge_project(my_user_id)

Sharing projects
----------------
It is not uncommon for multiple people to be working on the same project. To
facilitate this, the owner of a project can share the project with other users.
The owner can control whether the sharee can only see the project, see and edit
the project or see, edit and share the project. To avoid sharing getting out of control, only the creator (owner) of a project has control over the re-share
permission. In other words, if user A shares with user B, who shares with 
user C, user C cannot share with anyone.

.. code-block:: python

    my_user_id = 1
    my_friends_user_id = 2

    my_project_id = 123
    
    #Allow my friend to access and edit my project, but do not let him share it
    #with others.
    client.service.share_project(my_project_id, my_friends_user_id, 'Y', 'Y', 'N')
