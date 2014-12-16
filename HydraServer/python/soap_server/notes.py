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
from spyne.model.primitive import Integer, Unicode
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Note
from lib import notes

from hydra_base import HydraService

def _get_resource_notes(ref_key, ref_id, **kwargs):
    """
        Get all the notes for a given resource 
    """
    resource_notes = notes.get_notes(ref_key, ref_id, **kwargs)
    return [Note(r) for r in resource_notes]

def _add_note(ref_key, ref_id, note, **kwargs):
    """
        Add a note to a given resource
    """
    note.ref_key = ref_key
    note.ref_id = ref_id
    resource_note = notes.add_note(note, **kwargs)
    return Note(resource_note)

class NoteService(HydraService):

    """
        The data SOAP service
    """

    @rpc(Integer, _returns=SpyneArray(Note))
    def get_scenario_notes(ctx, scenario_id):
        """
            Get all the notes for a scenario
        """
        return _get_resource_notes('SCENARIO', scenario_id)

    @rpc(Integer, _returns=SpyneArray(Note))
    def get_network_notes(ctx, network_id):
        """
            Get all the notes for a network
        """
        return _get_resource_notes('NETWORK', network_id)

    @rpc(Integer, _returns=SpyneArray(Note))
    def get_node_notes(ctx, node_id):
        """
            Get all the notes for a node
        """
        return _get_resource_notes('NODE', node_id)

    @rpc(Integer, _returns=SpyneArray(Note))
    def get_link_notes(ctx, link_id):
        """
            Get all the notes for a link 
        """
        return _get_resource_notes('LINK', link_id)

    @rpc(Integer, _returns=SpyneArray(Note))
    def get_resourcegroup_notes(ctx, group_id):
        """
            Get all the notes for a resource_group
        """
        return _get_resource_notes('GROUP', group_id)

    @rpc(Integer, _returns=SpyneArray(Note))
    def get_project_notes(ctx, project_id):
        """
            Get all the notes for a project
        """
        return _get_resource_notes('PROJECT', project_id)

    @rpc(Integer, _returns=Note)
    def get_note(ctx, note_id):
        """
            Get an individual role by its ID.
        """
        note_i = notes.get_note(note_id, **ctx.in_header.__dict__)
        return Note(note_i)

    @rpc(Integer, Note, _returns=Note)
    def add_scenario_note(ctx, scenario_id, note):
        """
            Add a note to a given scenario
        """
        return _add_note('SCENARIO', scenario_id, note, **ctx.in_header.__dict__)

    @rpc(Integer, Note, _returns=Note)
    def add_network_note(ctx, network_id, note):
        """
            Add a note to a given network
        """
        return _add_note('NETWORK', network_id, note, **ctx.in_header.__dict__)

    @rpc(Integer, Note, _returns=Note)
    def add_project_note(ctx, project_id, note):
        """
            Add a note to a given project
        """
        return _add_note('PROJECT', project_id, note, **ctx.in_header.__dict__)

    @rpc(Integer, Note, _returns=Note)
    def add_node_note(ctx, node_id, note):
        """
            Add a note to a given node
        """
        return _add_note('NODE', node_id, note, **ctx.in_header.__dict__)

    @rpc(Integer, Note, _returns=Note)
    def add_link_note(ctx, link_id, note):
        """
            Add a note to a given link
        """
        return _add_note('LINK', link_id, note, **ctx.in_header.__dict__)

    @rpc(Integer, Note, _returns=Note)
    def add_resourcegroup_note(ctx, group_id, note):
        """
            Add a note to a given resourcegroup
        """
        return _add_note('GROUP', group_id, note, **ctx.in_header.__dict__)

    @rpc(Note, _returns=Note)
    def update_note(ctx, note):
        """
            Update a note. Ensure that scenario_id is specified in the note.
        """
        note_i = notes.update_note(note, **ctx.in_header.__dict__)
        return Note(note_i)

    @rpc(Integer, _returns=Unicode)
    def purge_note(ctx, note_id):
        """
            Remove a note permanently
        """
        notes.purge_note(note_id, **ctx.in_header.__dict__)
        return 'OK'
