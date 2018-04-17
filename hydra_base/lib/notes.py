#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
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

from ..db.model import Note
from .. import db
from ..exceptions import HydraError, ResourceNotFoundError
from sqlalchemy.orm.exc import NoResultFound

def _get_note(note_id):
    try:
        note_i = db.DBSession.query(Note).filter(Note.id == note_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Note %s not found"%note_id)
    return note_i

def get_notes(ref_key, ref_id, **kwargs):
    """
        Get all the notes for a resource, identifed by ref_key and ref_ido.
        Returns [] if no notes are found or if the resource doesn't exist.
    """
    notes = db.DBSession.query(Note).filter(Note.ref_key==ref_key)
    if ref_key == 'NETWORK':
        notes = notes.filter(Note.network_id == ref_id)
    elif ref_key == 'NODE':
        notes = notes.filter(Note.node_id == ref_id)
    elif ref_key == 'LINK':
        notes = notes.filter(Note.link_id == ref_id)
    elif ref_key == 'GROUP':
        notes = notes.filter(Note.group_id == ref_id)
    elif ref_key == 'PROJECT':
        notes = notes.filter(Note.project_id == ref_id)
    elif ref_key == 'SCENARIO':
        notes = notes.filter(Note.scenario_id == ref_id)
    else:
        raise HydraError("Ref Key %s not recognised.")

    note_rs = notes.all()

    return note_rs

def get_note(note_id, **kwargs):
    """
    Get a note by ID
    """
    note = _get_note(note_id)
    return note

def add_note(note, **kwargs):
    """
    Add a new note
    """
    note_i = Note()
    note_i.ref_key = note.ref_key

    note_i.set_ref(note.ref_key, note.ref_id)

    note_i.value = note.value

    note_i.created_by = kwargs.get('user_id')

    db.DBSession.add(note_i)
    db.DBSession.flush()

    return note_i

def update_note(note, **kwargs):
    """
    Update a note
    """
    note_i = _get_note(note.id)

    if note.ref_key != note_i.ref_key:
        raise HydraError("Cannot convert a %s note to a %s note. Please create a new note instead."%(note_i.ref_key, note.ref_key))

    note_i.set_ref(note.ref_key, note.ref_id)

    note_i.value = note.value

    db.DBSession.flush()

    return note_i

def purge_note(note_id, **kwargs):
    """
    Remove a note from the DB permenantly
    """
    note_i = _get_note(note_id)

    db.DBSession.delete(note_i)

    db.DBSession.flush()
