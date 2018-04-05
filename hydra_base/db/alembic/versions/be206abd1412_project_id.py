"""project_id

Revision ID: be206abd1412
Revises: f894faeb055d
Create Date: 2018-03-29 22:42:42.636390

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'be206abd1412'
down_revision = 'f894faeb055d'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tProject', 'project_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tProject', 'project_name', new_column_name='name', existing_type=sa.String(200))
            op.alter_column('tProject', 'project_description', new_column_name='description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tProject
        try:
            op.drop_table('tProject_new')
        except:
            log.info("tProject_new isn't there")

        try:
            # ## tProject
            op.create_table(
                'tProject_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.UniqueConstraint('name', 'created_by', 'status', name="unique project name")
            )

            op.execute("insert into tProject_new (id, name, description, status, cr_date, created_by) select project_id, project_name, project_description, status, cr_date, created_by from tProject")

            op.rename_table('tProject','tProject_old')
            op.rename_table('tProject_new', 'tProject')
            op.drop_table('tProject_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tProject', 'id', new_column_name='project_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tProject', 'name', new_column_name='project_name', existing_type=sa.String(200))
            op.alter_column('tProject', 'description', new_column_name='project_description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)

    else: ## sqlite
        try:
            # ## tProject
            op.create_table(
                'tProject_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.UniqueConstraint('name', 'created_by', 'status', name="unique project name")
            )

            op.execute("insert into tProject_new (project_id, project_name, project_description, status, cr_date, created_by) select id, name, description, status, cr_date, created_by from tProject")

            op.rename_table('tProject','tProject_old')
            op.rename_table('tProject_new', 'tProject')
            op.drop_table('tProject_old')

        except Exception as e:
            log.exception(e)
