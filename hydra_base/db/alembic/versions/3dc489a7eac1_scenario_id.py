"""scenario_id

Revision ID: 3dc489a7eac1
Revises: be206abd1412
Create Date: 2018-04-04 16:16:07.105230

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '3dc489a7eac1'
down_revision = 'be206abd1412'
branch_labels = None
depends_on = None

import logging
log = logging.getLogger(__name__)

def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tScenario

        try:
            op.alter_column('tScenario', 'scenario_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tScenario', 'scenario_name', new_column_name='name', existing_type=sa.String(200))
            op.alter_column('tScenario', 'scenario_description', new_column_name='description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tScenario_new')
        except:
            log.info('tScenario_new does not exist')

        try:
            # ## tScenario
            op.create_table(
                'tScenario_new',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('layout', sa.Text(1000)),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), index=True),
                sa.Column('start_time', sa.String(60)),
                sa.Column('end_time', sa.String(60)),
                sa.Column('locked', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('time_step', sa.String(60)),
                sa.UniqueConstraint('network_id', 'name', name="unique scenario name"),
            )

            op.execute("insert into tScenario_new (id, name, description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked) select scenario_id, scenario_name, scenario_description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked from tScenario")

            op.rename_table('tScenario','tScenario_old')
            op.rename_table('tScenario_new', 'tScenario')
            op.drop_table('tScenario_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tScenario

        try:
            op.alter_column('tScenario', 'id', new_column_name='scenario_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tScenario', 'name', new_column_name='scenario_name', existing_type=sa.String(200))
            op.alter_column('tScenario', 'description', new_column_name='scenario_description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tScenario_new')
        except:
            log.info('tScenario_new does not exist')

        try:
            # ## tScenario
            op.create_table(
                'tScenario_new',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('layout', sa.Text(1000)),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), index=True),
                sa.Column('start_time', sa.String(60)),
                sa.Column('end_time', sa.String(60)),
                sa.Column('locked', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('time_step', sa.String(60)),
                sa.UniqueConstraint('network_id', 'scenario_name', name="unique scenario name"),
            )

            op.execute("insert into tScenario_new (scenario_id, scenario_name, scenario_description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked) select id, name, description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked from tScenario")

            op.rename_table('tScenario','tScenario_old')
            op.rename_table('tScenario_new', 'tScenario')
            op.drop_table('tScenario_old')

        except Exception as e:
            log.exception(e)
