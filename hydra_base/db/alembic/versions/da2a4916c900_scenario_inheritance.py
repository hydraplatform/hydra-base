"""scenario_inheritance

Revision ID: da2a4916c900
Revises: 6883f6e39176
Create Date: 2018-10-25 09:24:28.693901

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'da2a4916c900'
down_revision = '6402bb82a531'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tScenario
        try:
            op.add_column('tScenario', sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tScenario.id'), nullable=True))
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
                sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tScenario.id')),
                sa.UniqueConstraint('network_id', 'name', name="unique scenario name"),
            )

            op.execute("insert into tScenario_new (id, name, description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked, parent_id) select id, name, description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked, null from tScenario")

            op.rename_table('tScenario','tScenario_old')
            op.rename_table('tScenario_new', 'tScenario')
            op.drop_table('tScenario_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tScenario

        try:
            op.drop_column('tScenario', 'parent_id')
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

            op.execute("insert into tScenario_new (id, name, description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked) select id, name, description, layout, status, cr_date, created_by, network_id, start_time, end_time, time_step, locked from tScenario")

            op.rename_table('tScenario','tScenario_old')
            op.rename_table('tScenario_new', 'tScenario')
            op.drop_table('tScenario_old')

        except Exception as e:
            log.exception(e)

