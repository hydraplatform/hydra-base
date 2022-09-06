"""rules

Revision ID: 35088e32c557
Revises: d759891ddbf4
Create Date: 2019-11-18 14:20:23.661254

"""
from alembic import op
import sqlalchemy as sa

import datetime

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '35088e32c557'
down_revision = 'd759891ddbf4'
branch_labels = None
depends_on = None

def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tRule', sa.Column('format', sa.String(80), nullable=True, server_default='text'))
            op.add_column('tRule', sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'), nullable=True))
            op.add_column('tRule', sa.Column('updated_by', sa.Integer(), sa.ForeignKey('tUser.id'), nullable=True))
            op.add_column('tRule', sa.Column('updated_at', sa.DateTime(), default=datetime.datetime.utcnow(), onupdate=datetime.datetime.utcnow(), nullable=True))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tRule_new')
        except:
            log.info('tRule_new does not exist')

        try:
            op.create_table(
                'tRule_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),

                sa.Column('name', sa.String(200), nullable=False),
                sa.Column('description', sa.String(1000), nullable=True),

                sa.Column('format', sa.String(80), nullable=False, server_default='text'),

                sa.Column('ref_key', sa.String(60),  nullable=False, index=True),

                sa.Column('value', sa.Text().with_variant(sa.mysql.LONGTEXT, 'mysql'),  nullable=True),

                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('scenario_id', sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=True),

                sa.Column('network_id', sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True),
                sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                sa.Column('group_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),

                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                sa.Column('updated_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                sa.Column('updated_at', sa.DateTime(), sa.ForeignKey('tUser.id')),
            )

            op.execute("insert into tRule_new (id, name, description, ref_key, value, status, scenario_id, network_id, node_id, link_id, group_id, created_by, updated_by, updated_at) select id, name, description, ref_key, value, status, scenario_id, network_id, node_id, link_id, group_id, null, null, null from tRule")

            op.rename_table('tRule','tRule_old')
            op.rename_table('tRule_new', 'tRule')
            op.drop_table('tRule_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.drop_column('tRule', 'format')
            op.drop_column('tRule', 'created_by')
            op.drop_column('tRule', 'updated_by')
            op.drop_column('tRule', 'updated_at')
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tRule_new')
        except:
            log.info('tRule_new does not exist')

        try:
            op.create_table(
                'tRule_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),

                sa.Column('name', sa.String(200), nullable=False),
                sa.Column('description', sa.String(1000), nullable=True),

                sa.Column('ref_key', sa.String(60),  nullable=False, index=True),

                sa.Column('value', sa.Text().with_variant(sa.mysql.LONGTEXT, 'mysql'),  nullable=True),

                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('scenario_id', sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=True),

                sa.Column('network_id', sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True),
                sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                sa.Column('group_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),

            )

            op.execute("insert into tRule_new (id, name, description, ref_key, value, status, scenario_id, network_id, node_id, link_id, group_id) select id, name, description, ref_key, value, status, scenario_id, network_id, node_id, link_id, group_id from tRule")

            op.rename_table('tRule','tRule_old')
            op.rename_table('tRule_new', 'tRule')
            op.drop_table('tRule_old')

        except Exception as e:
            log.exception(e)
