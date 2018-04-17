"""resourcegroupitem

Revision ID: 6b24d6443e31
Revises: f52ccd2b7ffb
Create Date: 2018-04-17 15:34:25.697772

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '6b24d6443e31'
down_revision = 'f52ccd2b7ffb'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tResourceGroupItemt

        try:
            op.alter_column('tResourceGroupItem', 'item_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tResourceGroupItem
        try:
            op.drop_table('tResourceGroupItem_new')
        except:
            log.info("tResourceGroupItem_new isn't there")

        try:
            # ## tResourceGroupItem
            op.create_table(
                'tResourceGroupItem_new',
                    sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('group_id', sa.Integer(), sa.ForeignKey('tResourceGroup.id')),
                    sa.Column('scenario_id', sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=False, index=True),
                    sa.Column('ref_key', sa.String(60),  nullable=False),
                    sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id')),
                    sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id')),
                    sa.Column('subgroup_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id')),
                    sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.UniqueConstraint('group_id', 'node_id', 'scenario_id', name='node_group_1'),
                    sa.UniqueConstraint('group_id', 'link_id', 'scenario_id',  name = 'link_group_1'),
                    sa.UniqueConstraint('group_id', 'subgroup_id', 'scenario_id', name = 'subgroup_group_1'),
            )

            op.execute("insert into tResourceGroupItem_new (id, group_id, scenario_id, ref_key, node_id, link_id, subgroup_id, cr_date) select item_id, group_id, scenario_id, ref_key, node_id, link_id, subgroup_id, cr_date from tResourceGroupItem")

            op.rename_table('tResourceGroupItem','tResourceGroupItem_old')
            op.rename_table('tResourceGroupItem_new', 'tResourceGroupItem')
            op.drop_table('tResourceGroupItem_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tResourceGroupItemt

        try:
            op.alter_column('tResourceGroupItem', 'id', new_column_name='item_id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tResourceGroupItem
        try:
            op.drop_table('tResourceGroupItem_new')
        except:
            log.info("tResourceGroupItem_new isn't there")

        try:
            # ## tResourceGroupItem
            op.create_table(
                'tResourceGroupItem_new',
                    sa.Column('item_id', sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('group_id', sa.Integer(), sa.ForeignKey('tResourceGroup.id')),
                    sa.Column('scenario_id', sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=False, index=True),
                    sa.Column('ref_key', sa.String(60),  nullable=False),
                    sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id')),
                    sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id')),
                    sa.Column('subgroup_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id')),
                    sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.UniqueConstraint('group_id', 'node_id', 'scenario_id', name='node_group_1'),
                    sa.UniqueConstraint('group_id', 'link_id', 'scenario_id',  name = 'link_group_1'),
                    sa.UniqueConstraint('group_id', 'subgroup_id', 'scenario_id', name = 'subgroup_group_1'),
            )

            op.execute("insert into tResourceGroupItem_new (item_id, group_id, scenario_id, ref_key, node_id, link_id, subgroup_id, cr_date) select id, group_id, scenario_id, ref_key, node_id, link_id, subgroup_id, cr_date from tResourceGroupItem")

            op.rename_table('tResourceGroupItem','tResourceGroupItem_old')
            op.rename_table('tResourceGroupItem_new', 'tResourceGroupItem')
            op.drop_table('tResourceGroupItem_old')

        except Exception as e:
            log.exception(e)
