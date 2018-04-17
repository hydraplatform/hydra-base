"""rule

Revision ID: 56ea2e814437
Revises: 6b24d6443e31
Create Date: 2018-04-17 16:01:19.297928

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '56ea2e814437'
down_revision = '6b24d6443e31'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tRule

        try:
            op.alter_column('tRule', 'rule_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tRule', 'rule_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tRule', 'rule_description', new_column_name='description', existing_type=sa.String(1000))
            op.alter_column('tRule', 'value', new_column_name='value', existing_type=sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'), nullable=True)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tRule
        try:
            op.drop_table('tRule_new')
        except:
            log.info("tRule_new isn't there")

        try:
            # ## tRule
            op.create_table(
                'tRule_new',
                    sa.Column('id',sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('name',sa.String(60), nullable=False),
                    sa.Column('description',sa.String(1000), nullable=False),
                    sa.Column('cr_date',sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.Column('ref_key',sa.String(60),  nullable=False, index=True),
                    sa.Column('value',sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True),
                    sa.Column('status',sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                    sa.Column('scenario_id',sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=False),
                    sa.Column('network_id',sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True),
                    sa.Column('node_id',sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                    sa.Column('link_id',sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                    sa.Column('group_id',sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),
                    sa.UniqueConstraint('scenario_id', 'name', name="unique rule name")
            )

            op.execute("insert into tRule_new (id, name, description, ref_key, status, value, scenario_id, network_id, node_id, link_id, group_id, cr_date) select rule_id, rule_name, rule_description, ref_key, status, value, scenario_id, network_id, node_id, link_id, group_id, cr_date from tRule")

            op.rename_table('tRule','tRule_old')
            op.rename_table('tRule_new', 'tRule')
            op.drop_table('tRule_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tRule

        try:
            op.alter_column('tRule', 'id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tRule', 'name', new_column_name='name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tRule', 'description', new_column_name='description', existing_type=sa.String(1000))
            op.alter_column('tRule', 'value', new_column_name='value', existing_type=sa.LargeBinary(), nullable=True)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tRule
        try:
            op.drop_table('tRule_new')
        except:
            log.info("tRule_new isn't there")

        try:
            # ## tRule
            op.create_table(
                'tRule_new',
                    sa.Column('rule_id',sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('rule_name',sa.String(60), nullable=False),
                    sa.Column('rule_description',sa.String(1000), nullable=False),
                    sa.Column('cr_date',sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.Column('ref_key',sa.String(60),  nullable=False, index=True),
                    sa.Column('value',sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True),
                    sa.Column('status',sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                    sa.Column('scenario_id',sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=False),
                    sa.Column('network_id',sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True),
                    sa.Column('node_id',sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                    sa.Column('link_id',sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                    sa.Column('group_id',sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),
                    sa.UniqueConstraint('scenario_id', 'name', name="unique rule name")
            )

            op.execute("insert into tRule_new (rule_id, rule_name, rule_description, ref_key, status, value, scenario_id, network_id, node_id, link_id, group_id, cr_date) select id, name, description, ref_key, status, value, scenario_id, network_id, node_id, link_id, group_id, cr_date from tRule")

            op.rename_table('tRule','tRule_old')
            op.rename_table('tRule_new', 'tRule')
            op.drop_table('tRule_old')

        except Exception as e:
            log.exception(e)
