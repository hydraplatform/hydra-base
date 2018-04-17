"""resource_attr_id

Revision ID: 08f0ebe40290
Revises: a7896842f484
Create Date: 2018-04-16 14:49:24.202974

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '08f0ebe40290'
down_revision = 'a7896842f484'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tResourceAttrt

        try:
            op.alter_column('tResourceAttr', 'resource_attr_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tResourceAttr
        try:
            op.drop_table('tResourceAttr_new')
        except:
            log.info("tResourceAttr_new isn't there")

        try:
            # ## tResourceAttr
            op.create_table(
                'tResourceAttr_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('attr_id', sa.Integer(), sa.ForeignKey('tAttr.id'),  nullable=False),
                sa.Column('ref_key', sa.String(60),  nullable=False, index=True),
                sa.Column('network_id', sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True,),
                sa.Column('project_id', sa.Integer(),  sa.ForeignKey('tProject.id'), index=True, nullable=True,),
                sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                sa.Column('group_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),
                sa.Column('attr_is_var', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('network_id', 'attr_id', name = 'net_attr_1'),
                sa.UniqueConstraint('project_id', 'attr_id', name = 'proj_attr_1'),
                sa.UniqueConstraint('node_id',    'attr_id', name = 'node_attr_1'),
                sa.UniqueConstraint('link_id',    'attr_id', name = 'link_attr_1'),
                sa.UniqueConstraint('group_id',   'attr_id', name = 'group_attr_1'),
            )

            op.execute("insert into tResourceAttr_new (id, attr_id, ref_key, network_id, project_id, node_id, link_id, group_id, attr_is_var, cr_date) select resource_attr_id, attr_id, ref_key, network_id, project_id, node_id, link_id, group_id, attr_is_var, cr_date from tResourceAttr")

            op.rename_table('tResourceAttr','tResourceAttr_old')
            op.rename_table('tResourceAttr_new', 'tResourceAttr')
            op.drop_table('tResourceAttr_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tResourceAttr', 'id', new_column_name='resource_attr_id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tResourceAttr
        try:
            op.drop_table('tResourceAttr_new')
        except:
            log.info("tResourceAttr_new isn't there")

        try:
            # ## tResourceAttr
            op.create_table(
                'tResourceAttr_new',
                sa.Column('resource_attr_id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('attr_id', sa.Integer(), sa.ForeignKey('tAttr.id'),  nullable=False),
                sa.Column('ref_key', sa.String(60),  nullable=False, index=True),
                sa.Column('network_id', sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True,),
                sa.Column('project_id', sa.Integer(),  sa.ForeignKey('tProject.id'), index=True, nullable=True,),
                sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                sa.Column('group_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),
                sa.Column('attr_is_var', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('network_id', 'attr_id', name = 'net_attr_1'),
                sa.UniqueConstraint('project_id', 'attr_id', name = 'proj_attr_1'),
                sa.UniqueConstraint('node_id',    'attr_id', name = 'node_attr_1'),
                sa.UniqueConstraint('link_id',    'attr_id', name = 'link_attr_1'),
                sa.UniqueConstraint('group_id',   'attr_id', name = 'group_attr_1'),
            )

            op.execute("insert into tResourceAttr_new (resource_attr_id, attr_id, ref_key, network_id, project_id, node_id, link_id, group_id, attr_is_var, cr_date) select id, attr_id, ref_key, network_id, project_id, node_id, link_id, group_id, attr_is_var, cr_date from tResourceAttr")

            op.rename_table('tResourceAttr','tResourceAttr_old')
            op.rename_table('tResourceAttr_new', 'tResourceAttr')
            op.drop_table('tResourceAttr_old')

        except Exception as e:
            log.exception(e)
