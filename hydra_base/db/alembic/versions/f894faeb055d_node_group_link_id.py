"""node_group_link_id

Revision ID: f894faeb055d
Revises: 74ab69a8dcb6
Create Date: 2018-03-29 11:35:42.103923

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'f894faeb055d'
down_revision = '74ab69a8dcb6'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tNode

        try:
            op.alter_column('tNode', 'node_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tNode', 'node_name', new_column_name='name', existing_type=sa.String(200))
            op.alter_column('tNode', 'node_description', new_column_name='description', existing_type=sa.String(1000))
            op.alter_column('tNode', 'node_x', new_column_name='x', existing_type=sa.Float(precision=10, asdecimal=True))
            op.alter_column('tNode', 'node_y', new_column_name='y', existing_type=sa.Float(precision=10, asdecimal=True))
        except Exception as e:
            log.exception(e)

        # ### tLink
        try:

            op.alter_column('tLink', 'link_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tLink', 'link_name', new_column_name='name', existing_type=sa.String(200))
            op.alter_column('tLink', 'link_description', new_column_name='description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)

        # ### tResourceGroup
        try:

            op.alter_column('tResourceGroup', 'group_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tResourceGroup', 'group_name', new_column_name='name', existing_type=sa.String(200))
            op.alter_column('tResourceGroup', 'group_description', new_column_name='description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tNode
        try:
            op.drop_table('tNode_new')
        except:
            log.info("tNode_new isn't there")

        try:
            op.create_table(
                'tNode_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('layout', sa.Text(1000)),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'),  nullable=False),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('x', sa.Float(precision=10, asdecimal=True)),
                sa.Column('y', sa.Float(precision=10, asdecimal=True)),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('name', 'network_id', name="unique node name")
            )

            op.execute("insert into tNode_new (id, name, description, layout, network_id, status, cr_date, x, y) select node_id, node_name, node_description, layout, network_id, status, cr_date, node_x, node_y from tNode")

            op.rename_table('tNode','tNode_old')
            op.rename_table('tNode_new', 'tNode')
            op.drop_table('tNode_old')

        except Exception as e:
            log.exception(e)

        # ## tLink
        try:
            op.drop_table('tLink_new')
        except:
            log.info("tLink_new isn't there")

        try:

            op.create_table(
                'tLink_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('layout', sa.Text(1000)),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'),  nullable=False),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('node_1_id', sa.Integer(), sa.ForeignKey('tNode.id'), nullable=False),
                sa.Column('node_2_id', sa.Integer(), sa.ForeignKey('tNode.id'), nullable=False),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('name', 'network_id', name="unique link name")
            )

            op.execute("insert into tLink_new (id, name, description, layout, network_id, status, cr_date, node_1_id, node_2_id) select link_id, link_name, link_description, layout, network_id, status, cr_date, node_1_id, node_2_id from tLink")

            op.rename_table('tLink','tLink_old')
            op.rename_table('tLink_new', 'tLink')
            op.drop_table('tLink_old')

        except Exception as e:
            log.exception(e)

        # ## tResourceGroup
        try:
            op.drop_table('tResourceGroup_new')
        except:
            log.info("tResourceGroup_new isn't there")

        try:

            op.create_table(
                'tResourceGroup_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'),  nullable=False),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('name', 'network_id', name="unique resourcegroup name")
            )

            op.execute("insert into tResourceGroup_new (id, name, description, network_id, status, cr_date) select group_id, group_name, group_description, network_id, status, cr_date, from tResourceGroup")

            op.rename_table('tResourceGroup','tResourceGroup_old')
            op.rename_table('tResourceGroup_new', 'tResourceGroup')
            op.drop_table('tResourceGroup_old')

        except Exception as e:
            log.exception(e)


def downgrade():

    if op.get_bind().dialect.name == 'mysql':

        # ### tNode

        op.alter_column('tNode', 'id', new_column_name='node_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
        op.alter_column('tNode', 'name', new_column_name='node_name', existing_type=sa.String(200))

        op.alter_column('tNode', 'description', new_column_name='node_description', existing_type=sa.String(1000))
        op.alter_column('tNode', 'x', new_column_name='node_x', existing_type=sa.String(1000))
        op.alter_column('tNode', 'y', new_column_name='node_y', existing_type=sa.String(1000))

        # ### tLink

        op.alter_column('tLink', 'id', new_column_name='link_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
        op.alter_column('tLink', 'name', new_column_name='link_name', existing_type=sa.String(200))
        op.alter_column('tLink', 'description', new_column_name='link_description', existing_type=sa.String(1000))

        # ### tResourceGroup

        op.alter_column('tResourceGroup', 'id', new_column_name='group_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
        op.alter_column('tResourceGroup', 'name', new_column_name='group_name', existing_type=sa.String(200))
        op.alter_column('tResourceGroup', 'description', new_column_name='group_description', existing_type=sa.String(1000))

    else:
        # ## tNode
        op.create_table(
            'tNode_new',
            sa.Column('node_id', sa.Integer, primary_key=True),
            sa.Column('node_name', sa.Text(200), nullable=False),
            sa.Column('node_description', sa.Text(1000)),
            sa.Column('layout', sa.Text(1000)),
            sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'),  nullable=False),
            sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
            sa.Column('x', sa.Float(precision=10, asdecimal=True)),
            sa.Column('y', sa.Float(precision=10, asdecimal=True)),
            sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            sa.UniqueConstraint('node_name', 'network_id', name="unique node name")
        )

        op.execute("insert into tNode_new (node_id, node_name, node_description, layout, network_id, status, cr_date, node_x, node_y) select id, name, description, layout, network_id, status, cr_date, x, y from tNode")

        op.rename_table('tNode','tNode_old')
        op.rename_table('tNode_new', 'tNode')

        # ## tLink
        op.create_table(
            'tLink_new',
            sa.Column('link_id', sa.Integer, primary_key=True),
            sa.Column('link_name', sa.Text(200), nullable=False),
            sa.Column('link_description', sa.Text(1000)),
            sa.Column('layout', sa.Text(1000)),
            sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'),  nullable=False),
            sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
            sa.Column('node_1_id', sa.Integer(), sa.ForeignKey('tNode.id'), nullable=False),
            sa.Column('node_2_id', sa.Integer(), sa.ForeignKey('tNode.id'), nullable=False),
            sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            sa.UniqueConstraint('link_name', 'network_id', name="unique link name")
        )

        op.execute("insert into tLink_new (link_id, link_name, link_description, layout, network_id, status, cr_date, node_1_id, node_2_id) select id, name, description, layout, network_id, status, cr_date, node_1_id, node_2_id from tLink")

        op.rename_table('tLink','tLink_old')
        op.rename_table('tLink_new', 'tLink')


        # ## tResourceGroup
        op.create_table(
            'tResourceGroup_new',
            sa.Column('group_id', sa.Integer, primary_key=True),
            sa.Column('group_name', sa.Text(200), nullable=False),
            sa.Column('group_description', sa.Text(1000)),
            sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'),  nullable=False),
            sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
            sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            sa.UniqueConstraint('group_name', 'network_id', name="unique resourcegroup name")
        )

        op.execute("insert into tResourceGroup_new (group_id, group_name, group_description, network_id, status, cr_date) select id, name, description, network_id, status, cr_date, from tResourceGroup")

        op.rename_table('tResourceGroup','tResourceGroup_old')
        op.rename_table('tResourceGroup_new', 'tResourceGroup')
