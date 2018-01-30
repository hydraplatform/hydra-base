"""simplify network

Revision ID: 1eb51c6159ac
Revises: 7d6099524c02
Create Date: 2017-12-29 11:36:35.323359

"""
from alembic import op
import sqlalchemy as sa
from hydra_base import db

# revision identifiers, used by Alembic.
revision = '1eb51c6159ac'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    
    if op.get_bind().dialect.name == 'mysql':
        op.drop_constraint(u'unique net name', 'tNetwork', type_='unique')
    
        op.drop_constraint(u'tlink_ibfk_1', 'tLink', type_='foreignkey')
        op.drop_constraint(u'tnetworkowner_ibfk_2', 'tNetworkOwner', type_='foreignkey')
        op.drop_constraint(u'tnode_ibfk_1', 'tNode', type_='foreignkey')
        op.drop_constraint(u'tnote_ibfk_4', 'tNote', type_='foreignkey')
        op.drop_constraint(u'tresourceattr_ibfk_2', 'tResourceAttr', type_='foreignkey')
        op.drop_constraint(u'tresourceattrmap_ibfk_2', 'tResourceAttrMap', type_='foreignkey')
        op.drop_constraint(u'tresourceattrmap_ibfk_1', 'tResourceAttrMap', type_='foreignkey')
        op.drop_constraint(u'tresourcegroup_ibfk_1', 'tResourceGroup', type_='foreignkey')
        op.drop_constraint(u'tresourcetype_ibfk_2', 'tResourceType', type_='foreignkey')
        op.drop_constraint(u'trule_ibfk_2', 'tRule', type_='foreignkey')
        op.drop_constraint(u'tscenario_ibfk_1', 'tScenario', type_='foreignkey')

        op.alter_column('tNetwork', 'network_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
        op.alter_column('tNetwork', 'network_name', new_column_name='name', existing_type=sa.String(200))
        op.alter_column('tNetwork', 'network_description', new_column_name='description', existing_type=sa.String(1000))

        op.create_unique_constraint('unique net name', 'tNetwork', ['name', 'project_id'])

        op.create_foreign_key(None, 'tLink', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tNetworkOwner', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tNode', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tNote', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tResourceAttr', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tResourceAttrMap', 'tNetwork', ['network_b_id'], ['id'])
        op.create_foreign_key(None, 'tResourceAttrMap', 'tNetwork', ['network_a_id'], ['id'])
        op.create_foreign_key(None, 'tResourceGroup', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tResourceType', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tRule', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tScenario', 'tNetwork', ['network_id'], ['id'])

    else:

        op.create_table(
            'tNetwork_new',
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text(200), nullable=False),
            sa.Column('description', sa.Text(1000)),
            sa.Column('layout', sa.Text(1000)),
            sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.project_id'),  nullable=False),
            sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
            sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            sa.Column('projection', sa.String(1000)),
            sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
            sa.UniqueConstraint('name', 'project_id', name="unique net name")
        )
        
        op.execute("insert into tNetwork_new (id, name, description, layout, project_id, status, cr_date, projection, created_by) select network_id, network_name, network_description, layout, project_id, status, cr_date, projection, created_by from tNetwork")

        
        op.rename_table('tNetwork','tNetwork_old')
        op.rename_table('tNetwork_new', 'tNetwork')
        
    # ### end Alembic commands ###


def downgrade():
    op.create_table(
        'tNetwork_new',
        sa.Column('network_id', sa.Integer, primary_key=True),
        sa.Column('network_name', sa.Text(200), nullable=False),
        sa.Column('network_description', sa.Text(1000)),
        sa.Column('layout', sa.Text(1000)),
        sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.project_id'),  nullable=False),
        sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
        sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
        sa.Column('projection', sa.String(1000)),
        sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
        sa.UniqueConstraint('name', 'project_id', name="unique net name")
    )
    
    op.execute("insert into tNetwork_new (network_id, network_name, network_description, layout, project_id, status, cr_date, projection, created_by) select id, name, description, layout, project_id, status, cr_date, projection, created_by from tNetwork")

    
    op.rename_table('tNetwork','tNetwork_old')
    op.rename_table('tNetwork_new', 'tNetwork')