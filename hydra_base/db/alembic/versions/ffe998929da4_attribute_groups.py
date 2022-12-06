"""Attribute groups

Revision ID: ffe998929da4
Revises: 1eb51c6159ac
Create Date: 2018-03-16 09:13:30.493034

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
import logging

# revision identifiers, used by Alembic.
revision = 'ffe998929da4'
down_revision = '1eb51c6159ac'
branch_labels = None
depends_on = None


def upgrade():
    try:
        # ### commands auto generated by Alembic - please adjust! ###
        op.create_table('tAttrGroup',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=200), nullable=False),
        sa.Column('description', sa.Text().with_variant(mysql.TEXT(1000), 'mysql'), nullable=True),
        sa.Column('layout', sa.Text().with_variant(mysql.TEXT(5000), 'mysql'), nullable=True),
        sa.Column('exclusive', sa.String(length=1), server_default=sa.text(u"'N'"), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('cr_date', sa.TIMESTAMP(), server_default=sa.text(u'CURRENT_TIMESTAMP'), nullable=False),
        sa.ForeignKeyConstraint(['project_id'], ['tProject.project_id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'project_id', name='unique attr group name')
        )
        op.create_index(op.f('ix_tAttrGroup_id'), 'tAttrGroup', ['id'], unique=False)
        op.create_table('tAttrGroupItem',
            sa.Column('group_id', sa.Integer(), nullable=False),
            sa.Column('attr_id', sa.Integer(), nullable=False),
            sa.Column('network_id', sa.Integer(), nullable=False),
            sa.ForeignKeyConstraint(['attr_id'], ['tAttr.attr_id'], ),
            sa.ForeignKeyConstraint(['group_id'], ['tAttrGroup.id'], ),
            sa.ForeignKeyConstraint(['network_id'], ['tNetwork.id'], ),
            sa.PrimaryKeyConstraint('group_id', 'attr_id', 'network_id')
        )
    except Exception as e:
        print("Unable to add attr group tables: %s"%(str(e)))

    try:
        op.create_foreign_key(None, 'tLink', 'tNetwork', ['network_id'], ['id'])
        op.create_unique_constraint('unique net name', 'tNetwork', ['name', 'project_id'])
        op.create_foreign_key(None, 'tNetworkOwner', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tNode', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tNote', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tResourceAttr', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tResourceAttrMap', 'tNetwork', ['network_a_id'], ['id'])
        op.create_foreign_key(None, 'tResourceAttrMap', 'tNetwork', ['network_b_id'], ['id'])
        op.create_foreign_key(None, 'tResourceGroup', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tResourceType', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tRule', 'tNetwork', ['network_id'], ['id'])
        op.create_foreign_key(None, 'tScenario', 'tNetwork', ['network_id'], ['id'])
    except Exception as e:
        print ("Unable to set foreign keys: %s" % str(e))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'tScenario', type_='foreignkey')
    op.drop_constraint(None, 'tRule', type_='foreignkey')
    op.drop_constraint(None, 'tResourceType', type_='foreignkey')
    op.drop_constraint(None, 'tResourceGroup', type_='foreignkey')
    op.drop_constraint(None, 'tResourceAttrMap', type_='foreignkey')
    op.drop_constraint(None, 'tResourceAttrMap', type_='foreignkey')
    op.drop_constraint(None, 'tResourceAttr', type_='foreignkey')
    op.drop_constraint(None, 'tNote', type_='foreignkey')
    op.drop_constraint(None, 'tNode', type_='foreignkey')
    op.drop_constraint(None, 'tNetworkOwner', type_='foreignkey')
    op.drop_constraint('unique net name', 'tNetwork', type_='unique')
    op.drop_constraint(None, 'tLink', type_='foreignkey')
    op.drop_table('tAttrGroupItem')
    op.drop_index(op.f('ix_tAttrGroup_id'), table_name='tAttrGroup')
    op.drop_table('tAttrGroup')
    # ### end Alembic commands ###