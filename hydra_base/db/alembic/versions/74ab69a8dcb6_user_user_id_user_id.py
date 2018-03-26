"""user.user_id -> user.id

Revision ID: 74ab69a8dcb6
Revises: ffe998929da4
Create Date: 2018-03-16 12:35:12.742624

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '74ab69a8dcb6'
down_revision = 'ffe998929da4'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':
        op.drop_constraint(u'tNetworkOwner_ibfk_1', 'tNetworkOwner', type_='foreignkey')
        op.drop_constraint(u'tNetwork_ibfk_2', 'tNetwork', type_='foreignkey')
        op.drop_constraint(u'tDatasetOwner_ibfk_1', 'tDatasetOwner', type_='foreignkey')
        op.drop_constraint(u'tDataset_ibfk_1', 'tDataset', type_='foreignkey')
        op.drop_constraint(u'tNote_ibfk_1', 'tNote', type_='foreignkey')
        op.drop_constraint(u'tProject_ibfk_1', 'tProject', type_='foreignkey')
        op.drop_constraint(u'tProjectOwner_ibfk_1', 'tProjectOwner', type_='foreignkey')
        op.drop_constraint(u'tRoleUser_ibfk_1', 'tRoleUser', type_='foreignkey')
        op.drop_constraint(u'tScenario_ibfk_2', 'tScenario', type_='foreignkey')
        
        op.alter_column('tUser', 'user_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)

        op.create_foreign_key(None, 'tNetworkOwner', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tNetwork', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tDatasetOwner', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tDataset', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tNote', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tProject', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tProjectOwner', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tRoleUser', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tScenario', 'tUser', ['created_by'], ['id'])
    else:
        op.create_table(
            'tUser_new',
            sa.Column('id', sa.Integer, primary_key=True, nullable=False),
            sa.Column('username', sa.Text(60),  nullable=False, unique=True),
            sa.Column('password', sa.Text(1000),  nullable=False),
            sa.Column('display_name', sa.Text(60),  nullable=False, server_default=text(u"''")),
            sa.Column('last_login', sa.TIMESTAMP()),
            sa.Column('last_edit', sa.TIMESTAMP()),
            sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP')),
        )

        op.execute("insert into tUser_new (id, username, password, display_name, last_login, last_edit, cr_date) select user_id, username, password, display_name, last_login, last_edit, cr_date from tUser")

        op.rename_table('tUser', 'tuser_old')
        op.rename_table('tUser_new', 'tUser')

def upgrade():
    if op.get_bind().dialect.name == 'mysql':
        op.drop_constraint(u'tNetworkOwner_ibfk_1', 'tNetworkOwner', type_='foreignkey')
        op.drop_constraint(u'tNetwork_ibfk_2', 'tNetwork', type_='foreignkey')
        op.drop_constraint(u'tDatasetOwner_ibfk_1', 'tDatasetOwner', type_='foreignkey')
        op.drop_constraint(u'tDataset_ibfk_1', 'tDataset', type_='foreignkey')
        op.drop_constraint(u'tNote_ibfk_1', 'tNote', type_='foreignkey')
        op.drop_constraint(u'tProject_ibfk_1', 'tProject', type_='foreignkey')
        op.drop_constraint(u'tProjectOwner_ibfk_1', 'tProjectOwner', type_='foreignkey')
        op.drop_constraint(u'tRoleUser_ibfk_1', 'tRoleUser', type_='foreignkey')
        op.drop_constraint(u'tScenario_ibfk_2', 'tScenario', type_='foreignkey')
        
        op.alter_column('tUser', 'user_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)

        op.create_foreign_key(None, 'tNetworkOwner', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tNetwork', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tDatasetOwner', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tDataset', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tNote', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tProject', 'tUser', ['created_by'], ['id'])
        op.create_foreign_key(None, 'tProjectOwner', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tRoleUser', 'tUser', ['user_id'], ['id'])
        op.create_foreign_key(None, 'tScenario', 'tUser', ['created_by'], ['id'])
    else:
        op.create_table(
            'tUser_new',
            sa.Column('user_id', sa.Integer, primary_key=True, nullable=False),
            sa.Column('username', sa.Text(60),  nullable=False, unique=True),
            sa.Column('password', sa.Text(1000),  nullable=False),
            sa.Column('display_name', sa.Text(60),  nullable=False, server_default=text(u"''")),
            sa.Column('last_login', sa.TIMESTAMP()),
            sa.Column('last_edit', sa.TIMESTAMP()),
            sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP')),
        )

        op.execute("insert into tUser_new (user_id, username, password, display_name, last_login, last_edit, cr_date) select id, username, password, display_name, last_login, last_edit, cr_date from tUser")

        op.rename_table('tUser', 'tuser_old')
        op.rename_table('tUser_new', 'tUser')
