"""user.user_id -> user.id

Revision ID: 74ab69a8dcb6
Revises: ffe998929da4
Create Date: 2018-03-16 12:35:12.742624

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '74ab69a8dcb6'
down_revision = 'ffe998929da4'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.alter_column('tUser', 'user_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)


    else:
        try:
            op.drop_table('tUser_new')
        except:
            log.info('Table tUser_new does not exist')

        try:
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
            op.drop_table('tUser_old')
        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.alter_column('tUser', 'user_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)
    else:
        try:
            op.drop_table('tUser_new')
        except:
            log.info('Table tUser_new does not exist')

        try:
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
            op.drop_table('tUser_old')

        except Exception as e:
            log.exception(e)
