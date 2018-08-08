"""change password type

Revision ID: 6883f6e39176
Revises: 21e8d704c020
Create Date: 2018-08-08 12:06:59.579756

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6883f6e39176'
down_revision = '21e8d704c020'
branch_labels = None
depends_on = None

import logging
log = logging.getLogger(__name__)

def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tUser

        try:
            op.alter_column('tUser', 'password', existing_type=sa.LargeBinary(), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tUser_new')
        except:
            log.info('tScenario_new does not exist')

        try:
            # ## tScenario
            op.create_table(
                'tUser_new',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('username', sa.Text(200), nullable=False),
                sa.Column('password', sa.LargeBinary),
                sa.Column('display_name', sa.String(60),  nullable=False, server_default=sa.text(u"''")),
                sa.Column('last_login', sa.TIMESTAMP()),
                sa.Column('last_edit', sa.TIMESTAMP()),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP'))
            )

            op.execute("insert into tUser_new (id, username, password, display_name, last_login, last_edit, cr_date) select id, username, password, display_name, last_login, last_edit, cr_date from tUser")

            op.rename_table('tUser','tUser_old')
            op.rename_table('tUser_new', 'tUser')
            op.drop_table('tUser_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tUser

        try:
            op.alter_column('tUser', 'password', existing_type=sa.Text(), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tUser_new')
        except:
            log.info('tScenario_new does not exist')

        try:
            # ## tScenario
            op.create_table(
                'tUser_new',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('username', sa.Text(200), nullable=False),
                sa.Column('password', sa.Text(1000)),
                sa.Column('display_name', sa.String(60),  nullable=False, server_default=sa.text(u"''")),
                sa.Column('last_login', sa.TIMESTAMP()),
                sa.Column('last_edit', sa.TIMESTAMP()),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP'))
            )

            op.execute("insert into tUser_new (id, username, password, display_name, last_login, last_edit, cr_date) select id, username, password, display_name, last_login, last_edit, cr_date from tUser")

            op.rename_table('tUser','tUser_old')
            op.rename_table('tUser_new', 'tUser')
            op.drop_table('tUser_old')

        except Exception as e:
            log.exception(e)
