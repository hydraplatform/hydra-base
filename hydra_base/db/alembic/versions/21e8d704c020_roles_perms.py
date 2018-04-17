"""roles_perms

Revision ID: 21e8d704c020
Revises: d0c45fdc4fac
Create Date: 2018-04-17 17:22:12.357775

"""
from alembic import op
import sqlalchemy as sa


import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '21e8d704c020'
down_revision = 'd0c45fdc4fac'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tPerm

        try:
            op.alter_column('tPerm', 'perm_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tPerm', 'perm_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tPerm', 'perm_code', new_column_name='code', existing_type=sa.String(60), nullable=False)

            op.alter_column('tRole', 'role_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tRole', 'role_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tRole', 'role_code', new_column_name='code', existing_type=sa.String(60), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tPerm
        try:
            op.drop_table('tPerm_new')
            op.drop_table('tRole_new')
        except:
            log.info("tPerm_new or tRole_new isn't there")

        try:
            # ## tPerm
            op.create_table(
                'tPerm_new',
                    sa.Column('id',sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('name',sa.String(60),  nullable=False),
                    sa.Column('code',sa.String(60),  nullable=False),
                    sa.Column('cr_date',sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            )

            op.execute("insert into tPerm_new (id, name, code, cr_date) select perm_id, perm_name, perm_code, cr_date from tPerm")

            op.rename_table('tPerm','tPerm_old')
            op.rename_table('tPerm_new', 'tPerm')
            op.drop_table('tPerm_old')

            # ## tRole
            op.create_table(
                'tRole_new',
                    sa.Column('id',sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('name',sa.String(60),  nullable=False),
                    sa.Column('code',sa.String(60),  nullable=False),
                    sa.Column('cr_date',sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            )

            op.execute("insert into tRole_new (id, name, code, cr_date) select role_id, role_name, role_code, cr_date from tRole")

            op.rename_table('tRole','tRole_old')
            op.rename_table('tRole_new', 'tRole')
            op.drop_table('tRole_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    pass
