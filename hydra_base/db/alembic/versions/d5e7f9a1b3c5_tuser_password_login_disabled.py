"""Add tUser.password_login_disabled

Set by whatever manages a user's second factor (HWI today) so that hydra-server's
username/password login refuses the user and they must authenticate with an OAuth bearer
token instead, which is only issued after the second factor.

Revision ID: d5e7f9a1b3c5
Revises: c3a9d7e41b58
Create Date: 2026-11-18 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd5e7f9a1b3c5'
down_revision = 'c3a9d7e41b58'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('tUser', sa.Column('password_login_disabled', sa.Boolean(), nullable=False,
                                     server_default=sa.false()))


def downgrade():
    op.drop_column('tUser', 'password_login_disabled')
