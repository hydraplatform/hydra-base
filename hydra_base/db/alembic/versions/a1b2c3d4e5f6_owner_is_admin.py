"""owner_is_admin

Revision ID: a1b2c3d4e5f6
Revises: e7a3c9f04b12
Create Date: 2026-06-23 00:00:00.000000

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'e7a3c9f04b12'
branch_labels = None
depends_on = None


def upgrade():
    for table in ('tProjectOwner', 'tNetworkOwner'):
        try:
            op.add_column(
                table,
                sa.Column('is_admin', sa.String(1), nullable=False, server_default='N')
            )
        except Exception as e:
            log.warning("Could not add is_admin to %s: %s", table, e)


def downgrade():
    for table in ('tProjectOwner', 'tNetworkOwner'):
        try:
            op.drop_column(table, 'is_admin')
        except Exception as e:
            log.warning("Could not drop is_admin from %s: %s", table, e)
