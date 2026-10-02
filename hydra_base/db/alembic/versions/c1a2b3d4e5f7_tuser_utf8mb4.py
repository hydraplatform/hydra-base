"""Convert tUser text columns to utf8mb4 to support non-latin display names

Revision ID: c1a2b3d4e5f7
Revises: b7f3e1a92c44
Create Date: 2026-09-23 00:00:00.000000

"""
from alembic import op

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'c1a2b3d4e5f7'
down_revision = 'b7f3e1a92c44'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.execute(
                "ALTER TABLE tUser CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            op.execute(
                "ALTER TABLE tUser MODIFY username VARCHAR(60) "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL"
            )
            op.execute(
                "ALTER TABLE tUser MODIFY display_name VARCHAR(200) "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT ''"
            )
        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.execute(
                "ALTER TABLE tUser CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci"
            )
        except Exception as e:
            log.exception(e)
