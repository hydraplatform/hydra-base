"""node_alt_coords

Revision ID: b7f3e1a92c44
Revises: a1b2c3d4e5f6
Create Date: 2026-07-09 00:00:00.000000

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'b7f3e1a92c44'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade():
    for column in ('alt_x', 'alt_y'):
        try:
            op.add_column(
                'tNode',
                sa.Column(column, sa.Float(precision=10, asdecimal=True))
            )
        except Exception as e:
            log.warning("Could not add %s to tNode: %s", column, e)


def downgrade():
    for column in ('alt_x', 'alt_y'):
        try:
            op.drop_column('tNode', column)
        except Exception as e:
            log.warning("Could not drop %s from tNode: %s", column, e)
