"""origin_network_id

Revision ID: 4c19318743f4
Revises: 5b63ea9df36f
Create Date: 2024-09-09 14:09:57.647689

"""

import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)


# revision identifiers, used by Alembic.
revision = "4c19318743f4"
down_revision = "5b63ea9df36f"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == "mysql":

        try:
            op.add_column(
                "tNetwork",
                sa.Column(
                    "origin_network_id",
                    sa.Integer(),
                    sa.ForeignKey("tNetwork.id"),
                    nullable=True,
                ),
            )

            op.add_column(
                "tNetwork",
                sa.Column(
                    "history_enabled", sa.Boolean(), nullable=False, default=False
                ),
            )
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == "mysql":
        try:
            op.drop_column("tNetwork", "origin_network_id")
        except Exception as e:
            log.critical(e)
