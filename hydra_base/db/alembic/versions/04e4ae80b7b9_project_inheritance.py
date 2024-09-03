"""project-inheritance

Revision ID: 04e4ae80b7b9
Revises: cec2b77ad85e
Create Date: 2022-01-10 10:00:02.614468

"""

from alembic import op
import sqlalchemy as sa

import logging

log = logging.getLogger(__name__)


# revision identifiers, used by Alembic.
revision = "04e4ae80b7b9"
down_revision = "a81a860cda39"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == "mysql":

        try:
            op.add_column(
                "tProject",
                sa.Column(
                    "parent_id",
                    sa.Integer(),
                    sa.ForeignKey("tProject.id"),
                    nullable=True,
                ),
            )
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == "mysql":
        try:
            op.drop_column("tProject", "parent_id")
        except Exception as e:
            log.exception(e)
