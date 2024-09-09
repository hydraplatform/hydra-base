"""ownership_audit

Revision ID: 5b63ea9df36f
Revises: 877adf863b33
Create Date: 2024-09-06 10:54:11.710155

"""

from alembic import op
import sqlalchemy as sa

import datetime

import logging

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "5b63ea9df36f"
down_revision = "877adf863b33"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == "mysql":

        # ### tDatasetOwner
        try:
            op.add_column(
                "tDatasetOwner",
                sa.Column(
                    "created_by", sa.Integer(), sa.ForeignKey("tUser.id"), nullable=True
                ),
            )
            op.add_column(
                "tDatasetOwner",
                sa.Column(
                    "updated_by", sa.Integer(), sa.ForeignKey("tUser.id"), nullable=True
                ),
            )
            op.add_column(
                "tDatasetOwner",
                sa.Column(
                    "updated_at",
                    sa.DateTime(),
                    default=datetime.datetime.utcnow(),
                    onupdate=datetime.datetime.utcnow(),
                    nullable=True,
                ),
            )
        except Exception as e:
            log.exception(e)
        # Project Owner
        try:
            op.add_column(
                "tProjectOwner",
                sa.Column(
                    "created_by", sa.Integer(), sa.ForeignKey("tUser.id"), nullable=True
                ),
            )
            op.add_column(
                "tProjectOwner",
                sa.Column(
                    "updated_by", sa.Integer(), sa.ForeignKey("tUser.id"), nullable=True
                ),
            )
            op.add_column(
                "tProjectOwner",
                sa.Column(
                    "updated_at",
                    sa.DateTime(),
                    default=datetime.datetime.utcnow(),
                    onupdate=datetime.datetime.utcnow(),
                    nullable=True,
                ),
            )
        except Exception as e:
            log.exception(e)
        # Network Owner
        try:
            op.add_column(
                "tNetworkOwner",
                sa.Column(
                    "created_by", sa.Integer(), sa.ForeignKey("tUser.id"), nullable=True
                ),
            )
            op.add_column(
                "tNetworkOwner",
                sa.Column(
                    "updated_by", sa.Integer(), sa.ForeignKey("tUser.id"), nullable=True
                ),
            )
            op.add_column(
                "tNetworkOwner",
                sa.Column(
                    "updated_at",
                    sa.DateTime(),
                    default=datetime.datetime.utcnow(),
                    onupdate=datetime.datetime.utcnow(),
                    nullable=True,
                ),
            )
        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == "mysql":

        # ### tScenario
        try:
            op.drop_column("tDataset", "updated_at")
            op.drop_column("tDataset", "updated_by")
        except Exception as e:
            log.exception(e)
