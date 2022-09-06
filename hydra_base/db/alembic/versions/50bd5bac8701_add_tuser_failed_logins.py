"""Add tUser.failed_logins

Revision ID: 50bd5bac8701
Revises: 35088e32c557
Create Date: 2020-08-10 11:56:48.925174

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)


# revision identifiers, used by Alembic.
revision = '50bd5bac8701'
down_revision = '35088e32c557'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tUser', sa.Column('failed_logins', sa.types.SMALLINT, nullable=True))
        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.drop_column('tUser', 'failed_logins')
        except Exception as e:
            log.exception(e)
