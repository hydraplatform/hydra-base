"""project_layout

Revision ID: 580425ade2e4
Revises: a81a860cda39
Create Date: 2022-09-05 13:58:54.066004

"""

from alembic import op
import sqlalchemy as sa

import logging

log = logging.getLogger(__name__)


# revision identifiers, used by Alembic.

revision = '580425ade2e4'
down_revision = 'a81a860cda39'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tProject', sa.Column('layout', sa.JSON()))
        except Exception as e:
            log.critical(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_column('tProject', 'layout')
        except Exception as e:
           log.exception(e)
