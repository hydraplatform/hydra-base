"""template_status

Revision ID: 1d3ab26415ec
Revises: b04cb2e57cb0
Create Date: 2020-12-11 10:24:28.203040

"""

import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)


# revision identifiers, used by Alembic.
revision = '1d3ab26415ec'
down_revision = 'b04cb2e57cb0'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tTemplate', sa.Column('status', sa.String(1), nullable=False, server_default=sa.text(u"'A'")))
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_column('tTemplate', 'status')
        except Exception as e:
            log.critical(e)
