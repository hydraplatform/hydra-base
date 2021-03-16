"""network_appdata

Revision ID: cec2b77ad85e
Revises: 1d3ab26415ec
Create Date: 2021-02-12 10:10:35.646470

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'cec2b77ad85e'
down_revision = '1d3ab26415ec'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tNetwork',
                          sa.Column('appdata',
                                    sa.Text().with_variant(sa.dialects.mysql.LONGTEXT, 'mysql')))
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_column('tNetwork', 'appdata')
        except Exception as e:
            log.critical(e)
