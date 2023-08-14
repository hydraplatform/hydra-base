"""project_appdata

Revision ID: 877adf863b33
Revises: 04e4ae80b7b9
Create Date: 2023-08-14 16:03:11.415679

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '877adf863b33'
down_revision = '04e4ae80b7b9'
branch_labels = None
depends_on = None

def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tProject',
                          sa.Column('appdata', sa.JSON()))
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_column('tProject', 'appdata')
        except Exception as e:
            log.critical(e)


def upgrade():
    pass


def downgrade():
    pass
