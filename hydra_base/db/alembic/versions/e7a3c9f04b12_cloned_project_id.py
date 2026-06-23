"""cloned_project_id

Revision ID: e7a3c9f04b12
Revises: d4e9b1f2c83a
Create Date: 2026-05-20 00:00:00.000000

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'e7a3c9f04b12'
down_revision = 'd4e9b1f2c83a'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.add_column('tProject',
                          sa.Column('cloned_project_id',
                                    sa.Integer(),
                                    sa.ForeignKey('tProject.id'),
                                    nullable=True))
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_column('tProject', 'cloned_project_id')
        except Exception as e:
            log.critical(e)
