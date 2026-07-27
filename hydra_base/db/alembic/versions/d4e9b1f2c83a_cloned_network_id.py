"""cloned_network_id

Revision ID: d4e9b1f2c83a
Revises: a81a860cda39
Create Date: 2026-05-20 00:00:00.000000

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'd4e9b1f2c83a'
down_revision = 'a81a860cda39'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.add_column('tNetwork',
                          sa.Column('cloned_network_id',
                                    sa.Integer(),
                                    sa.ForeignKey('tNetwork.id'),
                                    nullable=True))
        except Exception as e:
            log.critical(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_column('tNetwork', 'cloned_network_id')
        except Exception as e:
            log.critical(e)
