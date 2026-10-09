"""resourcescenario_dataset_id_index

Revision ID: c3a9d7e41b58
Revises: b7f3e1a92c44
Create Date: 2026-10-07 00:00:00.000000

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'c3a9d7e41b58'
down_revision = 'b7f3e1a92c44'
branch_labels = None
depends_on = None

INDEX_NAME = 'ix_tResourceScenario_dataset_id'


def _has_index_on_dataset_id():
    inspector = sa.inspect(op.get_bind())
    return any(ix['column_names'][:1] == ['dataset_id']
               for ix in inspector.get_indexes('tResourceScenario'))


def upgrade():
    # MySQL/InnoDB creates an index for the foreign key automatically, so only
    # add one where nothing already leads with dataset_id.
    try:
        if not _has_index_on_dataset_id():
            op.create_index(INDEX_NAME, 'tResourceScenario', ['dataset_id'])
    except Exception as e:
        log.warning("Could not create %s: %s", INDEX_NAME, e)


def downgrade():
    try:
        op.drop_index(INDEX_NAME, table_name='tResourceScenario')
    except Exception as e:
        log.warning("Could not drop %s: %s", INDEX_NAME, e)
