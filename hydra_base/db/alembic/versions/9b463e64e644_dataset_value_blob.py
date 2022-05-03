"""dataset_value_blob

Revision ID: 9b463e64e644
Revises: cec2b77ad85e
Create Date: 2022-05-03 21:17:43.525108

"""
import logging
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '9b463e64e644'
down_revision = 'cec2b77ad85e'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.alter_column('tDataset', 'value', existing_type=sa.Text().with_variant(mysql.LONGTEXT, 'mysql'), type_=sa.LargeBinary(length=(2**32)-1))
        except Exception as e:
            log.critical(e)


def downgrade():
        if op.get_bind().dialect.name == 'mysql':
            try:
                op.alter_column('tDataset', 'value', existing_type=sa.LargeBinary(), type_=sa.Text().with_variant(mysql.LONGTEXT, 'mysql'))
            except Exception as e:
                log.critical(e)
