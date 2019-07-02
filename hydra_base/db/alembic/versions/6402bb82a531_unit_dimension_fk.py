"""unit_dimension_fk

Revision ID: 6402bb82a531
Revises: 5e381835e382
Create Date: 2019-03-18 15:07:16.318347

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '6402bb82a531'
down_revision = '5e381835e382'
branch_labels = None
depends_on = None


def upgrade():

    try:
        op.add_column('tAttr', sa.Column('dimension_id', sa.Integer(), sa.ForeignKey('tDimension.id'), nullable=True))
        op.drop_column('tAttr', 'dimension')


        op.add_column('tTypeAttr', sa.Column('unit_id', sa.Integer(), sa.ForeignKey('tUnit.id'), nullable=True))
        op.drop_column('tTypeAttr', 'unit')

        op.add_column('tDataset', sa.Column('unit_id', sa.Integer(), sa.ForeignKey('tUnit.id'), nullable=True))
        op.drop_column('tDataset', 'unit')

    except Exception as e:
        log.exception(e)


    try:
        op.drop_constraint('unique name dimension', 'tAttr', type_='unique')
        op.create_unique_constraint('unique name dimension_id', 'tAttr', ['name', 'dimension_id'])
    except Exception as e:
        log.exception(e)

def downgrade():


    try:
        op.add_column('tAttr', sa.Column('dimension', sa.Integer, nullable=True))
        op.drop_column('tAttr', 'dimension_id')

        op.add_column('tTypeAttr', sa.Column('unit', sa.Integer(), nullable=True))
        op.drop_column('tTypeAttr', 'unit_id')

        op.add_column('tDataset', sa.Column('unit', sa.Integer(), nullable=True))
        op.drop_column('tDataset', 'unit_id')

    except Exception as e:
        log.exception(e)

