"""hybrid_attr_phase1

Revision ID: 9b9f7d7a4f21
Revises: 580425ade2e4, 877adf863b33, cec2b77ad85e
Create Date: 2026-07-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9b9f7d7a4f21'
down_revision = ('580425ade2e4', '877adf863b33', 'cec2b77ad85e')
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('tResourceAttr', sa.Column('attr_name', sa.String(length=200), nullable=True))
    op.add_column('tResourceAttr', sa.Column('dimension_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_tresourceattr_dimension_id', 'tResourceAttr', 'tDimension', ['dimension_id'], ['id'])

    op.add_column('tTypeAttr', sa.Column('attr_name', sa.String(length=200), nullable=True))
    op.add_column('tTypeAttr', sa.Column('dimension_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_ttypeattr_dimension_id', 'tTypeAttr', 'tDimension', ['dimension_id'], ['id'])

    op.execute(
        """
        UPDATE tResourceAttr
        SET attr_name = (
                SELECT name FROM tAttr WHERE tAttr.id = tResourceAttr.attr_id
            ),
            dimension_id = (
                SELECT dimension_id FROM tAttr WHERE tAttr.id = tResourceAttr.attr_id
            )
        WHERE attr_id IS NOT NULL
        """
    )

    op.execute(
        """
        UPDATE tTypeAttr
        SET attr_name = (
                SELECT name FROM tAttr WHERE tAttr.id = tTypeAttr.attr_id
            ),
            dimension_id = (
                SELECT dimension_id FROM tAttr WHERE tAttr.id = tTypeAttr.attr_id
            )
        WHERE attr_id IS NOT NULL
        """
    )

    op.alter_column('tResourceAttr', 'attr_id', existing_type=sa.Integer(), nullable=True)
    op.alter_column('tTypeAttr', 'attr_id', existing_type=sa.Integer(), nullable=True)


def downgrade():
    op.alter_column('tTypeAttr', 'attr_id', existing_type=sa.Integer(), nullable=False)
    op.alter_column('tResourceAttr', 'attr_id', existing_type=sa.Integer(), nullable=False)

    op.drop_constraint('fk_ttypeattr_dimension_id', 'tTypeAttr', type_='foreignkey')
    op.drop_column('tTypeAttr', 'dimension_id')
    op.drop_column('tTypeAttr', 'attr_name')

    op.drop_constraint('fk_tresourceattr_dimension_id', 'tResourceAttr', type_='foreignkey')
    op.drop_column('tResourceAttr', 'dimension_id')
    op.drop_column('tResourceAttr', 'attr_name')
