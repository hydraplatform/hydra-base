"""attribute_scoping

Revision ID: a81a860cda39
Revises: cec2b77ad85e
Create Date: 2021-12-16 13:48:55.286610

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a81a860cda39'
down_revision = 'cec2b77ad85e'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tAttr', sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.id'), nullable=True))
        except Exception as e:
            log.critical(e)

        try:
            op.add_column('tAttr', sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), nullable=True))
        except Exception as e:
            log.critical(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.drop_column('tAttr', sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.id'), nullable=True))
        except Exception as e:
            log.critical(e)

        try:
            op.drop_column('tAttr', sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), nullable=True))
        except Exception as e:
            log.critical(e)
