"""lengthen names

Revision ID: 52d59176f0da
Revises: 6883f6e39176
Create Date: 2018-12-27 16:07:41.052385

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '52d59176f0da'
down_revision = '6883f6e39176'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':


        try:
            op.alter_column('tTemplate', 'name', existing_type=sa.String(60), type_=sa.String(200), nullable=False, unique=True)
            op.add_column('tTemplate', sa.Column('description', sa.String(1000)))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tDataset_new')
        except:
            log.info("tDataset_new isn't there")

        try:
            op.create_table(
                'tTemplate_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(200),  nullable=False, unique=True),
                sa.Column('description', sa.String(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('layout', sa.Text().with_variant(sa.mysql.TEXT(4294967295), 'mysql'),  nullable=True)
            )

            op.execute("insert into tTemplate_new (id, name, description, cr_date, layout) select id, name, '', cr_date, layout from tTemplate")

            op.rename_table('tTemplate','tTemplate_old')
            op.rename_table('tTemplate_new', 'tTemplate')
            op.drop_table('tTemplate_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.alter_column('tTemplate', 'name', existing_type=sa.String(60), type_=sa.String(200), nullable=False, unique=True)
            op.drop_column('tTemplate',  'description')
        except Exception as e:
            log.exception(e)

    else: ## sqlite
        try:
            op.create_table(
                'tTemplate_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(60),  nullable=False, unique=True),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('layout', sa.Text().with_variant(sa.mysql.TEXT(4294967295), 'mysql'),  nullable=True)
            )

            op.execute("insert into tTemplate_new (id, name, cr_date, layout) select id, name, cr_date, layout from tTemplate")

            op.rename_table('tTemplate','tTemplate_old')
            op.rename_table('tTemplate_new', 'tTemplate')
            op.drop_table('tTemplate_old')

        except Exception as e:
            log.exception(e)
