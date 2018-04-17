"""dataset_collection

Revision ID: a48ad41579de
Revises: 08f0ebe40290
Create Date: 2018-04-17 14:43:11.872611

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'a48ad41579de'
down_revision = '08f0ebe40290'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tDatasetCollection

        try:
            op.alter_column('tDatasetCollection', 'collection_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tDatasetCollection', 'collection_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tDatasetCollection
        try:
            op.drop_table('tDatasetCollection_new')
        except:
            log.info("tDatasetCollection_new isn't there")

        try:
            # ## tDatasetCollection
            op.create_table(
                'tDatasetCollection_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.Text(60), nullable=False),
                sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP'))
            )

            op.execute("insert into tDatasetCollection_new (id, name, cr_date) select collection_id, collection_name, cr_date from tDatasetCollection")

            op.rename_table('tDatasetCollection','tDatasetCollection_old')
            op.rename_table('tDatasetCollection_new', 'tDatasetCollection')
            op.drop_table('tDatasetCollection_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tDatasetCollection', 'id', new_column_name='collection_id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tDatasetCollection', 'name', new_column_name='collection_name', existing_type=sa.String(60), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tDatasetCollection
        try:
            op.drop_table('tDatasetCollection_new')
        except:
            log.info("tDatasetCollection_new isn't there")

        try:
            # ## tDatasetCollection
            op.create_table(
                'tDatasetCollection_new',
                sa.Column('collection_id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('collection_name', sa.Text(60), nullable=False),
                sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP'))
            )

            op.execute("insert into tDatasetCollection_new (collection_id, collection_name, cr_date) select id, name, cr_date from tDatasetCollection")

            op.rename_table('tDatasetCollection','tDatasetCollection_old')
            op.rename_table('tDatasetCollection_new', 'tDatasetCollection')
            op.drop_table('tDatasetCollection_old')

        except Exception as e:
            log.exception(e)
