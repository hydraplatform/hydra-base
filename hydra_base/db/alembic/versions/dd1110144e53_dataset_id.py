"""dataset_id

Revision ID: dd1110144e53
Revises: 3dc489a7eac1
Create Date: 2018-04-10 22:55:42.740642

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'dd1110144e53'
down_revision = '3dc489a7eac1'
branch_labels = None
depends_on = None

# id         = Column(Integer(), primary_key=True, index=True, nullable=False)
# name       = Column(String(60),  nullable=False)
# type       = Column(String(60),  nullable=False)
# unit       = Column(String(60))
# hash       = Column(BIGINT(),  nullable=False, unique=True)

def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tDataset', 'dataset_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tDataset', 'data_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tDataset', 'data_type', new_column_name='type', existing_type=sa.String(60), nullable=False)
            op.alter_column('tDataset', 'data_units', new_column_name='unit', existing_type=sa.String(60))
            op.alter_column('tDataset', 'data_hash', new_column_name='hash', existing_type=sa.BIGINT(), nullable=False, unique=True)
            op.drop_column('tDataset', 'data_dimen')
            op.drop_column('tDataset', 'frequency')
            op.drop_column('tDataset', 'start_time')
        except Exception as e:
            log.exception(e)

        try:
            op.alter_column('tMetadata', 'metadata_name', new_column_name='key', existing_type=sa.String(60), nullable=False)
            op.alter_column('tMetadata', 'metadata_val', new_column_name='value', existing_type=sa.Text(1000).with_variant(mysql.TEXT(1000), 'mysql'), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tDataset
        try:
            op.drop_table('tDataset_new')
        except:
            log.info("tDataset_new isn't there")

        try:
            # ## tDataset
            op.create_table(
                'tDataset_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(60), nullable=False),
                sa.Column('type', sa.String(60),  nullable=False),
                sa.Column('unit', sa.String(60)),
                sa.Column('hash', sa.BIGINT(),  nullable=False, unique=True),
                sa.Column('value', sa.Text(),  nullable=True),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
            )

            op.execute("insert into tDataset_new (id, name, type, unit, hash, value, cr_date, created_by) select dataset_id, data_name, data_type, data_units, data_hash, value, cr_date, created_by from tDataset")

            op.rename_table('tDataset','tDataset_old')
            op.rename_table('tDataset_new', 'tDataset')
            op.drop_table('tDataset_old')

        except Exception as e:
            log.exception(e)

        # ## tMetadata
        try:
            op.drop_table('tMetadata_new')
        except:
            log.info("tMetadata_new isn't there")

        try:
            # ## tMetadata
            op.create_table(
                'tMetadata_new',
                sa.Column('dataset_id', sa.Integer, primary_key=True, index=True),
                sa.Column('key', sa.String(60), nullable=False, primary_key=True),
                sa.Column('value', sa.Text(),  nullable=False),
            )

            op.execute("insert into tMetadata_new (dataset_id, key, value) select dataset_id, metadata_name, metadata_val from tMetadata")

            op.rename_table('tMetadata','tMetadata_old')
            op.rename_table('tMetadata_new', 'tMetadata')
            op.drop_table('tMetadata_old')

        except Exception as e:
            log.exception(e)


def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tDataset', 'id', new_column_name='dataset_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tDataset', 'name', new_column_name='dasta_name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tDataset', 'type', new_column_name='data_type', existing_type=sa.String(60), nullable=False)
            op.alter_column('tDataset', 'unit', new_column_name='data_units', existing_type=sa.String(60))
            op.add_column('tDataset', 'data_dimen', sa.String(60))
        except Exception as e:
            log.exception(e)

        try:
            op.alter_column('tMetadata', 'key', new_column_name='metadata_name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tMetadata', 'value', new_column_name='metadata_val', existing_type=sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tDataset
        try:
            op.drop_table('tDataset_new')
        except:
            log.info("tDataset_new isn't there")

        try:
            # ## tDataset
            op.create_table(
                'tDataset_new',
                sa.Column('dataset_id', sa.Integer, primary_key=True),
                sa.Column('data_name', sa.Text(60), nullable=False),
                sa.Column('data_type', sa.String(60),  nullable=False),
                sa.Column('data_units', sa.String(60)),
                sa.Column('data_dimen', sa.String(60)),
                sa.Column('data_hash', sa.BIGINT(),  nullable=False, unique=True),
                sa.Column('value', sa.Text(),  nullable=True),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.Column('start_time', sa.Text(60),  nullable=True),
                sa.Column('frequency', sa.Text(10),  nullable=True),
            )

            op.execute("insert into tDataset_new (dataset_id, data_name, data_type, data_units, data_hash, value, cr_date, created_by) select id, name, type, unit, hash, value, cr_date, created_by from tDataset")

            op.rename_table('tDataset','tDataset_old')
            op.rename_table('tDataset_new', 'tDataset')
            op.drop_table('tDataset_old')

        except Exception as e:
            log.exception(e)
        # ## tMetadata
        try:
            op.drop_table('tMetadata_new')
        except:
            log.info("tMetadata_new isn't there")
        try:
            # ## tMetadata
            op.create_table(
                'tMetadata_new',
                sa.Column('dataset_id', sa.Integer, primary_key=True, index=True),
                sa.Column('metadata_name', sa.String(60), nullable=False, primary_key=True),
                sa.Column('metadata_val', sa.Text(),  nullable=False),
            )

            op.execute("insert into tMetadata_new (dataset_id, metadata_name, metadata_val) select dataset_id, key, value from tMetadata")

            op.rename_table('tMetadata','tMetadata_old')
            op.rename_table('tMetadata_new', 'tMetadata')
            op.drop_table('tMetadata_old')

        except Exception as e:
            log.exception(e)
