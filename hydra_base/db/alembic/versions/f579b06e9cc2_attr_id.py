"""attr_id

Revision ID: f579b06e9cc2
Revises: dd1110144e53
Create Date: 2018-04-11 21:28:54.173256

"""
from alembic import op
import sqlalchemy as sa

import logging
log = logging.getLogger(__name__)


# revision identifiers, used by Alembic.
revision = 'f579b06e9cc2'
down_revision = 'dd1110144e53'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.alter_column('tAttr', 'attr_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tAttr', 'attr_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tAttr', 'attr_dimen', new_column_name='dimension', existing_type=sa.String(60), server_default=sa.text(u"'dimensionless'"))
            op.alter_column('tAttr', 'attr_description', new_column_name='description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)
    else: ## sqlite

        # ## tAttr
        try:
            op.drop_table('tAttr_new')
        except:
            log.info("tAttr_new isn't there")

        try:
            # ## tAttr
            op.create_table(
                'tAttr_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(60), nullable=False),
                sa.Column('dimension', sa.String(60),  nullable=False),
                sa.Column('description', sa.String(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            )

            op.execute("insert into tAttr_new (id, name, dimension, description, cr_date) select attr_id, attr_name, attr_dimen, attr_description, cr_date from tAttr")

            op.rename_table('tAttr','tAttr_old')
            op.rename_table('tAttr_new', 'tAttr')
            op.drop_table('tAttr_old')

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
            op.alter_column('tAttr', 'id', new_column_name='attr_id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tAttr', 'name', new_column_name='attr_name', existing_type=sa.String(60), nullable=False)
            op.alter_column('tAttr', 'dimension', new_column_name='attr_dimen', existing_type=sa.String(60), server_default=text(u"'dimensionless'"))
            op.alter_column('tAttr', 'description', new_column_name='attr_description', existing_type=sa.String(1000))
        except Exception as e:
            log.exception(e)
    else: ## sqlite

        # ## tAttr
        try:
            op.drop_table('tAttr_new')
        except:
            log.info("tAttr_new isn't there")

        try:
            # ## tAttr
            op.create_table(
                'tAttr_new',
                sa.Column('attr_id', sa.Integer, primary_key=True),
                sa.Column('attr_name', sa.Text(60), nullable=False),
                sa.Column('attr_dimen', sa.String(60),  nullable=False),
                sa.Column('attr_description', sa.String(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
            )

            op.execute("insert into tAttr_new (attr_id, attr_name, attr_dimen, attr_description, cr_date) select id, name, dimension, description, cr_date from tAttr")

            op.rename_table('tAttr','tAttr_old')
            op.rename_table('tAttr_new', 'tAttr')
            op.drop_table('tAttr_old')

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
