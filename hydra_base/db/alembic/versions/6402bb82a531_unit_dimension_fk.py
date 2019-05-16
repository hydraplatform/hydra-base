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
    if op.get_bind().dialect.name == 'mysql':

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

    else: #sqlite
        """ tDataset """
        try:
            op.drop_table('tDataset_new')
        except:
            log.info("tDataset_new isn't there")
        try:
            op.create_table(
                'tDataset_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(200),  nullable=False),
                sa.Column('type', sa.String(1000), nullable=False),
                sa.Column('unit_id', sa.Integer(), sa.ForeignKey('tUnit.id'), nullable=True),
                sa.Column('hash', sa.BIGINT(),  nullable=False, unique=True),
                sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                sa.Column('hidden', sa.String(200),  nullable=False, server_default=sa.text(u'N')),
                sa.Column('value', sa.String(1000))
            )

            op.execute("insert into tDataset_new (id, name, type, hash, cr_date, created_by, hidden, value) select id, name, type, hash, cr_date, created_by, hidden, value from tDataset")

            op.rename_table('tDataset','tDataset_old')
            op.rename_table('tDataset_new', 'tDataset')
            op.drop_table('tDataset_old')

        except Exception as e:
            log.exception(e)

        try:
            op.drop_table('tAttr_new')
        except:
            log.info("tAttr_new isn't there")
        """ tAttr """
        try:
            op.create_table(
                'tAttr_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(200),  nullable=False, unique=True),
                sa.Column('dimension_id', sa.Integer(), sa.ForeignKey('tDimension.id'), nullable=True),
                sa.Column('description', sa.String(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP'))
            )

            op.execute("insert into tAttr_new (id, name, description, cr_date) select id, name, description, cr_date from tAttr")

            op.rename_table('tAttr','tAttr_old')
            op.rename_table('tAttr_new', 'tAttr')
            op.drop_table('tAttr_old')

        except Exception as e:
            log.exception(e)

        try:
            op.drop_table('tTypeAttr_new')
        except:
            log.info("tTypeAttr_new isn't there")
        """ tTypeAttr """
        try:
            op.create_table(
                'tTypeAttr_new',
                sa.Column('attr_id', sa.Integer(), sa.ForeignKey('tAttr.id'), primary_key=True, nullable=False),
                sa.Column('type_id', sa.Integer(), sa.ForeignKey('tTemplateType.id', ondelete='CASCADE'), primary_key=True, nullable=False),
                sa.Column('default_dataset_id', sa.Integer(), sa.ForeignKey('tDataset.id')),
                sa.Column('attr_is_var', sa.String(200), server_default=sa.text(u'N'), nullable=False),
                sa.Column('data_type', sa.String(1000)),
                sa.Column('data_restriction', sa.String(1000)),
                sa.Column('unit_id', sa.Integer(), sa.ForeignKey('tUnit.id'), nullable=True),
                sa.Column('description', sa.String(1000)),
                sa.Column('properties', sa.String(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(), server_default=sa.text(u'CURRENT_TIMESTAMP'), nullable=False)
            )

            op.execute("insert into tTypeAttr_new (attr_id, type_id, default_dataset_id, attr_is_var, data_type, data_restriction, description, properties, cr_date) select attr_id, type_id, default_dataset_id, attr_is_var, data_type, data_restriction, description, properties, cr_date from tTypeAttr")

            op.rename_table('tTypeAttr','tTypeAttr_old')
            op.rename_table('tTypeAttr_new', 'tTypeAttr')
            op.drop_table('tTypeAttr_old')

        except Exception as e:
            log.exception(e)

def downgrade():

    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tAttr', sa.Column('dimension', sa.Integer, nullable=True))
            op.drop_column('tAttr', 'dimension_id')

            op.add_column('tTypeAttr', sa.Column('unit', sa.Integer(), nullable=True))
            op.drop_column('tTypeAttr', 'unit_id')

            op.add_column('tDataset', sa.Column('unit', sa.Integer(), nullable=True))
            op.drop_column('tDataset', 'unit_id')

        except Exception as e:
            log.exception(e)

    else:
        pass
