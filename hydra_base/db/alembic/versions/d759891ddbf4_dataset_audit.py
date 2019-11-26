"""dataset_audit

Revision ID: d759891ddbf4
Revises: da2a4916c900
Create Date: 2019-11-14 13:44:19.028436

"""
from alembic import op
import sqlalchemy as sa

import datetime

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'd759891ddbf4'
down_revision = 'da2a4916c900'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tScenario
        try:
            op.add_column('tDataset', sa.Column('updated_by', sa.Integer(), sa.ForeignKey('tUser.id'), nullable=True))
            op.add_column('tDataset', sa.Column('updated_at', sa.DateTime(), default=datetime.datetime.utcnow(), onupdate=datetime.datetime.utcnow(), nullable=True))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tDataset_new')
        except:
            log.info('tDataset_new does not exist')

        try:
            # ## tScenario
            op.create_table(
                'tDataset_new',
                sa.Column('id', sa.Integer(), primary_key=True, index=True, nullable=False),
                sa.Column('name', sa.String(200),  nullable=False),
                sa.Column('type', sa.String(60),  nullable=False),
                sa.Column('unit_id', sa.Integer(), sa.ForeignKey('tUnit.id'),  nullable=True),
                sa.Column('hash', sa.BIGINT(),  nullable=False, unique=True),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                sa.Column('updated_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                sa.Column('updated_at', sa.DateTime(), sa.ForeignKey('tUser.id')),
                sa.Column('hidden', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('value', sa.Text(),  nullable=True)
            )

            op.execute("insert into tDataset_new (id, name, type, unit_id, hash, cr_date, created_by, updated_by, updated_at, hidden, value) select id, name, type, unit_id, hash, cr_date, created_by, null, null, hidden, value from tDataset")

            op.rename_table('tDataset','tDataset_old')
            op.rename_table('tDataset_new', 'tDataset')
            op.drop_table('tDataset_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tScenario
        try:
            op.drop_column('tDataset',  'updated_at')
            op.drop_column('tDataset', 'updated_by')
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tDataset_new')
        except:
            log.info('tDataset_new does not exist')

        try:
            # ## tScenario
            op.create_table(
                'tDataset_new',
                sa.Column('id', sa.Integer(), primary_key=True, index=True, nullable=False),
                sa.Column('name', sa.String(200),  nullable=False),
                sa.Column('type', sa.String(60),  nullable=False),
                sa.Column('unit_id', sa.Integer(), sa.ForeignKey('tUnit.id'),  nullable=True),
                sa.Column('hash', sa.BIGINT(),  nullable=False, unique=True),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                sa.Column('hidden', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('value', sa.Text(),  nullable=True)
            )

            op.execute("insert into tDataset_new (id, name, type, unit_id, hash, cr_date, created_by, hidden, value) select id, name, type, unit_id, hash, cr_date, created_by, hidden, value from tDataset")

            op.rename_table('tDataset','tDataset_old')
            op.rename_table('tDataset_new', 'tDataset')
            op.drop_table('tDataset_old')

        except Exception as e:
            log.exception(e)
