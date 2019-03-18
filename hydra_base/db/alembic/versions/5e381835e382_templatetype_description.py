"""templatetype_description

Revision ID: 5e381835e382
Revises: 52d59176f0da
Create Date: 2018-12-28 16:49:49.192505

"""
from alembic import op
import sqlalchemy as sa


import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '5e381835e382'
down_revision = '52d59176f0da'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.alter_column('tTemplateType', 'name', existing_type=sa.String(60), type_=sa.String(200), nullable=False, unique=True)
            op.add_column('tTemplateType', sa.Column('description', sa.String(1000)))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tTemplateType_new')
        except:
            log.info("tTemplateType_new isn't there")

        try:
            op.create_table(
                'tTemplateType_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(200),  nullable=False, unique=True),
                sa.Column('description', sa.String(1000)),
                sa.Column('template_id', sa.Integer(), sa.ForeignKey('tTemplate.id'), nullable=False),
                sa.Column('resource_type', sa.String(200)),
                sa.Column('alias' ,sa.String(100)),

                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('layout', sa.Text(),  nullable=True)
            )

            op.execute("insert into tTemplateType_new (id, name, description, template_id, resource_type, alias, cr_date, layout) select id, name, '', template_id, resource_type, alias, cr_date, layout from tTemplate")

            op.rename_table('tTemplateType','tTemplateType_old')
            op.rename_table('tTemplateType_new', 'tTemplateType')
            op.drop_table('tTemplateType_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.alter_column('tTemplateType', 'name', existing_type=sa.String(60), type_=sa.String(200), nullable=False, unique=True)
            op.drop_column('tTemplateType',  'description')
        except Exception as e:
            log.exception(e)

    else: ## sqlite
        try:
            op.create_table(
                'tTemplateType_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(60),  nullable=False, unique=True),
                sa.Column('template_id', sa.Integer(), sa.ForeignKey('tTemplate.id'), nullable=False),
                sa.Column('resource_type', sa.String(200)),
                sa.Column('alias' ,sa.String(100)),

                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('layout', sa.Text(),  nullable=True)
            )

            op.execute("insert into tTemplateType_new (id, name, template_id, resource_type, alias, cr_date, layout) select id, name, template_id, resource_type, alias, cr_date, layout from tTemplateType")

            op.rename_table('tTemplateType','tTemplateType_old')
            op.rename_table('tTemplateType_new', 'tTemplateType')
            op.drop_table('tTemplateType_old')

        except Exception as e:
            log.exception(e)
