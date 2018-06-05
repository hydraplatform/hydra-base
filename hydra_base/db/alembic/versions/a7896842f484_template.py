"""template

Revision ID: a7896842f484
Revises: f579b06e9cc2
Create Date: 2018-04-13 08:44:21.634411

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a7896842f484'
down_revision = 'f579b06e9cc2'
branch_labels = None
depends_on = None

import logging
log = logging.getLogger(__name__)


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ## tTemplate

        try:
            op.alter_column('tTemplate', 'template_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tTemplate', 'template_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
        except Exception as e:
            log.exception(e)

        # ## tTemplateType

        try:
            op.alter_column('tTemplateType', 'type_id', new_column_name='id', existing_type=sa.Integer(), autoincrement=True, nullable=False)
            op.alter_column('tTemplateType', 'type_name', new_column_name='name', existing_type=sa.String(60), nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tTemplate
        try:
            op.drop_table('tTemplate_new')
        except:
            log.info("tTemplate_new isn't there")

        try:
            # ## tTemplate
            op.create_table(
                'tTemplate_new',
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.Text(60), nullable=False, unique=True),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('layout', sa.Text(1000)),
            )

            op.execute("insert into tTemplate_new (id, name, cr_date, layout) select template_id, template_name, cr_date, layout from tTemplate")

            op.rename_table('tTemplate','tTemplate_old')
            op.rename_table('tTemplate_new', 'tTemplate')
            op.drop_table('tTemplate_old')

        except Exception as e:
            log.exception(e)

        # ## tTemplateType
        try:
            op.drop_table('tTemplateType_new')
        except:
            log.info("tTemplateType_new isn't there")

        try:
            # ## tTemplateType
            op.create_table(
                'tTemplateType_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(60),  nullable=False, unique=True),
                sa.Column('template_id', sa.Integer(), sa.ForeignKey('tTemplate.id'), nullable=False),
                sa.Column('resource_type', sa.String(60)),
                sa.Column('alias', sa.String(100)),
                sa.Column('layout', sa.Text(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('template_id', 'name', 'resource_type', name="unique type name"),
            )
            op.execute("insert into tTemplateType_new (name, id, template_id, resource_type, alias, layout, cr_date) select type_name, type_id, template_id, resource_type, alias, layout, cr_date from tTemplateType")

            op.rename_table('tTemplateType','tTemplateType_old')
            op.rename_table('tTemplateType_new', 'tTemplateType')
            op.drop_table('tTemplateType_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    pass
