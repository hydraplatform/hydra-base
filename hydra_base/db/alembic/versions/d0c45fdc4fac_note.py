"""note

Revision ID: d0c45fdc4fac
Revises: 56ea2e814437
Create Date: 2018-04-17 16:28:03.816876

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'd0c45fdc4fac'
down_revision = '56ea2e814437'
branch_labels = None
depends_on = None

def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tNote

        try:
            op.alter_column('tNote', 'note_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tNote', 'note_text', new_column_name='value', existing_type=sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'), nullable=True)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tNote
        try:
            op.drop_table('tNote_new')
        except:
            log.info("tNote_new isn't there")

        try:
            # ## tNote
            op.create_table(
                'tNote_new',
                    sa.Column('id',sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('ref_key',sa.String(60),  nullable=False, index=True),
                    sa.Column('value',sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True),
                    sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                    sa.Column('cr_date',sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.Column('scenario_id',sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=False),
                    sa.Column('project_id',sa.Integer(),  sa.ForeignKey('tProject.id'), index=True, nullable=True),
                    sa.Column('network_id',sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True),
                    sa.Column('node_id',sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                    sa.Column('link_id',sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                    sa.Column('group_id',sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),
            )

            op.execute("insert into tNote_new (id, ref_key, value, created_by, cr_date, scenario_id, project_id, network_id, node_id, link_id, group_id) select note_id, ref_key, note_text, created_by, cr_date, scenario_id, project_id, network_id, node_id, link_id, group_id from tNote")

            op.rename_table('tNote','tNote_old')
            op.rename_table('tNote_new', 'tNote')
            op.drop_table('tNote_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tNote

        try:
            op.alter_column('tNote', 'id', new_column_name='note_id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
            op.alter_column('tNote', 'value', new_column_name='note_text', existing_type=sa.Text().with_variant(mysql.TEXT(4294967295), 'mysql'), nullable=True)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tNote
        try:
            op.drop_table('tNote_new')
        except:
            log.info("tNote_new isn't there")

        try:
            # ## tNote
            op.create_table(
                'tNote_new',
                    sa.Column('note_id',sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('ref_key',sa.String(60),  nullable=False, index=True),
                    sa.Column('note_text',sa.LargeBinary(),  nullable=True),
                    sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id')),
                    sa.Column('cr_date',sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.Column('scenario_id',sa.Integer(), sa.ForeignKey('tScenario.id'),  nullable=False),
                    sa.Column('project_id',sa.Integer(),  sa.ForeignKey('tProject.id'), index=True, nullable=True),
                    sa.Column('network_id',sa.Integer(),  sa.ForeignKey('tNetwork.id'), index=True, nullable=True),
                    sa.Column('node_id',sa.Integer(),  sa.ForeignKey('tNode.id'), index=True, nullable=True),
                    sa.Column('link_id',sa.Integer(),  sa.ForeignKey('tLink.id'), index=True, nullable=True),
                    sa.Column('group_id',sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), index=True, nullable=True),
            )

            op.execute("insert into tNote_new (note_id, ref_key, note_text, created_by, cr_date, scenario_id, project_id, network_id, node_id, link_id, group_id) select id, ref_key, value, created_by, cr_date, scenario_id, project_id, network_id, node_id, link_id, group_id from tNote")

            op.rename_table('tNote','tNote_old')
            op.rename_table('tNote_new', 'tNote')
            op.drop_table('tNote_old')

        except Exception as e:
            log.exception(e)
