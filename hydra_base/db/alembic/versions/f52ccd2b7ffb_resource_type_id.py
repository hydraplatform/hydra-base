"""resource_type_id

Revision ID: f52ccd2b7ffb
Revises: a48ad41579de
Create Date: 2018-04-17 15:01:35.411693

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f52ccd2b7ffb'
down_revision = 'a48ad41579de'
branch_labels = None
depends_on = None

import logging
log = logging.getLogger(__name__)


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tResourceType

        try:
            op.alter_column('tResourceType', 'resource_type_id', new_column_name='id', existing_type=sa.Integer(), primary_key=True, autoincrement=True, nullable=False)
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        # ## tResourceType
        try:
            op.drop_table('tResourceType_new')
        except:
            log.info("tResourceType_new isn't there")

        try:
            # ## tResourceType
            op.create_table(
                'tResourceType_new',
                    sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                    sa.Column('type_id', sa.Integer(), sa.ForeignKey('tTemplateType.id'), primary_key=False, nullable=False),
                    sa.Column('ref_key', sa.String(60)),
                    sa.Column('network_id', sa.Integer(),  sa.ForeignKey('tNetwork.id'), nullable=True),
                    sa.Column('node_id', sa.Integer(),  sa.ForeignKey('tNode.id'), nullable=True),
                    sa.Column('link_id', sa.Integer(),  sa.ForeignKey('tLink.id'), nullable=True),
                    sa.Column('group_id', sa.Integer(),  sa.ForeignKey('tResourceGroup.id'), nullable=True),
                    sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                    sa.UniqueConstraint('network_id', 'type_id', name='net_type_1'),
                    sa.UniqueConstraint('node_id', 'type_id', name='node_type_1'),
                    sa.UniqueConstraint('link_id', 'type_id',  name = 'link_type_1'),
                    sa.UniqueConstraint('group_id', 'type_id', name = 'group_type_1'),
            )

            op.execute("insert into tResourceType_new (id, type_id, ref_key, network_id, node_id, link_id, group_id, cr_date) select resource_type_id, type_id, ref_key, network_id, node_id, link_id, group_id, cr_date from tResourceType")

            op.rename_table('tResourceType','tResourceType_old')
            op.rename_table('tResourceType_new', 'tResourceType')
            op.drop_table('tResourceType_old')

        except Exception as e:
            log.exception(e)




def downgrade():
    pass
