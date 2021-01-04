"""template_inheritance

Revision ID: b04cb2e57cb0
Revises: 50bd5bac8701
Create Date: 2020-08-14 10:42:47.565737

"""
import logging
from alembic import op
import sqlalchemy as sa

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'b04cb2e57cb0'
down_revision = '50bd5bac8701'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        try:
            op.add_column('tTemplate', sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tTemplate.id'), nullable=True))
        except Exception as e:
            log.critical(e)
        try:
            op.add_column('tTemplateType', sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tTemplateType.id'), nullable=True))
        except Exception as e:
            log.critical(e)
        try:
            op.add_column('tTemplateType', sa.Column('status', sa.String(1), nullable=False, server_default=sa.text(u"'A'")))
        except Exception as e:
            log.critical(e)

            #change the primary key on tTypeAttr from (type_id, attr_id) to an auto-incremented (id)
        try:
            op.drop_constraint('tTypeAttr_ibfk_1', 'tTypeAttr', type_='foreignkey')
        except Exception as e:
            log.critical(e)
        try:
            op.drop_constraint('tTypeAttr_ibfk_2', 'tTypeAttr', type_='foreignkey')
        except Exception as e:
            log.critical(e)
        try:
            op.execute('alter table tTypeAttr drop primary key')
        except Exception as e:
            log.critical(e)
        try:
            op.execute('alter table tTypeAttr add column id int primary key auto_increment;')
        except Exception as e:
            log.critical(e)
        try:
            op.add_column('tTypeAttr', sa.Column('status', sa.String(1), nullable=False, server_default=sa.text(u"'A'")))
        except Exception as e:
            log.critical(e)

        try:
            op.add_column('tTypeAttr', sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tTypeAttr.id'), nullable=True))
        except Exception as e:
            log.critical(e)

        try:
            op.create_foreign_key('tTypeAttr_ibfk_1', 'tTypeAttr', 'tAttr', ['attr_id'], ['id'])
        except Exception as e:
            log.critical(e)
        try:
            op.create_foreign_key('tTypeAttr_ibfk_2', 'tTypeAttr', 'tTemplateType', ['type_id'], ['id'])
        except Exception as e:
            log.critical(e)
        try:
            op.add_column('tResourceType', sa.Column('child_template_id', sa.Integer(), sa.ForeignKey('tTemplate.id'), nullable=True))
        except Exception as e:
            log.critical(e)



def downgrade():
    if op.get_bind().dialect.name == 'mysql':
        try:
            op.drop_constraint('tTemplate_ibfk_1', 'tTypeAttr', type_='foreignkey')
            op.drop_column('tTemplate', 'parent_id')
            op.drop_constraint('tTemplateType_ibfk_2', 'tTypeAttr', type_='foreignkey')
            op.drop_column('tTemplateType', 'parent_id')


            op.drop_constraint(None, 'tTypeAttr', type_='primarykey')
            op.drop_constraint('tTypeAttr_ibfk_1', 'tTypeAttr', type_='foreignkey')
            op.drop_constraint('tTypeAttr_ibfk_2', 'tTypeAttr', type_='foreignkey')
            op.execute('alter table tTypeAttr add primary key (attr_id, type_id)')
            op.drop_column('tTypeAttr', 'parent_id')
            op.drop_column('tTypeAttr', 'id')
            op.drop_column('tTypeAttr', 'status')
            op.add_constraint('tTypeAttr_ibfk_1', 'tTypeAttr', type_='foreignkey')
            op.add_constraint('tTypeAttr_ibfk_2', 'tTypeAttr', type_='foreignkey')
            op.drop_column('tResourceType', 'child_template_id')
        except Exception as e:
            log.exception(e)
