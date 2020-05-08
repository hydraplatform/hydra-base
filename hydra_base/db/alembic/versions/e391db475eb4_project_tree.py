"""project_tree

Revision ID: e391db475eb4
Revises: 35088e32c557
Create Date: 2020-05-08 11:48:15.006752

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e391db475eb4'
down_revision = '35088e32c557'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject
        try:
            op.add_column('tProject', sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tProject.id'), nullable=True))
            op.add_column('tProject', sa.Column('scenario_id', sa.Integer(), sa.ForeignKey('tScenario.id', use_alter=True), nullable=True))
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tProject_new')
        except:
            log.info('tProject_new does not exist')

        try:
            # ## tProject
            op.create_table(
                'tProject_new',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('layout', sa.Text(1000)),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), index=True),
                sa.Column('start_time', sa.String(60)),
                sa.Column('end_time', sa.String(60)),
                sa.Column('locked', sa.String(1),  nullable=False, server_default=sa.text(u"'N'")),
                sa.Column('time_step', sa.String(60)),
                sa.Column('parent_id', sa.Integer(), sa.ForeignKey('tProject.id')),
                sa.Column('scenario_id', sa.Integer(), sa.ForeignKey('tScenario.id', user_alter=True), nullable=True),
                sa.UniqueConstraint('network_id', 'name', name="unique scenario name"),
            )

            op.execute("insert into tProject_new (id, name, description, status, cr_date, created_by, parent_id, scenario_id) select id, name, description, status, cr_date, created_by, null, null from tProject")

            op.rename_table('tProject','tProject_old')
            op.rename_table('tProject_new', 'tProject')
            op.drop_table('tProject_old')

        except Exception as e:
            log.exception(e)

def downgrade():
    if op.get_bind().dialect.name == 'mysql':

        # ### tProject

        try:
            op.drop_column('tProject', 'parent_id')
            op.drop_column('tProject', 'scenario_id')
        except Exception as e:
            log.exception(e)

    else: ## sqlite

        try:
            op.drop_table('tProject_new')
        except:
            log.info('tProject_new does not exist')

        try:
            # ## tProject
            op.create_table(
                'tProject_new',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('name', sa.Text(200), nullable=False),
                sa.Column('description', sa.Text(1000)),
                sa.Column('status', sa.String(1),  nullable=False, server_default=sa.text(u"'A'")),
                sa.Column('cr_date', sa.TIMESTAMP(),  nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.Column('created_by', sa.Integer(), sa.ForeignKey('tUser.id'),  nullable=False),
                sa.UniqueConstraint('network_id', 'scenario_name', name="unique scenario name"),
            )

            op.execute("insert into tProject_new (id, name, description, status, cr_date, created_by) select id, name, description, status, cr_date, created_by from tProject")

            op.rename_table('tProject','tProject_old')
            op.rename_table('tProject_new', 'tProject')
            op.drop_table('tProject_old')

        except Exception as e:
            log.exception(e)
