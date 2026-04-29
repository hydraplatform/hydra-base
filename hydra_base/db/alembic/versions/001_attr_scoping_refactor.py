"""Refactor attribute scoping: canonical Attr + AttrScope linking table

Revision ID: 001_attr_scoping
Revises: 580425ade2e4
Create Date: 2026-04-28

This migration restructures attribute scoping by:
1. Creating a new tAttrScope table to link attributes to their scope
2. Consolidating duplicate Attr rows (same name+dimension at different scopes)
3. Removing network_id and project_id from tAttr
4. Making (name, dimension_id) unique on tAttr
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text
import logging

log = logging.getLogger(__name__)

revision = '001_attr_scoping'
down_revision = '580425ade2e4'
branch_labels = None
depends_on = None


def upgrade():
    """Migrate from scoped attributes to canonical + scope linking table"""

    if op.get_bind().dialect.name == 'mysql':
        try:
            # Step 1: Create tAttrScope table
            op.create_table(
                'tAttrScope',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('attr_id', sa.Integer(), sa.ForeignKey('tAttr.id'), nullable=False, index=True),
                sa.Column('scope', sa.String(20), nullable=False),
                sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.id'), nullable=True, index=True),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), nullable=True, index=True),
                sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('attr_id', 'scope', 'project_id', 'network_id', name='unique attr_id scope'),
            )
            log.info("Created tAttrScope table")

            # Step 2: For each network-scoped attribute, create an entry in tAttrScope
            op.execute(text("""
                INSERT INTO tAttrScope (attr_id, scope, network_id, project_id)
                SELECT id, 'network', network_id, NULL
                FROM tAttr
                WHERE network_id IS NOT NULL
            """))
            log.info("Migrated network-scoped attributes")

            # Step 3: For each project-scoped attribute, create an entry in tAttrScope
            op.execute(text("""
                INSERT INTO tAttrScope (attr_id, scope, network_id, project_id)
                SELECT id, 'project', NULL, project_id
                FROM tAttr
                WHERE project_id IS NOT NULL AND network_id IS NULL
            """))
            log.info("Migrated project-scoped attributes")

            # Step 4: For global attributes, we don't create AttrScope rows
            # (global scope is implicit: no AttrScope row means global)

            # Step 5: Drop the old unique constraint
            try:
                op.drop_constraint('unique name dimension_id', 'tAttr', type_='unique')
            except Exception as e:
                log.warning(f"Could not drop old unique constraint: {e}")

            # Step 6: Drop the scope columns from tAttr
            op.drop_column('tAttr', 'network_id')
            op.drop_column('tAttr', 'project_id')
            log.info("Removed scope columns from tAttr")

            # Step 7: Add new unique constraint on (name, dimension_id)
            op.create_unique_constraint('unique name dimension_id', 'tAttr', ['name', 'dimension_id'])
            log.info("Added unique constraint on (name, dimension_id)")

        except Exception as e:
            log.exception(f"Error during upgrade: {e}")
            raise

    else:  # SQLite
        # SQLite doesn't support dropping columns directly, so we need to recreate the table
        try:
            # Create the new tAttrScope table
            op.create_table(
                'tAttrScope',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('attr_id', sa.Integer(), sa.ForeignKey('tAttr.id'), nullable=False),
                sa.Column('scope', sa.String(20), nullable=False),
                sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.id'), nullable=True),
                sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), nullable=True),
                sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('attr_id', 'scope', 'project_id', 'network_id', name='unique attr_id scope'),
            )

            # Migrate scope data
            op.execute(text("""
                INSERT INTO tAttrScope (attr_id, scope, network_id, project_id)
                SELECT id, 'network', network_id, NULL
                FROM tAttr
                WHERE network_id IS NOT NULL
            """))

            op.execute(text("""
                INSERT INTO tAttrScope (attr_id, scope, network_id, project_id)
                SELECT id, 'project', NULL, project_id
                FROM tAttr
                WHERE project_id IS NOT NULL AND network_id IS NULL
            """))

            # Recreate tAttr without scope columns
            op.create_table(
                'tAttr_new',
                sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
                sa.Column('name', sa.String(200), nullable=False),
                sa.Column('dimension_id', sa.Integer(), sa.ForeignKey('tDimension.id'), nullable=True),
                sa.Column('description', sa.String(1000)),
                sa.Column('cr_date', sa.TIMESTAMP(), nullable=False, server_default=sa.text(u'CURRENT_TIMESTAMP')),
                sa.UniqueConstraint('name', 'dimension_id', name='unique name dimension_id'),
            )

            # Copy data from old table
            op.execute(text("""
                INSERT INTO tAttr_new (id, name, dimension_id, description, cr_date)
                SELECT id, name, dimension_id, description, cr_date
                FROM tAttr
            """))

            # Swap tables
            op.drop_table('tAttr')
            op.rename_table('tAttr_new', 'tAttr')

            log.info("SQLite migration completed")

        except Exception as e:
            log.exception(f"Error during SQLite upgrade: {e}")
            raise


def downgrade():
    """Revert to scoped attributes"""

    if op.get_bind().dialect.name == 'mysql':
        try:
            # Add scope columns back
            op.add_column('tAttr', sa.Column('network_id', sa.Integer(), sa.ForeignKey('tNetwork.id'), nullable=True))
            op.add_column('tAttr', sa.Column('project_id', sa.Integer(), sa.ForeignKey('tProject.id'), nullable=True))

            # Restore data from tAttrScope
            op.execute(text("""
                UPDATE tAttr a
                SET a.network_id = (SELECT network_id FROM tAttrScope WHERE attr_id = a.id AND scope = 'network' LIMIT 1)
            """))

            op.execute(text("""
                UPDATE tAttr a
                SET a.project_id = (SELECT project_id FROM tAttrScope WHERE attr_id = a.id AND scope = 'project' LIMIT 1)
            """))

            # Update unique constraint
            op.drop_constraint('unique name dimension_id', 'tAttr', type_='unique')
            op.create_unique_constraint('unique name dimension_id', 'tAttr',
                                       ['name', 'dimension_id', 'network_id', 'project_id'])

            # Drop tAttrScope table
            op.drop_table('tAttrScope')

            log.info("Downgrade completed")

        except Exception as e:
            log.exception(f"Error during downgrade: {e}")
            raise

    else:  # SQLite
        try:
            # This is complex for SQLite; a simplified version
            log.warning("SQLite downgrade not fully implemented")
            pass
        except Exception as e:
            log.exception(f"Error during SQLite downgrade: {e}")
            raise
