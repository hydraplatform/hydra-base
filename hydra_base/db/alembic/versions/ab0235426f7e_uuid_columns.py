"""uuid_columns

Revision ID: ab0235426f7e
Revises: 877adf863b33
Create Date: 2024-09-10 12:35:33.789052

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ab0235426f7e"
down_revision = "877adf863b33"
branch_labels = None
depends_on = None


def upgrade():
    try:
        op.add_column(
            "tProject",
            sa.Column(
                "uuid",
                sa.VARCHAR(36),
                unique=True,
            ),
        )
        #insert project UUIDs

        op.add_column(
            "tNetwork",
            sa.Column(
                "uuid",
                sa.VARCHAR(36),
                unique=True,
            ),
        )
        #insert network UUIDs

        
    except Exception as e:
        print(f"Unable to add returns_data column to app table: {e}")


def downgrade():
    try:
        op.drop_column("tProject", "uuid")
        op.drop_column("tNetwork", "uuid")
    except:
        print("Unable to remove uuid columns")
