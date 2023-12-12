"""node_alt_coordinates

Revision ID: f848e9eb4bf3
Revises: 877adf863b33
Create Date: 2023-10-25 10:09:45.543368

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = 'f848e9eb4bf3'
down_revision = '877adf863b33'
branch_labels = None
depends_on = None


def upgrade():
    try:
        op.add_column('tNode', sa.Column('alt_x', sa.Float(precision=10, asdecimal=True)))
    except:
        print("Error adding lat column to tNode")
    try:
        op.add_column('tNode', sa.Column('alt_y', sa.Float(precision=10, asdecimal=True)))
    except:
        print("Error adding lon column to tNode")
    
    conn = op.get_bind()
    conn.execute(
        text(
            """
                update tNode n, tNetwork nt set 
                    n.alt_x=n.x,
                    n.alt_y=n.y 
                where
                    n.id=n.id
                    and n.network_id=nt.id
                    and nt.projection is not null;
            """
        )
    )

    conn.execute(
        text(
            """
                update tNode n, tNetwork nt set 
                    n.x=null,
                    n.y=null
                where
                    n.id=n.id
                    and n.network_id=nt.id
                    and nt.projection is not null;
            """
        )
    )

def downgrade():
    try:
        op.drop_column('tNode', 'lat')
    except:
        print("Error dropping lat column from tNode ")
    try:
        op.drop_column('tNode', 'lon')
    except:
        print("Error dropping lon column from tNode ")
