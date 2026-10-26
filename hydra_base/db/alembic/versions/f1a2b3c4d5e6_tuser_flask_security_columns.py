"""Add Flask-Security columns to tUser and backfill from hwi.user

Revision ID: f1a2b3c4d5e6
Revises: b7f3e1a92c44
Create Date: 2026-07-16 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
import uuid

import logging
log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = 'f1a2b3c4d5e6'
down_revision = 'b7f3e1a92c44'
branch_labels = None
depends_on = None

ROOT_USER_ID = 1
ROOT_PLACEHOLDER_EMAIL = 'root@system.internal'

NEW_COLUMNS = [
    sa.Column('email', sa.String(255), nullable=True),
    sa.Column('active', sa.Boolean(), nullable=True),
    sa.Column('confirmed_at', sa.TIMESTAMP(), nullable=True),
    sa.Column('first_name', sa.String(255), nullable=True),
    sa.Column('last_name', sa.String(255), nullable=True),
    sa.Column('demographic', sa.String(255), nullable=True),
    sa.Column('country_code', sa.String(255), nullable=True),
    sa.Column('organization', sa.String(255), nullable=True),
    sa.Column('current_login_at', sa.TIMESTAMP(), nullable=True),
    sa.Column('last_login_ip', sa.String(255), nullable=True),
    sa.Column('current_login_ip', sa.String(255), nullable=True),
    sa.Column('login_count', sa.Integer(), nullable=True),
    sa.Column('fs_uniquifier', sa.String(255), nullable=True),
]


def upgrade():
    bind = op.get_bind()

    for col in NEW_COLUMNS:
        try:
            op.add_column('tUser', col)
        except Exception as e:
            log.exception(e)

    # Backfill from hwi's `user` table -- same physical database, matched
    # 1:1 on email/username except tUser.id == ROOT_USER_ID, which has no
    # corresponding hwi login and is handled separately below.
    try:
        bind.execute(sa.text("""
            UPDATE tUser t
            JOIN user u ON LOWER(u.email) = LOWER(t.username)
            SET t.email = u.email,
                t.active = u.active,
                t.confirmed_at = u.confirmed_at,
                t.first_name = u.first_name,
                t.last_name = u.last_name,
                t.demographic = u.demographic,
                t.country_code = u.country_code,
                t.organization = u.organization,
                t.current_login_at = u.current_login_at,
                t.last_login_ip = u.last_login_ip,
                t.current_login_ip = u.current_login_ip,
                t.login_count = u.login_count
        """))
    except Exception as e:
        log.exception(e)

    # Root/system user has no hwi.user counterpart: give it a synthetic,
    # inactive identity so email/fs_uniquifier stay NOT NULL uniformly
    # rather than carving out a permanent nullable exception.
    try:
        bind.execute(
            sa.text("""
                UPDATE tUser
                SET email = :email, active = FALSE
                WHERE id = :root_id AND email IS NULL
            """),
            {"email": ROOT_PLACEHOLDER_EMAIL, "root_id": ROOT_USER_ID},
        )
    except Exception as e:
        log.exception(e)

    # fs_uniquifier has no natural source value -- generate one per row.
    try:
        result = bind.execute(sa.text("SELECT id FROM tUser WHERE fs_uniquifier IS NULL"))
        for (user_id,) in result:
            bind.execute(
                sa.text("UPDATE tUser SET fs_uniquifier = :fsu WHERE id = :uid"),
                {"fsu": uuid.uuid4().hex, "uid": user_id},
            )
    except Exception as e:
        log.exception(e)

    # Tighten constraints now that every row is populated.
    try:
        op.alter_column('tUser', 'email', type_=sa.String(255), nullable=False)
        op.alter_column('tUser', 'active', type_=sa.Boolean(), nullable=False, server_default=sa.true())
        op.alter_column('tUser', 'fs_uniquifier', type_=sa.String(255), nullable=False)
        op.create_unique_constraint('uq_tuser_email', 'tUser', ['email'])
        op.create_unique_constraint('uq_tuser_fs_uniquifier', 'tUser', ['fs_uniquifier'])
    except Exception as e:
        log.exception(e)


def downgrade():
    try:
        op.drop_constraint('uq_tuser_fs_uniquifier', 'tUser', type_='unique')
        op.drop_constraint('uq_tuser_email', 'tUser', type_='unique')
    except Exception as e:
        log.exception(e)

    for col in reversed(NEW_COLUMNS):
        try:
            op.drop_column('tUser', col.name)
        except Exception as e:
            log.exception(e)
