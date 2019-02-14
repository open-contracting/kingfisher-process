"""Add 'deleted_at in collections

Revision ID: 8add39cb253d
Revises: aef60ba6c0bc
Create Date: 2019-02-14 11:44:09.162034

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8add39cb253d'
down_revision = 'aef60ba6c0bc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('collection', sa.Column('deleted_at', sa.DateTime, timezone=False, nullable=True))

    op.execute(""" UPDATE collection SET deleted_at = NULL """)
    op.execute(""" ALTER TABLE collection ALTER COLUMN check_data SET DEFAULT NULL """)
    op.alter_column('collection', 'deleted_at', timezone=False, nullable=False)


def downgrade():
    op.drop_column('collection', 'deleted_at')
