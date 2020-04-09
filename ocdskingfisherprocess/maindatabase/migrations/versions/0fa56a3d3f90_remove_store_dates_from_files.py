"""Remove store dates from files

Revision ID: 0fa56a3d3f90
Revises: 8e3f80979dc9
Create Date: 2019-12-18 15:35:02.475651

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '0fa56a3d3f90'
down_revision = '8e3f80979dc9'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('collection_file', 'store_start_at')
    op.drop_column('collection_file', 'store_end_at')

    op.drop_column('collection_file_item', 'store_start_at')
    op.drop_column('collection_file_item', 'store_end_at')


def downgrade():
    op.add_column('collection_file', sa.Column('store_start_at', sa.DateTime(timezone=False), nullable=True))
    op.add_column('collection_file', sa.Column('store_end_at', sa.DateTime(timezone=False), nullable=True))

    op.add_column('collection_file_item', sa.Column('store_start_at', sa.DateTime(timezone=False), nullable=True))
    op.add_column('collection_file_item', sa.Column('store_end_at', sa.DateTime(timezone=False), nullable=True))
