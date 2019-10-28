"""collection_cached_counts

Revision ID: 53699ddc9872
Revises: b6563e8f5cb7
Create Date: 2019-10-28 16:22:33.458857

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '53699ddc9872'
down_revision = 'b6563e8f5cb7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('collection',
                  sa.Column('cached_releases_count', sa.Integer, nullable=True)
                  )
    op.add_column('collection',
                  sa.Column('cached_records_count', sa.Integer, nullable=True),
                  )
    op.add_column('collection',
                  sa.Column('cached_compiled_releases_count', sa.Integer, nullable=True),
                  )


def downgrade():
    op.drop_column('collection', 'cached_releases_count')
    op.drop_column('collection', 'cached_records_count')
    op.drop_column('collection', 'cached_compiled_releases_count')
