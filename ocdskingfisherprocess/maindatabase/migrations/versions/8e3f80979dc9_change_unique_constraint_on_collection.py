"""Change unique constraint on collection

Revision ID: 8e3f80979dc9
Revises: 3d5fae27a215
Create Date: 2019-12-18 13:14:56.466907

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8e3f80979dc9'
down_revision = '3d5fae27a215'
branch_labels = None
depends_on = None


def upgrade():
    """
    SELECT source_id, data_version, sample, COUNT(*) FROM collection
    WHERE transform_type IS NULL or transform_type = ''
    GROUP BY source_id, data_version, sample
    HAVING COUNT(*) > 1;
    """
    # 0 rows

    op.drop_constraint('unique_collection_identifiers', 'collection')
    op.create_index('unique_collection_identifiers', 'collection', ['source_id', 'data_version', 'sample'],
                    unique=True, postgresql_where=sa.text("transform_type = ''"))


def downgrade():
    op.drop_index('unique_collection_identifiers', 'collection')
    op.create_unique_constraint('unique_collection_identifiers', 'collection', [
        'source_id', 'data_version', 'sample', 'transform_from_collection_id', 'transform_type',
    ])
