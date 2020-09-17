"""add-collection-id

Revision ID: bdfc23d8334c
Revises: 0fa56a3d3f90
Create Date: 2020-01-09 08:30:01.478751

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'bdfc23d8334c'
down_revision = '0fa56a3d3f90'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'record',
        sa.Column('collection_id', sa.Integer,
                  sa.ForeignKey('collection.id', name='fk_record_collection_id'),
                  nullable=True))
    op.add_column(
        'release',
        sa.Column('collection_id', sa.Integer,
                  sa.ForeignKey('collection.id', name='fk_release_collection_id'),
                  nullable=True))
    op.add_column(
        'compiled_release',
        sa.Column('collection_id', sa.Integer,
                  sa.ForeignKey('collection.id', name='fk_compiled_release_collection_id'),
                  nullable=True))

    op.create_index('record_collection_id_idx', 'record', ['collection_id'])
    op.create_index('release_collection_id_idx', 'release', ['collection_id'])
    op.create_index('compiled_release_collection_id_idx', 'compiled_release', ['collection_id'])


def downgrade():
    op.drop_column('record', 'collection_id')
    op.drop_column('release', 'collection_id')
    op.drop_column('compiled_release', 'collection_id')
