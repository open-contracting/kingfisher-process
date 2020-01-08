"""Add collection_id to release, record and compiled_release

Revision ID: 294a65dc6c29
Revises: 0fa56a3d3f90
Create Date: 2019-12-18 15:56:34.374799

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '294a65dc6c29'
down_revision = '0fa56a3d3f90'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('record', sa.Column('collection_id', sa.Integer, sa.ForeignKey('collection.id',
                  name='fk_record_collection_id'), nullable=True))
    op.add_column('release', sa.Column('collection_id', sa.Integer, sa.ForeignKey('collection.id',
                  name='fk_release_collection_id'), nullable=True))
    op.add_column('compiled_release', sa.Column('collection_id', sa.Integer, sa.ForeignKey('collection.id',
                  name='fk_compiled_release_collection_id'), nullable=True))

    op.create_index('record_collection_id_idx', 'record', ['collection_id'])
    op.create_index('release_collection_id_idx', 'release', ['collection_id'])
    op.create_index('compiled_release_collection_id_idx', 'compiled_release', ['collection_id'])

    op.execute(
        "UPDATE record SET collection_id = record_with_collection.collection_id "
        "FROM record_with_collection WHERE record.id = record_with_collection.id")
    op.execute(
        "UPDATE release SET collection_id = release_with_collection.collection_id "
        "FROM release_with_collection WHERE release.id = release_with_collection.id")
    op.execute(
        "UPDATE compiled_release SET collection_id = compiled_release_with_collection.collection_id "
        "FROM compiled_release_with_collection WHERE compiled_release.id = compiled_release_with_collection.id")

    op.alter_column('record', 'collection_id', nullable=False)
    op.alter_column('release', 'collection_id', nullable=False)
    op.alter_column('compiled_release', 'collection_id', nullable=False)


def downgrade():
    op.drop_column('record', 'collection_id')
    op.drop_column('release', 'collection_id')
    op.drop_column('compiled_release', 'collection_id')
