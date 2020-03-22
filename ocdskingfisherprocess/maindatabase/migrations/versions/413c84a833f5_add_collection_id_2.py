"""add_collection_id_2

Revision ID: 413c84a833f5
Revises: bdfc23d8334c
Create Date: 2020-01-13 13:59:44.130399

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '413c84a833f5'
down_revision = 'bdfc23d8334c'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    res = conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'record_with_collection'")
    if res.scalar():
        op.execute(
            "UPDATE record SET collection_id = record_with_collection.collection_id "
            "FROM record_with_collection WHERE record.id = record_with_collection.id")

    res = conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'release_with_collection'")
    if res.scalar():
        op.execute(
            "UPDATE release SET collection_id = release_with_collection.collection_id "
            "FROM release_with_collection WHERE release.id = release_with_collection.id")

    res = conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'compiled_release_with_collection'")
    if res.scalar():
        op.execute(
            "UPDATE compiled_release SET collection_id = compiled_release_with_collection.collection_id "
            "FROM compiled_release_with_collection WHERE compiled_release.id = compiled_release_with_collection.id")

    op.alter_column('record', 'collection_id', nullable=False)
    op.alter_column('release', 'collection_id', nullable=False)
    op.alter_column('compiled_release', 'collection_id', nullable=False)


def downgrade():
    op.alter_column('record', 'collection_id', nullable=True)
    op.alter_column('release', 'collection_id', nullable=True)
    op.alter_column('compiled_release', 'collection_id', nullable=True)
