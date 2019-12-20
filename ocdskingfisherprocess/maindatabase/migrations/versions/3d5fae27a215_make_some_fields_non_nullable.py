"""Make some fields non-nullable

Revision ID: 3d5fae27a215
Revises: b2ff4e525bcb
Create Date: 2019-12-17 13:35:21.791027

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '3d5fae27a215'
down_revision = 'b2ff4e525bcb'
branch_labels = None
depends_on = None


def upgrade():
    # When changing a column that is part of a constraint, its new value must not cause a unique violation error.

    # SELECT collection_id, COUNT(*) FROM collection_file WHERE filename IS NULL GROUP BY collection_id
    # HAVING COUNT(*) > 1;
    # 0 rows
    op.execute("UPDATE collection_file SET filename = '' WHERE filename IS NULL")
    op.alter_column('collection_file', 'filename', nullable=False)

    # SELECT collection_file_id, COUNT(*) FROM collection_file_item WHERE number IS NULL GROUP BY collection_file_id
    # HAVING COUNT(*) > 1;
    # 0 rows
    # number is an integer, which has no 'no data' representation, besides NULL.
    op.alter_column('collection_file_item', 'number', nullable=False)

    # SELECT release_id, COUNT(*) FROM release_check WHERE override_schema_version IS NULL GROUP BY release_id
    # HAVING COUNT(*) > 1;
    # 0 rows
    op.execute("UPDATE release_check SET override_schema_version = '' WHERE override_schema_version IS NULL")
    op.alter_column('release_check', 'override_schema_version', nullable=False)

    # SELECT record_id, COUNT(*) FROM record_check WHERE override_schema_version IS NULL GROUP BY record_id
    # HAVING COUNT(*) > 1;
    # 0 rows
    op.execute("UPDATE record_check SET override_schema_version = '' WHERE override_schema_version IS NULL")
    op.alter_column('record_check', 'override_schema_version', nullable=False)

    # SELECT release_id, COUNT(*) FROM release_check_error WHERE override_schema_version IS NULL GROUP BY release_id
    # HAVING COUNT(*) > 1;
    # 0 rows
    op.execute("UPDATE release_check_error SET override_schema_version = '' WHERE override_schema_version IS NULL")
    op.alter_column('release_check_error', 'override_schema_version', nullable=False)

    # SELECT record_id, COUNT(*) FROM record_check_error WHERE override_schema_version IS NULL GROUP BY record_id
    # HAVING COUNT(*) > 1;
    # 0 rows
    op.execute("UPDATE record_check_error SET override_schema_version = '' WHERE override_schema_version IS NULL")
    op.alter_column('record_check_error', 'override_schema_version', nullable=False)

    # The following columns have indexes, but not constraints.
    op.execute("UPDATE release SET release_id = '' WHERE release_id IS NULL")
    op.execute("UPDATE release SET ocid = '' WHERE ocid IS NULL")
    op.execute("UPDATE record SET ocid = '' WHERE ocid IS NULL")
    op.execute("UPDATE compiled_release SET ocid = '' WHERE ocid IS NULL")
    op.alter_column('release', 'release_id', nullable=False)
    op.alter_column('release', 'ocid', nullable=False)
    op.alter_column('record', 'ocid', nullable=False)
    op.alter_column('compiled_release', 'ocid', nullable=False)

    # The following columns have neither indexes nor constraints.
    op.execute("UPDATE collection_file SET url = '' WHERE url IS NULL")
    op.alter_column('collection_file', 'url', nullable=False)


def downgrade():
    pass
