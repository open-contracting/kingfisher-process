"""view

Revision ID: aef60ba6c0bc
Revises: a9e65f49b868
Create Date: 2019-02-06 14:19:41.539672

"""
from alembic import op
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = 'aef60ba6c0bc'
down_revision = 'a9e65f49b868'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute(
        text(
            """
                CREATE VIEW release_with_collection
                AS
                SELECT
                release.id,
                release.collection_file_item_id,
                release.release_id,
                release.ocid,
                release.data_id,
                release.package_data_id,
                collection_file.collection_id AS collection_id
                FROM release
                JOIN collection_file_item on collection_file_item.id = release.collection_file_item_id
                JOIN collection_file on collection_file.id = collection_file_item.collection_file_id
            """
        )
    )

    conn.execute(
        text(
            """
                CREATE VIEW record_with_collection
                AS
                SELECT
                record.id,
                record.collection_file_item_id,
                record.ocid,
                record.data_id,
                record.package_data_id,
                collection_file.collection_id AS collection_id
                FROM record
                JOIN collection_file_item on collection_file_item.id = record.collection_file_item_id
                JOIN collection_file on collection_file.id = collection_file_item.collection_file_id
            """
        )
    )

    conn.execute(
        text(
            """
                CREATE VIEW compiled_release_with_collection
                AS
                SELECT
                compiled_release.id,
                compiled_release.collection_file_item_id,
                compiled_release.ocid,
                compiled_release.data_id,
                collection_file.collection_id AS collection_id
                FROM compiled_release
                JOIN collection_file_item on collection_file_item.id = compiled_release.collection_file_item_id
                JOIN collection_file on collection_file.id = collection_file_item.collection_file_id
            """
        )
    )


def downgrade():
    conn = op.get_bind()
    conn.execute(text("""DROP VIEW IF EXISTS release_with_collection"""))
    conn.execute(text("""DROP VIEW IF EXISTS record_with_collection"""))
    conn.execute(text("""DROP VIEW IF EXISTS compiled_release_with_collection"""))
