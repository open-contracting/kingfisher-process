"""Add indexes to foreign keys

Revision ID: b2ff4e525bcb
Revises: 53699ddc9872
Create Date: 2019-12-17 13:23:54.335035

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'b2ff4e525bcb'
down_revision = '53699ddc9872'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('collection_transform_from_collection_id_idx', 'collection', ['transform_from_collection_id'])
    op.create_index('collection_file_collection_id_idx', 'collection_file', ['collection_id'])
    op.create_index('collection_file_item_collection_file_id_idx', 'collection_file_item', ['collection_file_id'])
    op.create_index('collection_note_collection_id_idx', 'collection_note', ['collection_id'])
    op.create_index('record_package_data_id_idx', 'record', ['package_data_id'])
    op.create_index('record_check_record_id_idx', 'record_check', ['record_id'])
    op.create_index('record_check_error_record_id_idx', 'record_check_error', ['record_id'])
    op.create_index('release_package_data_id_idx', 'release', ['package_data_id'])
    op.create_index('release_check_release_id_idx', 'release_check', ['release_id'])
    op.create_index('release_check_error_release_id_idx', 'release_check_error', ['release_id'])


def downgrade():
    op.drop_index('collection_transform_from_collection_id_idx')
    op.drop_index('collection_file_collection_id_idx')
    op.drop_index('collection_file_item_collection_file_id_idx')
    op.drop_index('collection_note_collection_id_idx')
    op.drop_index('record_package_data_id_idx')
    op.drop_index('record_check_record_id_idx')
    op.drop_index('record_check_error_record_id_idx')
    op.drop_index('release_package_data_id_idx')
    op.drop_index('release_check_release_id_idx')
    op.drop_index('release_check_error_release_id_idx')
