"""index on release and record file item

Revision ID: d8a32a738af4
Revises: 3811a084cc66
Create Date: 2019-03-26 17:51:45.836678

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'd8a32a738af4'
down_revision = '3811a084cc66'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('release_collection_file_item_id_idx', 'release', ['collection_file_item_id'])
    op.create_index('record_collection_file_item_id_idx', 'record', ['collection_file_item_id'])

    op.create_index('release_ocid_idx', 'release', ['ocid'])
    op.create_index('record_ocid_idx', 'record', ['ocid'])
    op.create_index('compiled_release_ocid_idx', 'compiled_release', ['ocid'])


def downgrade():
    op.drop_index('release_collection_file_item_id_idx')
    op.drop_index('record_collection_file_item_id_idx')

    op.drop_index('release_ocid_idx')
    op.drop_index('record_ocid_idx')
    op.drop_index('compiled_release_ocid_idx')
