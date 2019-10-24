"""index

Revision ID: b6563e8f5cb7
Revises: 0a8ab8b2756f
Create Date: 2019-10-24 14:38:44.345928

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b6563e8f5cb7'
down_revision = '0a8ab8b2756f'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('compiled_release_collection_file_item_id_idx', 'compiled_release', ['collection_file_item_id'])


def downgrade():
    op.drop_index('compiled_release_collection_file_item_id_idx')
