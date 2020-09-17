"""delete-indexes

Revision ID: 0a8ab8b2756f
Revises: d8a32a738af4
Create Date: 2019-09-12 15:37:47.306066

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '0a8ab8b2756f'
down_revision = 'd8a32a738af4'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('release_data_id_idx', 'release', ['data_id'])
    op.create_index('record_data_id_idx', 'record', ['data_id'])
    op.create_index('compiled_release_data_id_idx', 'compiled_release', ['data_id'])


def downgrade():
    op.drop_index('release_data_id_idx')
    op.drop_index('record_data_id_idx')
    op.drop_index('compiled_release_data_id_idx')
