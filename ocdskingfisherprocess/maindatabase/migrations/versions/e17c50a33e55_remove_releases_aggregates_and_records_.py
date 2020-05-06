"""Remove releases_aggregates and records_aggregates from CoVE's output

Revision ID: e17c50a33e55
Revises: 413c84a833f5
Create Date: 2020-05-06 16:03:50.769144

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'e17c50a33e55'
down_revision = '413c84a833f5'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE release_check SET cove_output = cove_output - 'releases_aggregates'")
    op.execute("UPDATE record_check SET cove_output = cove_output - 'records_aggregates'")


def downgrade():
    pass
