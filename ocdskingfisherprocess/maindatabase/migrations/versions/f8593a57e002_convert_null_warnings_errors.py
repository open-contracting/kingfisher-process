"""convert-null-warnings-errors

Revision ID: f8593a57e002
Revises: e17c50a33e55
Create Date: 2020-09-28 13:52:34.377134

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'f8593a57e002'
down_revision = 'e17c50a33e55'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("update collection_file set errors = NULL where errors = 'null';")
    op.execute("update collection_file set warnings = NULL where warnings = 'null';")
    op.execute("update collection_file_item set errors = NULL where errors = 'null';")
    op.execute("update collection_file_item set warnings = NULL where warnings = 'null';")


def downgrade():
    # It's not possible to downgrade this as we don't know which data to change back.
    # Nor is it needed; the database can handle both kinds of NULLS ok.
    pass
