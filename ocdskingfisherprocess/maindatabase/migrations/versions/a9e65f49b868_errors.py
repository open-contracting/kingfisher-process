"""errors

Revision ID: a9e65f49b868
Revises: c2b053a5bb53
Create Date: 2019-01-30 15:23:37.424889

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = 'a9e65f49b868'
down_revision = 'c2b053a5bb53'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('collection_file',
                  sa.Column('errors', JSONB, nullable=True),
                  )


def downgrade():
    op.drop_column('collection_file', 'errors')
