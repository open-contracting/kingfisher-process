"""item-warnings-errors

Revision ID: 3811a084cc66
Revises: b66e30eb6816
Create Date: 2019-03-15 15:14:24.254514

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = '3811a084cc66'
down_revision = 'b66e30eb6816'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('collection_file_item',
                  sa.Column('warnings', JSONB, nullable=True),
                  )
    op.add_column('collection_file_item',
                  sa.Column('errors', JSONB, nullable=True),
                  )


def downgrade():
    op.drop_column('collection_file', 'warnings')
    op.drop_column('collection_file', 'errors')
