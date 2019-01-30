"""url

Revision ID: c2b053a5bb53
Revises: 357acf5588cc
Create Date: 2019-01-30 13:47:52.205581

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c2b053a5bb53'
down_revision = '357acf5588cc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('collection_file',
                  sa.Column('url', sa.Text, nullable=True),
                  )


def downgrade():
    op.drop_column('collection_file', 'url')
