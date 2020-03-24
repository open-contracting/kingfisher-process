"""collection-options

Revision ID: ea7c906e135e
Revises: 413c84a833f5
Create Date: 2020-03-24 14:07:22.505690

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = 'ea7c906e135e'
down_revision = '413c84a833f5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('collection',
                  sa.Column('options', JSONB, nullable=False, default=sa.text("'{}'::jsonb"),
                            server_default=sa.text("'{}'::jsonb")),
                  )


def downgrade():
    op.drop_column('collection', 'options')
