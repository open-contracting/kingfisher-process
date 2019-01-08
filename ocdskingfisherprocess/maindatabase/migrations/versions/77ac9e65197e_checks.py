"""checks

Revision ID: 77ac9e65197e
Revises: 45cd673618df
Create Date: 2019-01-09 14:24:45.358705

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '77ac9e65197e'
down_revision = '45cd673618df'
branch_labels = None
depends_on = None


def upgrade():
    # In theory you should be able to use "server_default=True" but that doesn't work with Booleans.

    op.add_column('collection',
                  sa.Column('check_data', sa.Boolean, nullable=True)
                  )
    op.execute(""" UPDATE collection SET check_data = 'f' """)
    op.execute(""" ALTER TABLE collection ALTER COLUMN check_data SET DEFAULT 'f' """)
    op.alter_column('collection', 'check_data', nullable=False)

    op.add_column('collection',
                  sa.Column('check_older_data_with_schema_version_1_1', sa.Boolean, nullable=True),
                  )
    op.execute(""" UPDATE collection SET check_older_data_with_schema_version_1_1 = 'f' """)
    op.execute(""" ALTER TABLE collection ALTER COLUMN check_older_data_with_schema_version_1_1 SET DEFAULT 'f' """)
    op.alter_column('collection', 'check_older_data_with_schema_version_1_1', nullable=False)


def downgrade():
    op.drop_column('collection', 'check_data')
    op.drop_column('collection', 'check_older_data_with_schema_version_1_1')
