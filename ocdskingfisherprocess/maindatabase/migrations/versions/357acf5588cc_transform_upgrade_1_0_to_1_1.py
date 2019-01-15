"""transform_upgrade_1_0_to_1_1

Revision ID: 357acf5588cc
Revises: 4b3ef3e46d75
Create Date: 2019-01-11 15:18:35.749060

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '357acf5588cc'
down_revision = '4b3ef3e46d75'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'transform_upgrade_1_0_to_1_1_status_release',
        sa.Column(
            'source_release_id',
            sa.Integer,
            sa.ForeignKey(
                "release.id",
                name="fk_transform_upgrade_1_0_to_1_1_status_release_source_release_id"
            ),
            nullable=False,
            primary_key=True
        )
    )

    op.create_table(
        'transform_upgrade_1_0_to_1_1_status_record',
        sa.Column(
            'source_record_id',
            sa.Integer,
            sa.ForeignKey(
                "record.id",
                name="fk_transform_upgrade_1_0_to_1_1_status_record_source_record_id"
            ),
            nullable=False,
            primary_key=True
        )
    )


def downgrade():
    pass
