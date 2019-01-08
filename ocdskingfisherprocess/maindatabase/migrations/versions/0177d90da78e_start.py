"""start

Revision ID: 0177d90da78e
Revises:
Create Date: 2018-12-14 08:58:07.868680

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = '0177d90da78e'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('collection',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('source_id', sa.Text, nullable=False),
                    sa.Column('data_version', sa.Text, nullable=False),
                    sa.Column('store_start_at', sa.DateTime(timezone=False), nullable=False),
                    sa.Column('store_end_at', sa.DateTime(timezone=False), nullable=True),
                    sa.Column('sample', sa.Boolean, nullable=False, default=False),
                    sa.UniqueConstraint('source_id', 'data_version', 'sample'),
                    )

    op.create_table('collection_file',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_id', sa.Integer,
                              sa.ForeignKey("collection.id"), nullable=False),
                    sa.Column('filename', sa.Text, nullable=True),
                    sa.Column('store_start_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('store_end_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('warnings', JSONB, nullable=True),
                    sa.UniqueConstraint('collection_id', 'filename'),
                    )

    op.create_table('collection_file_item',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_id', sa.Integer,
                              sa.ForeignKey("collection_file.id"),
                              nullable=False),
                    sa.Column('store_start_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('store_end_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('number', sa.Integer),
                    sa.UniqueConstraint('collection_file_id', 'number'),
                    )

    op.create_table('data',

                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('hash_md5', sa.Text, nullable=False, unique=True),
                    sa.Column('data', JSONB, nullable=False),
                    )

    op.create_table('package_data',

                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('hash_md5', sa.Text, nullable=False, unique=True),
                    sa.Column('data', JSONB, nullable=False),
                    )

    op.create_table('release',

                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_item_id', sa.Integer,
                              sa.ForeignKey("collection_file_item.id"), nullable=False),
                    sa.Column('release_id', sa.Text, nullable=True),
                    sa.Column('ocid', sa.Text, nullable=True),
                    sa.Column('data_id', sa.Integer, sa.ForeignKey("data.id"), nullable=False),
                    sa.Column('package_data_id', sa.Integer, sa.ForeignKey("package_data.id"),
                              nullable=False),
                    )

    op.create_table('record',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_item_id', sa.Integer,
                              sa.ForeignKey("collection_file_item.id"), nullable=False),
                    sa.Column('ocid', sa.Text, nullable=True),
                    sa.Column('data_id', sa.Integer, sa.ForeignKey("data.id"), nullable=False),
                    sa.Column('package_data_id', sa.Integer, sa.ForeignKey("package_data.id"),
                              nullable=False),
                    )

    op.create_table('release_check',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('release_id', sa.Integer, sa.ForeignKey("release.id"), index=True,
                              unique=False, nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('cove_output', JSONB, nullable=False),
                    sa.UniqueConstraint('release_id', 'override_schema_version',
                                        name='ix_release_check_release_id_and_more')
                    )

    op.create_table('record_check',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('record_id', sa.Integer, sa.ForeignKey("record.id"), index=True,
                              unique=False,
                              nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('cove_output', JSONB, nullable=False),
                    sa.UniqueConstraint('record_id', 'override_schema_version',
                                        name='ix_record_check_record_id_and_more')
                    )

    op.create_table('release_check_error',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('release_id', sa.Integer, sa.ForeignKey("release.id"),
                              index=True,
                              unique=False, nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('error', sa.Text, nullable=False),
                    sa.UniqueConstraint('release_id', 'override_schema_version',
                                        name='ix_release_check_error_release_id_and_more')
                    )

    op.create_table('record_check_error',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('record_id', sa.Integer, sa.ForeignKey("record.id"),
                              index=True,
                              unique=False, nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('error', sa.Text, nullable=False),
                    sa.UniqueConstraint('record_id', 'override_schema_version',
                                        name='ix_record_check_error_record_id_and_more')
                    )


def downgrade():
    pass
