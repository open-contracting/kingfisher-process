"""start

Revision ID: 45cd673618df
Revises:
Create Date: 2019-01-08 15:53:26.336666

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = '45cd673618df'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('collection',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('source_id', sa.Text, nullable=False),
                    sa.Column('data_version', sa.DateTime(timezone=False), nullable=False),
                    sa.Column('store_start_at', sa.DateTime(timezone=False), nullable=False),
                    sa.Column('store_end_at', sa.DateTime(timezone=False), nullable=True),
                    sa.Column('sample', sa.Boolean, nullable=False, default=False),
                    sa.UniqueConstraint('source_id', 'data_version', 'sample', name='unique_collection_identifiers'),
                    )

    op.create_table('collection_file',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_id', sa.Integer,
                              sa.ForeignKey("collection.id", name="fk_collection_file_collection_id"), nullable=False),
                    sa.Column('filename', sa.Text, nullable=True),
                    sa.Column('store_start_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('store_end_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('warnings', JSONB, nullable=True),
                    sa.UniqueConstraint('collection_id', 'filename', name='unique_collection_file_identifiers'),
                    )

    op.create_table('collection_file_item',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_id', sa.Integer,
                              sa.ForeignKey("collection_file.id", name="fk_collection_file_item_collection_file_id"),
                              nullable=False),
                    sa.Column('store_start_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('store_end_at', sa.DateTime(timezone=False),
                              nullable=True),
                    sa.Column('number', sa.Integer),
                    sa.UniqueConstraint('collection_file_id', 'number', name='unique_collection_file_item_identifiers'),
                    )

    op.create_table('data',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('hash_md5', sa.Text, nullable=False),
                    sa.Column('data', JSONB, nullable=False),
                    sa.UniqueConstraint('hash_md5', name='unique_data_hash_md5'),
                    )

    op.create_table('package_data',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('hash_md5', sa.Text, nullable=False),
                    sa.Column('data', JSONB, nullable=False),
                    sa.UniqueConstraint('hash_md5', name='unique_package_data_hash_md5'),
                    )

    op.create_table('release',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_item_id', sa.Integer,
                              sa.ForeignKey("collection_file_item.id", name="fk_release_collection_file_item_id"), nullable=False),
                    sa.Column('release_id', sa.Text, nullable=True),
                    sa.Column('ocid', sa.Text, nullable=True),
                    sa.Column('data_id', sa.Integer, sa.ForeignKey("data.id", name="fk_release_data_id"), nullable=False),
                    sa.Column('package_data_id', sa.Integer, sa.ForeignKey("package_data.id", name="fk_release_package_data_id"),
                              nullable=False),
                    )

    op.create_table('record',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_item_id', sa.Integer,
                              sa.ForeignKey("collection_file_item.id", name="fk_record_collection_file_item_id"), nullable=False),
                    sa.Column('ocid', sa.Text, nullable=True),
                    sa.Column('data_id', sa.Integer, sa.ForeignKey("data.id", name="fk_record_data_id"), nullable=False),
                    sa.Column('package_data_id', sa.Integer, sa.ForeignKey("package_data.id", name="fk_record_package_data_id"),
                              nullable=False),
                    )

    op.create_table('release_check',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('release_id', sa.Integer, sa.ForeignKey("release.id", name="fk_release_check_release_id"),
                              nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('cove_output', JSONB, nullable=False),
                    sa.UniqueConstraint('release_id', 'override_schema_version',
                                        name='unique_release_check_release_id_and_more')
                    )

    op.create_table('record_check',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('record_id', sa.Integer, sa.ForeignKey("record.id", name="fk_record_check_record_id"),
                              nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('cove_output', JSONB, nullable=False),
                    sa.UniqueConstraint('record_id', 'override_schema_version',
                                        name='unique_record_check_record_id_and_more')
                    )

    op.create_table('release_check_error',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('release_id', sa.Integer, sa.ForeignKey("release.id", name="fk_release_check_error_release_id"),
                              nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('error', sa.Text, nullable=False),
                    sa.UniqueConstraint('release_id', 'override_schema_version',
                                        name='unique_release_check_error_release_id_and_more')
                    )

    op.create_table('record_check_error',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('record_id', sa.Integer, sa.ForeignKey("record.id", name="fk_record_check_error_record_id"),
                              nullable=False),
                    sa.Column('override_schema_version', sa.Text, nullable=True),
                    sa.Column('error', sa.Text, nullable=False),
                    sa.UniqueConstraint('record_id', 'override_schema_version',
                                        name='unique_record_check_error_record_id_and_more')
                    )


def downgrade():
    pass
