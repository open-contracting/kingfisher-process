"""compiled_release

Revision ID: 4b3ef3e46d75
Revises: 77ac9e65197e
Create Date: 2019-01-10 12:54:51.904662

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '4b3ef3e46d75'
down_revision = '77ac9e65197e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('compiled_release',

                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_file_item_id', sa.Integer,
                              sa.ForeignKey("collection_file_item.id",
                                            name="fk_complied_release_collection_file_item_id"),
                              nullable=False),
                    sa.Column('ocid', sa.Text, nullable=True),
                    sa.Column('data_id', sa.Integer,
                              sa.ForeignKey("data.id", name="fk_complied_release_data_id"), nullable=False),
                    )

    op.add_column('collection',

                  sa.Column('transform_type', sa.Text, nullable=True),
                  )

    op.add_column('collection',
                  sa.Column('transform_from_collection_id', sa.Integer,
                            sa.ForeignKey("collection.id"), nullable=True)
                  )

    op.drop_constraint('unique_collection_identifiers', 'collection')
    op.create_unique_constraint('unique_collection_identifiers', 'collection', ['source_id', 'data_version', 'sample',
                                                                                'transform_from_collection_id',
                                                                                'transform_type'])


def downgrade():
    pass
