"""note

Revision ID: b66e30eb6816
Revises: 8add39cb253d
Create Date: 2019-02-26 13:09:27.596374

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b66e30eb6816'
down_revision = '8add39cb253d'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('collection_note',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('collection_id', sa.Integer,
                              sa.ForeignKey("collection.id",
                                            name="fk_collection_file_collection_id"),
                              nullable=False),
                    sa.Column('note', sa.Text, nullable=False),
                    sa.Column('stored_at', sa.DateTime(timezone=False), nullable=False),
                    )


def downgrade():
    op.drop_table('collection_note')
