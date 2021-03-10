import collections
import datetime
import json
import logging
import os
from functools import partial

import alembic.config
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from ocdskingfisherprocess.models import CollectionModel, CollectionNoteModel, FileItemModel, FileModel
from ocdskingfisherprocess.signals import KINGFISHER_SIGNALS
from ocdskingfisherprocess.util import get_hash_md5_for_data


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class DuplicateFileItemRowError(Exception):
    pass


class DataBase:

    def __init__(self, config):
        self.config = config
        self._engine = None

        self.metadata = sa.MetaData()

        self.collection_table = sa.Table('collection', self.metadata,
                                         sa.Column('id', sa.Integer, primary_key=True),
                                         sa.Column('source_id', sa.Text, nullable=False),
                                         sa.Column('data_version', sa.DateTime(timezone=False), nullable=False),
                                         sa.Column('store_start_at', sa.DateTime(timezone=False), nullable=False),
                                         sa.Column('store_end_at', sa.DateTime(timezone=False), nullable=True),
                                         sa.Column('sample', sa.Boolean, nullable=False, default=False),
                                         sa.Column('check_data', sa.Boolean, nullable=False, default=False),
                                         sa.Column('check_older_data_with_schema_version_1_1', sa.Boolean,
                                                   nullable=False, default=False),
                                         sa.Column('transform_from_collection_id', sa.Integer,
                                                   sa.ForeignKey("collection.id"), nullable=True),
                                         sa.Column('transform_type', sa.Text, nullable=False),
                                         sa.Column('deleted_at', sa.DateTime(timezone=False), nullable=True),
                                         sa.Column('cached_releases_count', sa.Integer, nullable=True),
                                         sa.Column('cached_records_count', sa.Integer, nullable=True),
                                         sa.Column('cached_compiled_releases_count', sa.Integer, nullable=True),
                                         sa.Index('unique_collection_identifiers',
                                                  'source_id', 'data_version', 'sample',
                                                  unique=True,
                                                  postgresql_where=sa.text("transform_type = ''")),
                                         sa.Index('collection_transform_from_collection_id_idx',
                                                  'transform_from_collection_id'),
                                         )

        self.collection_note_table = sa.Table('collection_note', self.metadata,
                                              sa.Column('id', sa.Integer, primary_key=True),
                                              sa.Column('collection_id', sa.Integer,
                                                        sa.ForeignKey("collection.id",
                                                                      name="fk_collection_file_collection_id"),
                                                        nullable=False),
                                              sa.Column('note', sa.Text, nullable=False),
                                              sa.Column('stored_at', sa.DateTime(timezone=False), nullable=False),
                                              sa.Index('collection_note_collection_id_idx', 'collection_id'),
                                              )

        self.collection_file_table = sa.Table('collection_file', self.metadata,
                                              sa.Column('id', sa.Integer, primary_key=True),
                                              sa.Column('collection_id', sa.Integer,
                                                        sa.ForeignKey("collection.id",
                                                                      name="fk_collection_file_collection_id"),
                                                        nullable=False),
                                              sa.Column('filename', sa.Text, nullable=False),
                                              sa.Column('url', sa.Text, nullable=False),
                                              sa.Column('warnings', JSONB(none_as_null=True), nullable=True),
                                              sa.Column('errors', JSONB(none_as_null=True), nullable=True),
                                              sa.UniqueConstraint('collection_id', 'filename',
                                                                  name='unique_collection_file_identifiers'),
                                              sa.Index('collection_file_collection_id_idx', 'collection_id'),
                                              )

        self.collection_file_item_table = sa.Table('collection_file_item', self.metadata,
                                                   sa.Column('id', sa.Integer, primary_key=True),
                                                   sa.Column(
                                                       'collection_file_id',
                                                       sa.Integer,
                                                       sa.ForeignKey(
                                                           "collection_file.id",
                                                           name="fk_collection_file_item_collection_file_id"
                                                       ),
                                                       nullable=False
                                                   ),
                                                   sa.Column('number', sa.Integer, nullable=False),
                                                   sa.Column('warnings', JSONB(none_as_null=True), nullable=True),
                                                   sa.Column('errors', JSONB(none_as_null=True), nullable=True),
                                                   sa.UniqueConstraint('collection_file_id', 'number',
                                                                       name='unique_collection_file_item_identifiers'),
                                                   sa.Index('collection_file_item_collection_file_id_idx',
                                                            'collection_file_id'),
                                                   )

        self.data_table = sa.Table('data', self.metadata,
                                   sa.Column('id', sa.Integer, primary_key=True),
                                   sa.Column('hash_md5', sa.Text, nullable=False),
                                   sa.Column('data', JSONB, nullable=False),
                                   sa.UniqueConstraint('hash_md5', name='unique_data_hash_md5'),
                                   )

        self.package_data_table = sa.Table('package_data', self.metadata,
                                           sa.Column('id', sa.Integer, primary_key=True),
                                           sa.Column('hash_md5', sa.Text, nullable=False),
                                           sa.Column('data', JSONB, nullable=False),
                                           sa.UniqueConstraint('hash_md5', name='unique_package_data_hash_md5'),
                                           )

        self.release_table = sa.Table('release', self.metadata,
                                      sa.Column('id', sa.Integer, primary_key=True),
                                      sa.Column('collection_id', sa.Integer, sa.ForeignKey('collection.id',
                                                name='fk_release_collection_id'), nullable=False),
                                      sa.Column('collection_file_item_id', sa.Integer,
                                                sa.ForeignKey("collection_file_item.id",
                                                              name="fk_release_collection_file_item_id"),
                                                nullable=False),
                                      sa.Column('release_id', sa.Text, nullable=False),
                                      sa.Column('ocid', sa.Text, nullable=False),
                                      sa.Column('data_id', sa.Integer,
                                                sa.ForeignKey("data.id", name="fk_release_data_id"), nullable=False),
                                      sa.Column('package_data_id', sa.Integer,
                                                sa.ForeignKey("package_data.id", name="fk_release_package_data_id"),
                                                nullable=False),
                                      sa.Index('release_collection_id_idx', 'collection_id'),
                                      sa.Index('release_collection_file_item_id_idx', 'collection_file_item_id'),
                                      sa.Index('release_ocid_idx', 'ocid'),
                                      sa.Index('release_package_data_id_idx', 'package_data_id'),
                                      )

        self.record_table = sa.Table('record', self.metadata,
                                     sa.Column('id', sa.Integer, primary_key=True),
                                     sa.Column('collection_id', sa.Integer,
                                               sa.ForeignKey(
                                                   'collection.id',
                                                   name='fk_record_collection_id'
                                               ),
                                               nullable=False),
                                     sa.Column(
                                         'collection_file_item_id',
                                         sa.Integer,
                                         sa.ForeignKey(
                                             "collection_file_item.id",
                                             name="fk_record_collection_file_item_id"
                                         ),
                                         nullable=False
                                     ),
                                     sa.Column('ocid', sa.Text, nullable=False),
                                     sa.Column('data_id', sa.Integer,
                                               sa.ForeignKey("data.id", name="fk_record_data_id"), nullable=False),
                                     sa.Column('package_data_id', sa.Integer,
                                               sa.ForeignKey("package_data.id", name="fk_record_package_data_id"),
                                               nullable=False),
                                     sa.Index('record_collection_id_idx', 'collection_id'),
                                     sa.Index('record_collection_file_item_id_idx', 'collection_file_item_id'),
                                     sa.Index('record_ocid_idx', 'ocid'),
                                     sa.Index('record_package_data_id_idx', 'package_data_id'),
                                     )

        self.compiled_release_table = sa.Table('compiled_release', self.metadata,
                                               sa.Column('id', sa.Integer, primary_key=True),
                                               sa.Column('collection_id', sa.Integer,
                                                         sa.ForeignKey(
                                                             'collection.id',
                                                             name='fk_compiled_release_collection_id'
                                                         ),
                                                         nullable=False),
                                               sa.Column(
                                                   'collection_file_item_id',
                                                   sa.Integer,
                                                   sa.ForeignKey("collection_file_item.id",
                                                                 name="fk_complied_release_collection_file_item_id"),
                                                   nullable=False
                                               ),
                                               sa.Column('ocid', sa.Text, nullable=False),
                                               sa.Column('data_id', sa.Integer,
                                                         sa.ForeignKey("data.id", name="fk_complied_release_data_id"),
                                                         nullable=False),
                                               sa.Index('compiled_release_collection_id_idx', 'collection_id'),
                                               sa.Index(
                                                   'compiled_release_collection_file_item_id_idx',
                                                   'collection_file_item_id'
                                               ),
                                               sa.Index('compiled_release_ocid_idx', 'ocid'),
                                               )

        self.release_check_table = sa.Table('release_check', self.metadata,
                                            sa.Column('id', sa.Integer, primary_key=True),
                                            sa.Column('release_id', sa.Integer,
                                                      sa.ForeignKey("release.id", name="fk_release_check_release_id"),
                                                      nullable=False),
                                            sa.Column('override_schema_version', sa.Text, nullable=False),
                                            sa.Column('cove_output', JSONB, nullable=False),
                                            sa.UniqueConstraint('release_id', 'override_schema_version',
                                                                name='unique_release_check_release_id_and_more'),
                                            sa.Index('release_check_release_id_idx', 'release_id'),
                                            )

        self.record_check_table = sa.Table('record_check', self.metadata,
                                           sa.Column('id', sa.Integer, primary_key=True),
                                           sa.Column('record_id', sa.Integer,
                                                     sa.ForeignKey("record.id", name="fk_record_check_record_id"),
                                                     nullable=False),
                                           sa.Column('override_schema_version', sa.Text, nullable=False),
                                           sa.Column('cove_output', JSONB, nullable=False),
                                           sa.UniqueConstraint('record_id', 'override_schema_version',
                                                               name='unique_record_check_record_id_and_more'),
                                           sa.Index('record_check_record_id_idx', 'record_id'),
                                           )

        self.release_check_error_table = sa.Table('release_check_error', self.metadata,
                                                  sa.Column('id', sa.Integer, primary_key=True),
                                                  sa.Column(
                                                      'release_id',
                                                      sa.Integer,
                                                      sa.ForeignKey("release.id",
                                                                    name="fk_release_check_error_release_id"),
                                                      nullable=False
                                                  ),
                                                  sa.Column('override_schema_version', sa.Text, nullable=False),
                                                  sa.Column('error', sa.Text, nullable=False),
                                                  sa.UniqueConstraint(
                                                      'release_id',
                                                      'override_schema_version',
                                                      name='unique_release_check_error_release_id_and_more'),
                                                  sa.Index('release_check_error_release_id_idx', 'release_id'),
                                                  )

        self.record_check_error_table = sa.Table('record_check_error', self.metadata,
                                                 sa.Column('id', sa.Integer, primary_key=True),
                                                 sa.Column(
                                                     'record_id',
                                                     sa.Integer,
                                                     sa.ForeignKey("record.id",
                                                                   name="fk_record_check_error_record_id"),
                                                     nullable=False
                                                 ),
                                                 sa.Column('override_schema_version', sa.Text, nullable=False),
                                                 sa.Column('error', sa.Text, nullable=False),
                                                 sa.UniqueConstraint(
                                                     'record_id',
                                                     'override_schema_version',
                                                     name='unique_record_check_error_record_id_and_more'),
                                                 sa.Index('record_check_error_record_id_idx', 'record_id'),
                                                 )

        self.transform_upgrade_1_0_to_1_1_status_release_table = sa.Table(
            'transform_upgrade_1_0_to_1_1_status_release',
            self.metadata,
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

        self.transform_upgrade_1_0_to_1_1_status_record_table = sa.Table(
            'transform_upgrade_1_0_to_1_1_status_record',
            self.metadata,
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

    def get_engine(self):
        # We only create a connection if actually needed; sometimes people do operations that don't need a database
        # and in that case no need to connect.
        # But this side of kingfisher now always requires a DB, so there should not be a problem opening a connection!
        if not self._engine:
            self._engine = sa.create_engine(
                self.config.database_uri,
                json_serializer=SetEncoder().encode,
                json_deserializer=partial(
                    json.loads,
                    object_pairs_hook=collections.OrderedDict),
            )
        return self._engine

    def delete_tables(self):
        engine = self.get_engine()
        engine.execute("drop table if exists transform_upgrade_1_0_to_1_1_status_record cascade")
        engine.execute("drop table if exists transform_upgrade_1_0_to_1_1_status_release cascade")
        engine.execute("drop table if exists record_check cascade")
        engine.execute("drop table if exists record_check_error cascade")
        engine.execute("drop table if exists release_check cascade")
        engine.execute("drop table if exists release_check_error cascade")
        engine.execute("drop table if exists record cascade")
        engine.execute("drop table if exists release cascade")
        engine.execute("drop table if exists compiled_release cascade")
        engine.execute("drop table if exists package_data cascade")
        engine.execute("drop table if exists data cascade")
        engine.execute("drop table if exists collection_file_item")
        engine.execute("drop table if exists collection_file_status cascade")  # This is the old table name
        engine.execute("drop table if exists collection_file cascade")
        engine.execute("drop table if exists source_session_file_status cascade")  # This is the old table name
        engine.execute("drop table if exists collection_note cascade")
        engine.execute("drop table if exists collection cascade")
        engine.execute("drop table if exists source_session cascade")  # This is the old table name
        engine.execute("drop table if exists alembic_version cascade")

    def create_tables(self):
        # Note this DOES NOT work with self.config!
        # It works with a brand new config instance that is created in ocdskingfisher/maindatabase/migrations/env.py
        # Not sure how to solve that
        alembicargs = [
            '--config', os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'mainalembic.ini')),
            '--raiseerr',
            'upgrade', 'head',
        ]
        alembic.config.main(argv=alembicargs)

    def get_collection_id(self, source_id, data_version, sample,
                          transform_from_collection_id=None, transform_type=''):

        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_table]) \
                .where((self.collection_table.c.source_id == source_id) &
                       (self.collection_table.c.data_version == data_version) &
                       (self.collection_table.c.sample == sample) &
                       (self.collection_table.c.transform_from_collection_id == transform_from_collection_id) &
                       (self.collection_table.c.transform_type == transform_type))
            result = connection.execute(s)
            collection = result.fetchone()
            if collection:
                return collection['id']

    def get_or_create_collection_id(self, source_id, data_version, sample,
                                    transform_from_collection_id=None, transform_type=''):

        collection_id = self.get_collection_id(
            source_id,
            data_version,
            sample,
            transform_from_collection_id=transform_from_collection_id,
            transform_type=transform_type)
        if collection_id:
            return collection_id

        with self.get_engine().begin() as connection:
            value = connection.execute(self.collection_table.insert(), {
                'source_id': source_id,
                'data_version': data_version,
                'sample': sample,
                'transform_type': transform_type,
                'transform_from_collection_id': transform_from_collection_id,
                'store_start_at': datetime.datetime.utcnow(),
                'check_data': False,
                'check_older_data_with_schema_version_1_1': False,
            })
            collection_id = value.inserted_primary_key[0]

        KINGFISHER_SIGNALS.signal('new_collection_created').send('anonymous', collection_id=collection_id)
        return collection_id

    def get_all_collections(self):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_table]).order_by(self.collection_table.c.id.asc())
            return [
                CollectionModel(
                    database_id=collection['id'],
                    source_id=collection['source_id'],
                    data_version=collection['data_version'],
                    sample=collection['sample'],
                    transform_type=collection['transform_type'],
                    transform_from_collection_id=collection['transform_from_collection_id'],
                    check_data=collection['check_data'],
                    check_older_data_with_schema_version_1_1=collection['check_older_data_with_schema_version_1_1'],
                    store_start_at=collection['store_start_at'],
                    store_end_at=collection['store_end_at'],
                    deleted_at=collection['deleted_at'],
                ) for collection in connection.execute(s)
            ]

    def get_collections_that_transform_this_collection(self, collection_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_table]) \
                .where(self.collection_table.c.transform_from_collection_id == collection_id)

            return [
                CollectionModel(
                    database_id=collection['id'],
                    source_id=collection['source_id'],
                    data_version=collection['data_version'],
                    sample=collection['sample'],
                    transform_type=collection['transform_type'],
                    transform_from_collection_id=collection['transform_from_collection_id'],
                    check_data=collection['check_data'],
                    check_older_data_with_schema_version_1_1=collection['check_older_data_with_schema_version_1_1'],
                    store_start_at=collection['store_start_at'],
                    store_end_at=collection['store_end_at'],
                    deleted_at=collection['deleted_at'],
                ) for collection in connection.execute(s)
            ]

    def get_collection(self, collection_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_table]) \
                .where(self.collection_table.c.id == collection_id)
            result = connection.execute(s)
            collection = result.fetchone()
            if collection:
                return CollectionModel(
                    database_id=collection['id'],
                    source_id=collection['source_id'],
                    data_version=collection['data_version'],
                    sample=collection['sample'],
                    transform_type=collection['transform_type'],
                    transform_from_collection_id=collection['transform_from_collection_id'],
                    check_data=collection['check_data'],
                    check_older_data_with_schema_version_1_1=collection['check_older_data_with_schema_version_1_1'],
                    store_start_at=collection['store_start_at'],
                    store_end_at=collection['store_end_at'],
                    deleted_at=collection['deleted_at'],
                )

    def get_all_notes_in_collection(self, collection_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_note_table]) \
                .where(self.collection_note_table.c.collection_id == collection_id) \
                .order_by(self.collection_note_table.c.stored_at.asc())
            return [
                CollectionNoteModel(
                    database_id=collection_note['id'],
                    note=collection_note['note'],
                    stored_at=collection_note['stored_at'],
                ) for collection_note in connection.execute(s)
            ]

    def get_all_files_in_collection(self, collection_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_file_table]) \
                .where(self.collection_file_table.c.collection_id == collection_id) \
                .order_by(self.collection_file_table.c.id.asc())
            return [
                FileModel(
                    database_id=collection_file['id'],
                    filename=collection_file['filename'],
                    url=collection_file['url'],
                    warnings=collection_file['warnings'],
                    errors=collection_file['errors'],
                ) for collection_file in connection.execute(s)
            ]

    def get_all_files_items_in_file(self, file):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_file_item_table]) \
                .where(self.collection_file_item_table.c.collection_file_id == file.database_id) \
                .order_by(self.collection_file_item_table.c.number.asc())
            return [
                FileItemModel(
                    database_id=result['id'],
                    number=result['number'],
                    errors=result['errors'],
                    warnings=result['warnings'],
                ) for result in connection.execute(s)
            ]

    def is_release_check_done(self, release_id, override_schema_version=''):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.release_check_table.c.id]) \
                .where((self.release_check_table.c.release_id == release_id) &
                       (self.release_check_table.c.override_schema_version == override_schema_version))
            result = connection.execute(s)
            if result.fetchone():
                return True

            s = sa.sql.select([self.release_check_error_table.c.id]) \
                .where((self.release_check_error_table.c.release_id == release_id) &
                       (self.release_check_error_table.c.override_schema_version == override_schema_version))
            result = connection.execute(s)
            if result.fetchone():
                return True

        return False

    def is_record_check_done(self, record_id, override_schema_version=''):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.record_check_table.c.id]) \
                .where((self.record_check_table.c.record_id == record_id) &
                       (self.record_check_table.c.override_schema_version == override_schema_version))
            result = connection.execute(s)
            if result.fetchone():
                return True

            s = sa.sql.select([self.record_check_error_table.c.id]) \
                .where((self.record_check_error_table.c.record_id == record_id) &
                       (self.record_check_error_table.c.override_schema_version == override_schema_version))
            result = connection.execute(s)
            if result.fetchone():
                return True

        return False

    def mark_collection_file_store_done(self, collection_id, filename, warnings=None):
        with self.get_engine().begin() as connection:
            connection.execute(
                self.collection_file_table.update()
                    .where((self.collection_file_table.c.collection_id == collection_id) &
                           (self.collection_file_table.c.filename == filename))
                    .values(warnings=warnings if warnings and len(warnings) > 0 else None,
                            )
            )

    def get_package_data(self, package_data_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.package_data_table]) \
                .where(self.package_data_table.c.id == package_data_id)
            result = connection.execute(s)
            data_row = result.fetchone()
            return data_row['data']

    def get_data(self, data_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.data_table]) \
                .where(self.data_table.c.id == data_id)
            result = connection.execute(s)
            data_row = result.fetchone()
            return data_row['data']

    def mark_collection_store_done(self, collection_id):
        with self.get_engine().begin() as connection:
            connection.execute(
                self.collection_table.update()
                    .where(
                        (self.collection_table.c.id == collection_id) & self.collection_table.c.store_end_at.is_(None)
                    ).values(store_end_at=datetime.datetime.utcnow())
            )

        KINGFISHER_SIGNALS.signal('collection-store-finished').send('anonymous', collection_id=collection_id)
        return collection_id

    def store_collection_file_errors(self, collection_id, file_name, url, errors):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_file_table]) \
                .where((self.collection_file_table.c.collection_id == collection_id) &
                       (self.collection_file_table.c.filename == file_name))
            result = connection.execute(s)

            collection_file_table_row = result.fetchone()

            if collection_file_table_row:
                return

            connection.execute(self.collection_file_table.insert(), {
                'collection_id': collection_id,
                'filename': file_name,
                'url': url,
                'errors': errors,
            })

    def store_collection_file_item_errors(self, collection_id, file_name, number, url, errors):
        with self.get_engine().begin() as connection:

            # Collection File Table
            s = sa.sql.select([self.collection_file_table]) \
                .where((self.collection_file_table.c.collection_id == collection_id) &
                       (self.collection_file_table.c.filename == file_name))
            result = connection.execute(s)

            collection_file_table_row = result.fetchone()

            if collection_file_table_row:
                collection_file_id = collection_file_table_row['id']
            else:
                value = connection.execute(self.collection_file_table.insert(), {
                    'collection_id': collection_id,
                    'filename': file_name,
                    'url': url,
                })

                collection_file_id = value.inserted_primary_key[0]

            # Collection File Item
            s = sa.sql.select([self.collection_file_item_table]) \
                .where((self.collection_file_item_table.c.collection_file_id == collection_file_id) &
                       (self.collection_file_item_table.c.number == number))
            result = connection.execute(s)

            collection_file_item_table_row = result.fetchone()

            if collection_file_item_table_row:
                raise Exception('That Number is already used!')
            else:
                connection.execute(self.collection_file_item_table.insert(), {
                    'collection_file_id': collection_file_id,
                    'number': number,
                    'errors': errors,
                })

    def can_mark_collection_deleted(self, collection_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_table]) \
                .where(self.collection_table.c.transform_from_collection_id == collection_id)

            result = connection.execute(s)
            destination_collection = result.fetchone()
            if destination_collection:
                # This collection is the source collection for something else!
                # We can't delete it if that transform is still active!
                if not destination_collection['store_end_at'] and not destination_collection['deleted_at']:
                    return False

        return True

    def mark_collection_deleted_at(self, collection_id):
        with self.get_engine().begin() as connection:
            connection.execute(
                self.collection_table.update()
                    .where(
                        (self.collection_table.c.id == collection_id) & self.collection_table.c.deleted_at.is_(None)
                    ).values(deleted_at=datetime.datetime.utcnow())
            )

    def delete_collection(self, collection_id):
        self._delete_collection_run_sql("release_check_error", """
            DELETE FROM release_check_error
            WHERE release_id IN (
                SELECT id FROM release
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("record_check_error", """
            DELETE FROM record_check_error
            WHERE record_id IN (
                SELECT id FROM record
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("record_check", """
            DELETE FROM record_check
            WHERE record_id IN (
                SELECT id FROM record
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("release_check", """
            DELETE FROM release_check
            WHERE release_id IN (
                SELECT id FROM release
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("compiled_release", """
            DELETE FROM compiled_release
            WHERE id IN (
                SELECT id FROM compiled_release
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("transform_upgrade_1_0_to_1_1_status_record", """
            DELETE FROM transform_upgrade_1_0_to_1_1_status_record
            WHERE source_record_id IN (
                SELECT id FROM record
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("record", """
            DELETE FROM record
            WHERE id IN (
                SELECT id FROM record
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("transform_upgrade_1_0_to_1_1_status_release", """
            DELETE FROM transform_upgrade_1_0_to_1_1_status_release
            WHERE source_release_id IN (
                SELECT id FROM release
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql("release", """
            DELETE FROM release
            WHERE id IN (
                SELECT id FROM release
                WHERE collection_id = :collection_id
            );""", collection_id)
        self._delete_collection_run_sql_in_blocks(
            "collection_file_item",
            """
                SELECT collection_file_item.id
                FROM collection_file_item
                JOIN collection_file ON collection_file_item.collection_file_id = collection_file.id
                WHERE collection_file.collection_id = :collection_id
                LIMIT 10000
            """,
            collection_id,
            "collection_file_item"
        )
        self._delete_collection_run_sql(
            "collection_file", "DELETE FROM collection_file WHERE collection_id = :collection_id;", collection_id)
        self._delete_collection_run_sql(
            "collection_note", "DELETE FROM collection_note WHERE collection_id = :collection_id;", collection_id)
        self._delete_collection_run_sql(
            "collection",
            """
                UPDATE collection
                SET transform_from_collection_id = NULL
                WHERE transform_from_collection_id = :collection_id;
            """,
            collection_id)
        self._delete_collection_run_sql(
            "collection", "DELETE FROM collection WHERE id = :collection_id;", collection_id)

    def _delete_collection_run_sql(self, label, sql, collection_id):
        logger = logging.getLogger('ocdskingfisher.database.delete-collection')
        logger.debug("Deleting " + label + " for collection " + str(collection_id))
        data = {'collection_id': collection_id}
        # We execute every SQL statement in it's own transaction, to try and keep the size of the transactions small
        # It doesn't matter if we re-do a delete, so it doesn't matter if there is a problem half way through!
        with self.get_engine().begin() as connection:
            connection.execute(sa.sql.expression.text(sql), data)

    def _delete_collection_run_sql_in_blocks(self, label, select_sql, collection_id, delete_from_table):
        logger = logging.getLogger('ocdskingfisher.database.delete-collection')
        logger.debug("Deleting " + label + " for collection " + str(collection_id))
        data_get = {'collection_id': collection_id}
        while True:
            with self.get_engine().begin() as connection:
                ids_to_delete = [str(row['id'])
                                 for row in connection.execute(sa.sql.expression.text(select_sql), data_get)]
                if len(ids_to_delete) == 0:
                    return
                connection.execute(
                    sa.sql.expression.text(
                        "DELETE FROM " + delete_from_table + " WHERE id IN (" + ",".join(ids_to_delete) + ")"),
                    {}
                )

    def delete_orphan_data(self):
        self._delete_orphan_data_data()
        self._delete_orphan_data_package_data()

    def _delete_orphan_data_data(self):
        data_get = {}
        sql_get = """
            SELECT data.id
            FROM data
            LEFT JOIN release ON release.data_id = data.id
            LEFT JOIN record ON record.data_id = data.id
            LEFT JOIN compiled_release ON compiled_release.data_id = data.id
            WHERE release.data_id IS NULL AND record.data_id IS NULL AND compiled_release.data_id IS NULL
            LIMIT 10000;
        """
        logger = logging.getLogger('ocdskingfisher.database.delete-collection')
        logger.debug("Deleting data")
        while True:
            with self.get_engine().begin() as connection:
                ids_to_delete = [str(row['id'])
                                 for row in connection.execute(sa.sql.expression.text(sql_get), data_get)]
                if len(ids_to_delete) == 0:
                    return
                connection.execute(
                    sa.sql.expression.text("DELETE FROM data WHERE id IN (" + ",".join(ids_to_delete) + ")"),
                    {}
                )

    def _delete_orphan_data_package_data(self):
        sql_get = """
            SELECT package_data.id
            FROM package_data
            LEFT JOIN release ON release.package_data_id = package_data.id
            LEFT JOIN record ON record.package_data_id = package_data.id
            WHERE release.package_data_id IS NULL AND record.package_data_id IS NULL
            LIMIT 10000;
        """
        logger = logging.getLogger('ocdskingfisher.database.delete-collection')
        logger.debug("Deleting package_data")
        while True:
            with self.get_engine().begin() as connection:
                ids_to_delete = [row['id'] for row in connection.execute(sa.sql.text(sql_get))]
                if not ids_to_delete:
                    return
                connection.execute(
                    sa.sql.text("DELETE FROM package_data WHERE id IN :ids"),
                    ids=tuple(ids_to_delete)
                )

    def _get_check_query(self, obj_type, collection_id, override_schema_version):
        data = {'collection_id': collection_id}
        sql = """
            SELECT release.id, release.data_id, release.package_data_id
            FROM release
        """
        if override_schema_version:
            sql += """
                LEFT JOIN package_data ON package_data.id = release.package_data_id
                WHERE release.collection_id = :collection_id
                    AND NOT EXISTS (
                        SELECT FROM release_check
                        WHERE release_id = release.id AND override_schema_version = :override_schema_version
                    )
                    AND NOT EXISTS (
                        SELECT FROM release_check_error
                        WHERE release_id = release.id AND override_schema_version = :override_schema_version
                    )
                    AND coalesce(data ->> 'version', '1.0') <> :override_schema_version;
            """
            data['override_schema_version'] = override_schema_version
        else:
            sql += """
                WHERE release.collection_id = :collection_id
                    AND NOT EXISTS (
                        SELECT FROM release_check
                        WHERE release_id = release.id AND override_schema_version = ''
                    )
                    AND NOT EXISTS (
                        SELECT FROM release_check_error
                        WHERE release_id = release.id AND override_schema_version = ''
                    );
            """

        return sql.replace('release', obj_type), data

    def get_releases_to_check(self, collection_id, override_schema_version=''):
        sql, data = self._get_check_query('release', collection_id, override_schema_version)

        with self.get_engine().begin() as connection:
            query = sa.sql.expression.text(sql)
            return connection.execute(query, data)

    def get_records_to_check(self, collection_id, override_schema_version=''):
        sql, data = self._get_check_query('record', collection_id, override_schema_version)

        with self.get_engine().begin() as connection:
            query = sa.sql.expression.text(sql)
            return connection.execute(query, data)

    def add_collection_note(self, collection_id, note):
        with self.get_engine().begin() as connection:
            s = sa.sql.select([self.collection_note_table]) \
                .where((self.collection_note_table.c.collection_id == collection_id) &
                       (self.collection_note_table.c.note == note))
            result = connection.execute(s)

            collection_note_table_row = result.fetchone()
            if not collection_note_table_row:
                connection.execute(self.collection_note_table.insert(), {
                    'collection_id': collection_id,
                    'note': note,
                    'stored_at': datetime.datetime.utcnow(),
                })

    def mark_collection_check_data(self, collection_id, value):
        with self.get_engine().begin() as connection:
            connection.execute(
                self.collection_table.update()
                    .where(self.collection_table.c.id == collection_id)
                    .values(check_data=value)
            )

    def mark_collection_check_older_data_with_schema_version_1_1(self, collection_id, value):
        with self.get_engine().begin() as connection:
            connection.execute(
                self.collection_table.update()
                    .where(self.collection_table.c.id == collection_id)
                    .values(check_older_data_with_schema_version_1_1=value)
            )

    def update_collection_cached_columns(self, collection_id):
        with self.get_engine().begin() as connection:
            s = sa.sql.expression.text(
                "SELECT count(*) as release_count FROM release WHERE collection_id = :collection_id")
            result = connection.execute(s, {"collection_id": collection_id})
            data = result.fetchone()

            connection.execute(
                self.collection_table.update()
                    .where(self.collection_table.c.id == collection_id)
                    .values(cached_releases_count=data['release_count'])
            )

        with self.get_engine().begin() as connection:
            s = sa.sql.expression.text(
                "SELECT count(*) as record_count FROM record WHERE collection_id = :collection_id")
            result = connection.execute(s, {"collection_id": collection_id})
            data = result.fetchone()

            connection.execute(
                self.collection_table.update()
                    .where(self.collection_table.c.id == collection_id)
                    .values(cached_records_count=data['record_count'])
            )

        with self.get_engine().begin() as connection:
            s = sa.sql.expression.text(
                "SELECT count(*) as compiled_release_count FROM compiled_release " +
                "WHERE collection_id = :collection_id")
            result = connection.execute(s, {"collection_id": collection_id})
            data = result.fetchone()

            connection.execute(
                self.collection_table.update()
                    .where(self.collection_table.c.id == collection_id)
                    .values(cached_compiled_releases_count=data['compiled_release_count'])
            )


class DatabaseStore:

    def __init__(self, database, collection_id, file_name, number, url='',
                 allow_existing_collection_file_item_table_row=False, warnings=None):
        self.database = database
        self.collection_id = collection_id
        self.collection = None
        self.file_name = file_name
        self.url = url
        self.number = number
        self.connection = None
        self.transaction = None
        self.collection_file_id = None
        self.collection_file_item_id = None
        self.allow_existing_collection_file_item_table_row = allow_existing_collection_file_item_table_row
        self.warnings = warnings

    def __enter__(self):
        self.connection = self.database.get_engine().connect()
        self.transaction = self.connection.begin()

        self.collection = self.database.get_collection(self.collection_id)

        # Collection File!
        s = sa.sql.select([self.database.collection_file_table]) \
            .where((self.database.collection_file_table.c.collection_id == self.collection_id) &
                   (self.database.collection_file_table.c.filename == self.file_name))
        result = self.connection.execute(s)

        collection_file_table_row = result.fetchone()

        if collection_file_table_row:
            self.collection_file_id = collection_file_table_row['id']
        else:
            value = self.connection.execute(self.database.collection_file_table.insert(), {
                'collection_id': self.collection_id,
                'filename': self.file_name,
                'url': self.url,
            })
            self.collection_file_id = value.inserted_primary_key[0]

        # Collection File Item!
        s = sa.sql.select([self.database.collection_file_item_table]) \
            .where((self.database.collection_file_item_table.c.collection_file_id == self.collection_file_id) &
                   (self.database.collection_file_item_table.c.number == self.number))
        result = self.connection.execute(s)

        collection_file_item_table_row = result.fetchone()

        if collection_file_item_table_row:
            self.collection_file_item_id = collection_file_item_table_row['id']
            if not self.allow_existing_collection_file_item_table_row:
                raise DuplicateFileItemRowError(
                    "DatabaseStore class tried to insert a duplicate collection_file_item row! "
                    "collection_file_id = {} number = {} existing row id = {}" .format(
                        self.collection_file_id, self.number, self.collection_file_item_id))
        else:
            value = self.connection.execute(self.database.collection_file_item_table.insert(), {
                'collection_file_id': self.collection_file_id,
                'number': self.number,
                'warnings': (self.warnings if isinstance(self.warnings, list) and len(self.warnings) > 0 else None),
            })
            self.collection_file_item_id = value.inserted_primary_key[0]

        # DB queries that will be used repeatably, we pre-build and reuse for speed
        self.database_get_existing_data = sa.sql.expression.text("""
            WITH ins AS (
                INSERT INTO data (hash_md5, data)
                VALUES (:hash_md5, :data)
                ON CONFLICT(hash_md5) DO NOTHING
                RETURNING id
            )
            SELECT COALESCE(
                (SELECT id FROM ins),
                (SELECT id FROM data WHERE hash_md5 = :hash_md5)
            ) AS id
        """)
        self.database_get_existing_package_data = sa.sql.expression.text("""
            WITH ins AS (
                INSERT INTO package_data (hash_md5, data)
                VALUES (:hash_md5, :data)
                ON CONFLICT(hash_md5) DO NOTHING
                RETURNING id
            )
            SELECT COALESCE(
                (SELECT id FROM ins),
                (SELECT id FROM package_data WHERE hash_md5 = :hash_md5)
            ) AS id
        """)

        return self

    def __exit__(self, type, value, traceback):
        if type:
            self.transaction.rollback()
            self.connection.close()
        else:
            self.transaction.commit()
            self.connection.close()

            # XXX The code otherwise uselessly sends messages that do no work.
            if self.collection.check_data:
                KINGFISHER_SIGNALS.signal('collection-data-store-finished').send(
                    'anonymous',
                    collection_id=self.collection_id,
                    collection_file_item_id=self.collection_file_item_id
                )

    def insert_record(self, row, package_data):
        ocid = row.get('ocid', '')
        package_data_id = self.get_id_for_package_data(package_data)
        data_id = self.get_id_for_data(row)
        self.connection.execute(self.database.record_table.insert(), {
            'collection_id': self.collection_id,
            'collection_file_item_id': self.collection_file_item_id,
            'ocid': ocid,
            'data_id': data_id,
            'package_data_id': package_data_id,
        })

    def insert_release(self, row, package_data):
        ocid = row.get('ocid', '')
        release_id = row.get('id', '')
        package_data_id = self.get_id_for_package_data(package_data)
        data_id = self.get_id_for_data(row)
        self.connection.execute(self.database.release_table.insert(), {
            'collection_id': self.collection_id,
            'collection_file_item_id': self.collection_file_item_id,
            'release_id': release_id,
            'ocid': ocid,
            'data_id': data_id,
            'package_data_id': package_data_id,
        })

    def insert_compiled_release(self, row):
        ocid = row.get('ocid', '')
        data_id = self.get_id_for_data(row)
        self.connection.execute(self.database.compiled_release_table.insert(), {
            'collection_id': self.collection_id,
            'collection_file_item_id': self.collection_file_item_id,
            'ocid': ocid,
            'data_id': data_id,
        })

    def get_id_for_package_data(self, data):
        hash_md5 = get_hash_md5_for_data(data)
        result = self.connection.execute(self.database_get_existing_package_data, {
            'hash_md5': hash_md5,
            'data': json.dumps(data),
        })
        return result.fetchone().id

    def get_id_for_data(self, data):
        hash_md5 = get_hash_md5_for_data(data)
        result = self.connection.execute(self.database_get_existing_data, {
            'hash_md5': hash_md5,
            'data': json.dumps(data),
        })
        return result.fetchone().id
