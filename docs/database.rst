Database tables reference
=========================

.. image:: _static/relationships.real.large.png
   :target: _static/relationships.real.large.png

..
   To update the diagram, see https://ocp-software-handbook.readthedocs.io/en/latest/services/postgresql.html#generate-entity-relationship-diagram
   java -jar schemaspy.jar -t pgsql -dp postgresql.jar -host localhost -db kingfisher_process -o schemaspy -norows -I '(django|auth).*'

Tables
------

collection
  A collection, like the files written to the crawl directory by Kingfisher Collect
collection_file
  A file containing a record package or release package, like from Kingfisher Collect
collection_file_item
  A passthrough table
collection_note
  A collection note

  All warnings and handled errors are logged as notes. Use the ``code`` column to filter by severity.
package_data
  The metadata for a record package or release package

  The metadata is stored separately, to only store one copy of the same metadata within the same collection or across different collections.
data
  The data for a record, release or compiled release

  The data is stored separately, to only store one copy of the same data across different collections of the same publication.
record
  A record, from a record package
release
  A release, from a release package
compiled_release
  A compiled release
record_check
  A record's check results
release_check
  A release's check results
processing_step
  A temporary row to track incomplete operations (load, compile, check) on collection files

The format of the ``cove_output`` column of the ``*_check`` tables is described in the `lib-cove-ocds documentation <https://github.com/open-contracting/lib-cove-ocds?tab=readme-ov-file#output-json-format>`__ (also used by the `OCDS Data Review Tool <https://review.standard.open-contracting.org>`__), without:

-  ``additional_checks``
-  ``records_aggregates``
-  ``releases_aggregates``

collection
~~~~~~~~~~

source_id
  The source from which the files were retrieved, like the spider name from Kingfisher Collect
data_version
  The time at which the files were retrieved, like the ``start_time`` statistic from Kingfisher Collect
sample
  Whether the files represent a sample from the source
transform_type
  One of "compile-releases" or "upgrade-1-0-to-1-1"
transform_from_collection_id
  The parent collection from which this collection is derived
options
  A JSON object for the `routing slip <https://www.enterpriseintegrationpatterns.com/patterns/messaging/RoutingTable.html>`__ pattern
steps
  A JSON array with one or more of "upgrade", "compile", "check"
data_type
  A JSON object like ``{"format": "release package", "concatenated": false, "array": false}`` (see OCDS Kit's `detect_format() <https://ocdskit.readthedocs.io/en/latest/api/util.html#ocdskit.util.detect_format>`__ function)
scrapyd_job
  The ID of the job in Scrapyd (for example, to find the crawl log)
expected_files_count
  The number of messages to expect from Kingfisher Collect
store_start_at
  The time at which the collection was added
store_end_at
  The time at which the collection was closed
compilation_started
  Whether compilation has started
completed_at
  The time at which processing completed
deleted_at
  The time at which the collection was deleted
cached_releases_count
  The number of rows in the ``release`` table for this collection, once completed
cached_records_count
  The number of rows in the ``record`` table for this collection, once completed
cached_compiled_releases_count
  The number of rows in the ``compiled_release`` table for this collection, once completed
