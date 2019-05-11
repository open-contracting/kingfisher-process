Data Model
==========

.. _collections:

Collections
-----------

Collections are distinct sets of OCDS data. They are the largest unit on which this tool operates.

A collection is uniquely identified by the combination of:

* Name (``source_id``): A string. If the collection was created by Kingfisher Scrape, this is the ``name`` attribute of the `spider <https://github.com/open-contracting/kingfisher-scrape/tree/master/kingfisher_scrapy/spiders>`__.
* Date (``data_version``): The date and time at which the collection was created. If the collection was created by Kingfisher Scrape, this is the ``start_time`` `statistic <https://docs.scrapy.org/en/latest/topics/stats.html>`__ of the crawl.
* Sample (``sample``): A boolean. Whether the collection is only a sample of the data from the source.
* Base collection (``transform_from_collection_id``): An integer. The ID of the collection that was transformed into this collection.
* Transform type (``transform_type``): A string. The identifier of the transformer that was used to produce this collection.

Each collection is given an integer ID; this is used to refer to the collection in the :doc:`cli/index` and the database.

Collections are created by Kingfisher Scrape, the :ref:`web API <web-api>`, or the :doc:`cli/new-collection` command.

.. _schema-check-flags:

Schema check flags
~~~~~~~~~~~~~~~~~~

Collections have flags that indicate what operations to perform on them. These are:

check_data
    Run `CoVE <https://github.com/OpenDataServices/cove>`__ schema checks on the data in this collection

check_older_data_with_schema_version_1_1
    Force OCDS 1.1 checks to be run on OCDS 1.0 data (instead of OCDS 1.0 checks)

To configure the default values for these flags, see :doc:`config`.

.. _transformed-collections:

Transformed collections
~~~~~~~~~~~~~~~~~~~~~~~

Presently, the tool offers two transformers:

upgrade-1-0-to-1-1
    upgrade a collection's data from OCDS 1.0 to OCDS 1.1

compile-releases
    merge a collection's releases into compiled releases

To transform a collection, create a new collection that refers to the base collection, with either the :doc:`cli/new-transform-compile-releases` or :doc:`cli/new-transform-upgrade-1-0-to-1-1` command, then run the :doc:`cli/transform-collection` command.

Files
-----

A collection contains one or more files. A file is uniquely identified by its collection and filename. Files can have:

errors
    The file could not be retrieved. Presently, errors are either reported by Kingfisher Scrape or caught by the :doc:`cli/local-load` command.

warnings
    The file contents had to be modified in order to be stored. Presently, the only warning is about the removal of control characters.

File types
~~~~~~~~~~

The :doc:`cli/local-load` command must be given the type of the file to load:

record
    A single record

release
    A single release

record_list
    A JSON array of records, like ``[ { record-1 }, { record-2 } ]``

release_list
    A JSON array of releases

record_package
    A single record package

release_package
    A single release package

record_package_list
    A JSON array of record packages, like ``[ { record-package-1 }, { record-package-2 } ]``

release_package_list
    A JSON array of release packages

record_package_json_lines
    `Line-delimited JSON <https://en.wikipedia.org/wiki/JSON_streaming>`__, in which each line is a record package

release_package_json_lines
    As above, but release packages

record_package_list_in_results
    A JSON object with a ``results`` key whose value is a JSON array of record packages, like ``{ "results": [ { record-package-1 }, { record-package-2 } ] }``

release_package_list_in_results
    As above, but release packages


Items
-----

A file contains one or more items. An item is an OCDS resource: a release, record, release package or record package. An item is uniquely identified by its index within the file. Indices are ``0``-based.

Files of the type ``record``, ``release``, ``record_package``, or ``release_package`` have one item only. Files of other types have one or more items.
