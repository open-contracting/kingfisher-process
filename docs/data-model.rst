Data Model
==========

Collections
-----------

Collections are a set of data that are handled separately.

A collection is defined uniquely by a combination of all the 5 variables listed below.

* Name. A String. Can be anything you want.
* Date. The date the collection started.
* Sample. A Boolean flag.

Collections have a set of flags on them that describe what operations to do on them. These are:

* `check_data`. Should data in this collection be checked?
* `check_older_data_with_schema_version_1_1`. If the data is less than schema version 1.1, then should it be also checked with the version forced to 1.1?

Default values for these can be configured - see :doc:`config`.

A collection is also given a numeric ID, which for convenience is normally used to refer to the collection.

Files
-----

Each collection contains one or more files.

Each file is uniquely identified in a collection by it's file name.

Files can have `warnings` and/or `errors`:

*  `errors` indicate the file could not be processed at all for some reason. In this case, you should not expect to find any data in this file.
*  `warnings` indicate the file could still be processed. In this case, you should still find some data in this file.

Data Types for Files
--------------------

When giving file to this software to load, you must specify a data type. This can be:

*  record - the file is a record.
*  release - the file is a release.
*  record_package - the file is a record package.
*  release_package - the file is a release package.
*  record_package_json_lines - the file is JSON lines, and every line is a record package
*  release_package_json_lines - see last entry, but release packages.
*  record_package_list - the file is a list of record packages. eg [  { record-package-1 } , { record-package-2 } ]
*  release_package_list - see last entry, but release packages.
*  record_list - the file is a list of records. eg [  { record-1 } , { record-2 } ]
*  release_list - see last entry, but releases.
*  record_package_list_in_results - the file is a list of record packages in the results attribute. eg { 'results': [  { record-package-1 } , { record-package-2 } ]  }
*  release_package_list_in_results - see last entry, but release packages.

Items
-----

Each File contains one or more items, where an item as a piece of OCDS data - a release, record, release package or record-package.

Some files only contain one item, and in that case there will only be one item per file.

Some files contain many items. For example;

* JSON Lines files
* A file downloaded from an API where the file is a JSON object that contains a list of records. eg http://www.contratosabiertos.cdmx.gob.mx/api/contratos/array

Each items has an integer number, which lists the order they appear in.

Each item is uniquely identified in a file by it's number.
