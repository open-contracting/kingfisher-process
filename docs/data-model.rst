Data Model
==========

Collections
-----------

Collections are a set of data that are handled separately.

A collection is defined uniquely by a combination of all the variables listed below.

* Name. A String. Can be anything you want.
* Date. The date the collection started.
* Sample. A Boolean flag.

A collection is also given a numeric ID.

Files
-----

Each collection contains one or more files.

Each file is uniquely identified in a collection by it's file name.

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
