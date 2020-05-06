Compile Release Transform
=========================

This transform takes records and releases from the source collection and attempts to put compiled releases in the destination collection.

Process
-------

This describes the process the transform will use to look for or compile data.

First it will load all OCID's in the source collection, in records or releases. It will than attempt to process each OCID separately.

For an OCID, if there is a record it will follow the process to extract information out of the record. (If there is more than one record for an OCID, it will pick one at random and log it has done this).

It will attempt the following things in order, and use the first one that is successful.

* If there are releases without a date field, it will log this.
* If there are releases with a date field, and none of them are linked releases, it will compile a release itself.
* If there is a compiled release in the record, that will just be used and this will be logged.
* If there is a release tagged ``compiled``, it will be used and this will be logged. (If there is more than one, one will be picked at random and this will be logged.)
* If we get this far, we can't process the OCID. That will be logged.

For an OCID, if there are no records it will instead look to releases.

It will attempt the following things in order, and use the first one that is successful.

* If there is a release tagged ``compiled``, it will be used and this will be logged. (If there is more than one, one will be picked at random and this will be logged.)
* If there are releases without a date field, it will log this.
* If there are releases with a date field, it will compile a release itself.
* If we get this far, we can't process the OCID. That will be logged.


Checking for logs
-----------------

There are several places to check for logs from the transform.

* In the `collection_note` table
* In the `warnings` column on the `collection_file_item` table

These will be linked to the destination collection and when looking you should filter by that.