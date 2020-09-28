Looking for Collection Errors
=============================

If you have loaded a collection in the database, you will want to check to see if any errors were encountered. This page describes how to do that.

Note this is errors processing the data - it may be that we managed to process the data fine, but in doing so we realised the data fails a check. Such a check failure would be stored normally and we would carry on processing. This page is about errors that mean we can not process data normally for any reason.

This page can be used for original source collections and collections that hold the results of a transform - there may have been errors while processing the transform.

Notes
-----

Notes may contain errors for a collection.

.. code-block:: shell-session

    SELECT * FROM collection_note WHERE collection_id =3;

Files & File Items
------------------

Both `collection_file` and `collection_file_item` tables have an `errors` and a `warnings` column that may store details of errors encountered.

.. code-block:: shell-session

    SELECT * FROM collection_file WHERE collection_id =3 AND (errors IS NOT NULL OR warnings IS NOT NULL);
    SELECT * FROM collection_file_item JOIN collection_file ON collection_file_item.collection_file_id = collection_file.id
       WHERE collection_file.collection_id = 3 AND
       (collection_file_item.errors IS NOT NULL OR collection_file_item.warnings IS NOT NULL );

Errors with checks
------------------

A collection will be checked. Any errors encountered while checking will be stored in the `release_check_error` or `record_check_error` tables.

You can check for problems here with:

.. code-block:: shell-session

    SELECT release.id AS release_id, release_check_error.error FROM release_check_error JOIN release ON release_check_error.release_id = release.id
       WHERE release.collection_id=3;
    SELECT record.id AS record_id, record_check_error.error FROM record_check_error JOIN record ON record_check_error.record_id = record.id
       WHERE record.collection_id=3;

