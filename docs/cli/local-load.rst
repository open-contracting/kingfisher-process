local-load
==========

This command loads files from disk into an existing collection in the system.

You need to create a collection to load the data into - see :doc:`new-collection`.

.. code-block:: shell-session

    python ocdskingfisher-process-cli local-load 1 /data/moldova release_package

- Pass the ID of the collection you want to load the data into. Use :doc:`list-collections` to look up the ID you want.
- Pass the directory you want to load files from.
- Pass the type of the files. For possible options, see data types for files in :doc:`../data-model`


It will load files with a default encoding of `utf-8`. You can change it with the option `--encoding`.

.. code-block:: shell-session

    python ocdskingfisher-process-cli local-load 2 /data/uk_contracts_finder release_package --encoding ISO-8859-1

By default, afterwards the collection store will be marked as ended.
If you want to leave it open (eg. so you can load more files) use the optional flag `--keep-collection-store-open`:

.. code-block:: shell-session

    python ocdskingfisher-process-cli local-load --keep-collection-store-open 1 /data/moldova release_package

If you want to manually end the store see :doc:`end-collection-store`.
