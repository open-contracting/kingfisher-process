Command line tool - local-load option
===========================================

This command loads files from disk into an existing collection in the system.

You need to create a collection to load the data into - see :doc:`cli-new-collection`.

.. code-block:: shell-session

    python ocdskingfisher-process-cli local-load 1 /data/moldova release_package

- Pass the ID of the collection you want to load the data into. Use :doc:`cli-list-collections` to look up the ID you want.
- Pass the directory you want to load files from.
- Pass the type of the files. For possible options, see data types for files in :doc:`data-model`


It will load files with a default encoding of `utf-8`. You can change it with the option `--encoding`.

.. code-block:: shell-session

    python ocdskingfisher-process-cli local-load 2 /data/uk_contracts_finder release_package --encoding ISO-8859-1

After loading, you should mark the collection storage as ended - see :doc:`cli-end-collection-store`.