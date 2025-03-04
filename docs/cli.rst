Command-line interface
======================

.. code-block:: bash

   ./manage.py --help

Commands
--------

.. _cli-load:

load
~~~~

Load data into a collection, asynchronously.

.. code-block:: bash

   ./manage.py load [OPTIONS] PATH [PATH ...]

-s SOURCE, --source SOURCE
                      the source from which the files were retrieved (append '_local' if not sourced from Scrapy)
-t TIME, --time TIME  the time at which the files were retrieved in 'YYYY-MM-DD HH:MM:SS' format (defaults to the earliest file modification time)
--sample              whether the files represent a sample from the source
-n NOTE, --note NOTE  a note to add to the collection
-f, --force           use the provided --source value, regardless of whether it is recognized
-u, --upgrade         upgrade the collection to the latest OCDS version
-c, --compile         create compiled releases from the collection
-e, --check           run structural checks on the collection
-k, --keep-open       keep collection open for future file additions

.. note::

   If the files are arrays of packages, only the first package's metadata is saved. In other words, it is assumed that all packages have the same metadata.

addfiles
~~~~~~~~

Load data into an **open** root collection, asynchronously.

.. code-block:: bash

   ./manage.py addfiles collection_id path [path ...]

closecollection
~~~~~~~~~~~~~~~

Close an **open** root collection and its derived collections, if any.

.. code-block:: bash

   ./manage.py closecollection collection_id

addchecks
~~~~~~~~~

Add processing steps to check data, if unchecked.

.. code-block:: bash

   ./manage.py addchecks collection_id

cancelcollection
~~~~~~~~~~~~~~~~

Cancel all processing of a collection.

.. note::

   For performance, the finisher worker picks one message for each collection, and ignores the rest. It requeues the message until the collection is completed. If the collection can never be completed, cancel the collection to stop the requeueing.

.. code-block:: bash

   ./manage.py cancelcollection collection_id

deletecollection
~~~~~~~~~~~~~~~~

Delete a collection and its ancestors.

Rows in the ``package_data`` and ``data`` tables are not deleted. Use :ref:`cli-deleteorphan` instead.

.. code-block:: bash

   ./manage.py deletecollection collection_id

collectionstatus
~~~~~~~~~~~~~~~~

Get the status of a root collection and its children.

.. code-block:: bash

   ./manage.py collectionstatus collection_id

.. _cli-deleteorphan:

deleteorphan
~~~~~~~~~~~~

Delete rows from the data and package_data tables that relate to no collections.

.. code-block:: bash

   ./manage.py deleteorphan

.. _cli-workers:

Workers
-------

.. note::

   `Consumers declare and bind queues, not publishers <https://ocp-software-handbook.readthedocs.io/en/latest/services/rabbitmq.html#bindings>`__.

   Start each worker before publishing messages (for example, with the :ref:`cli-load` command).

.. tip::

   Set the ``LOG_LEVEL`` environment variable to ``DEBUG`` to see log messages about message processing. For example:

   .. code-block:: bash

      env LOG_LEVEL=DEBUG ./manage.py finisher

.. _cli-api_loader:

api_loader
~~~~~~~~~~

Create collection files.

Consumes messages published by other software, like Kingfisher Collect.

.. code-block:: bash

   ./manage.py api_loader

.. _cli-file_worker:

file_worker
~~~~~~~~~~~

Create releases, records and compiled releases.

.. code-block:: bash

   ./manage.py file_worker

checker
~~~~~~~

Check collection files.

Performs no work if the collection's ``steps`` field excludes "check".

Errors if the ``ENABLE_CHECKER`` :ref:`environment variable<environment-variables>` is not set.

.. code-block:: bash

   ./manage.py checker

compiler
~~~~~~~~

Start compilation and route messages to the release compiler or record compiler.

Performs no work if the collection's ``steps`` field excludes "compile".

For a collection of release packages, starts compilation at most once if all collection files are loaded and the collection is closed.

.. code-block:: bash

   ./manage.py compiler

.. _cli-record_compiler:

record_compiler
~~~~~~~~~~~~~~~

Create compiled releases from records.

.. code-block:: bash

   ./manage.py record_compiler

.. _cli-release_compiler:

release_compiler
~~~~~~~~~~~~~~~~

Create compiled releases from releases with the same OCID.

.. code-block:: bash

   ./manage.py release_compiler

.. _cli-finisher:

finisher
~~~~~~~~

Set collections as completed, close compiled collections and cache row counts.

.. code-block:: bash

   ./manage.py finisher

wiper
~~~~~

Delete collections and their ancestors.

Rows in the ``package_data`` and ``data`` tables are not deleted.

.. code-block:: bash

   ./manage.py wiper
