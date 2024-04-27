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

deletecollection
~~~~~~~~~~~~~~~~

Delete a collection and its ancestors.

Rows in the ``package_data`` and ``data`` tables are not deleted.

.. code-block:: bash

   ./manage.py deletecollection collection_id

collectionstatus
~~~~~~~~~~~~~~~~

Get the status of a root collection and its children.

.. code-block:: bash

   ./manage.py collectionstatus collection_id

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

api_loader
~~~~~~~~~~

Create collection files.

Consumes messages published by other software, like Kingfisher Collect.

.. code-block:: bash

   ./manage.py api_loader

file_worker
~~~~~~~~~~~

Create records, releases and compiled releases.

.. code-block:: bash

   ./manage.py file_worker

checker
~~~~~~~

Check collection files.

Performs no checks if the collection's ``steps`` field excludes "check".

Errors if the ``ENABLE_CHECKER`` :ref:`environment variable<environment-variables>` is not set.

.. code-block:: bash

   ./manage.py checker

compiler
~~~~~~~~

Start compilation and route messages to the record or release compilers.

Performs no checks if the collection's ``steps`` field excludes "compile".

For a collection of release packages, starts compilation at most once if all collection files are loaded and the collection is closed.

.. code-block:: bash

   ./manage.py compiler

record_compiler
~~~~~~~~~~~~~~~

Create compiled releases from records.

.. code-block:: bash

   ./manage.py record_compiler

release_compiler
~~~~~~~~~~~~~~~~

Create compiled releases from releases with the same OCID.

.. code-block:: bash

   ./manage.py release_compiler

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
