Logging
=======

If logging is configured using a :ref:`default configuration<config-logging>`, then log messages are written to ``info.log`` (and possibily ``debug.log``).

Log messages are formatted as::

    %(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s

You can find the meaning of the ``%(…)s`` attributes in the `Python documentation <https://docs.python.org/3/library/logging.html#logrecord-attributes>`__.

In particular, you can use the ``name`` attribute to filter messages by topic. For example:

.. code-block:: bash

    grep NAME info.log | less

where ``NAME`` is one of:

ocdskingfisher.checks
  An ``INFO`` or ``DEBUG``-level message for each collection, file item, release and record that is checked for structural errors.
ocdskingfisher.cli
  An ``INFO``-level message whenever a CLI command is run, by a user or by `cron <https://en.wikipedia.org/wiki/Cron>`__.
ocdskingfisher.cli.check-collections
  An ``INFO``-level message when checking each collection, and when starting and finishing the command.
ocdskingfisher.cli.delete-collections
  An ``INFO``-level message when deleting each collection and orphan data, and when starting and finishing the command.
ocdskingfisher.cli.transform-collections
  An ``INFO``-level message when starting and finishing the command.
ocdskingfisher.database.delete-collection
  A ``DEBUG``-level message for each step of deleting a collection.
ocdskingfisher.redis-queue
  An ``INFO``-level message for each Redis message received.
ocdskingfisher.redis-queue-collection-store-finished
  An ``INFO``-level message for each Redis message received.
ocdskingfisher.cli.update-collection-caches
  An ``INFO``-level message when updating each collection, and when starting the command.
odskingfisher.web
  An informative message for each web API call.
