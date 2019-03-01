Command line tool
=================


You can use the tool with the provided CLI script. There are various sub commands.

You can pass the `quiet` flag to all sub commands, to get less output printed to the terminal.

.. code-block:: shell-session

    python ocdskingfisher-process-cli --quiet run ...

Commands for working with the data and marking that you want actions to happen:

.. toctree::

   cli-list-collections.rst
   cli-new-collection.rst
   cli-local-load.rst
   cli-end-collection-store.rst
   cli-new-collection-note.rst
   cli-new-transform-compile-releases.rst
   cli-new-transform-upgrade-1-0-to-1-1.rst
   cli-delete-collection.rst

Commands for processing actions that have been requested:

.. toctree::

   cli-check-collection.rst
   cli-check-collections.rst
   cli-transform-collection.rst
   cli-transform-collections.rst
   cli-delete-collections.rst
   cli-process-redis-queue.rst

Commands for installing and upgrading:

.. toctree::

   cli-upgrade-database.rst

