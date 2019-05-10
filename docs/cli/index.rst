Command-line tool
=================

You can use the tool with the provided CLI script. There are various sub-commands.

You can pass the `quiet` flag to all sub commands, to get less output printed to the terminal.

.. code-block:: shell-session

    python ocdskingfisher-process-cli --quiet <command> ...

Installing and upgrading:

.. toctree::

   upgrade-database.rst

Working with the data and marking that you want actions to happen:

.. toctree::

   list-collections.rst
   new-collection.rst
   local-load.rst
   end-collection-store.rst
   new-collection-note.rst
   new-transform-compile-releases.rst
   new-transform-upgrade-1-0-to-1-1.rst
   delete-collection.rst

Processing actions that have been requested:

.. toctree::

   check-collection.rst
   check-collections.rst
   transform-collection.rst
   transform-collections.rst
   delete-collections.rst
   process-redis-queue.rst
