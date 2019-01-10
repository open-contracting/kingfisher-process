Command line tool - check-collection option
===========================================

This command checks all data so far in a collection.

It can be run multiple times on a collection, and data already checked will not be rechecked.

Pass the ID of the collection you want checked. Use :doc:`cli-list-collections` to look up the ID you want.

.. code-block:: shell-session

    python ocdskingfisher-process-cli check-collection 17

