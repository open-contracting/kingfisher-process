transform-collection
====================

This command runs the configured transformer for the collection.

It can be run multiple times on a collection, and data already transformed will not be retransformed.

Pass the ID of the collection you want the work done in. Use :doc:`list-collections` to look up the ID you want.

.. code-block:: shell

    python ocdskingfisher-process-cli transform-collection 17

.. admonition:: OCDS Helpdesk deployment

   Don't use this. A cron job runs :doc:`transform-collections` once per hour.
