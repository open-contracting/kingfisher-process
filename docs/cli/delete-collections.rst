delete-collections
==================

This command deletes the collections in the system where `deleted_at` column is not null.

.. code-block:: shell-session

    python ocdskingfisher-process-cli delete-collections

.. note:: OCDS Helpdesk deployment

   Don't use this. A cron job runs this once a month.
