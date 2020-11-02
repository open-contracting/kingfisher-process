process-redis-queue-collection-store-finished
=============================================


This commands processes any data in the Redis queue for a collection that has just been marked as finishing it's store stage.

It will keep running until you stop it manually.

It is safe to run more than one of these commands at once.

.. code-block:: shell

    python ocdskingfisher-process-cli process-redis-queue-collection-store-finished

Running from cron
-----------------

You can also pass a maximum number of seconds that the process should run for.

.. code-block:: shell

    python ocdskingfisher-process-cli process-redis-queue-collection-store-finished--runforseconds 60

Soon after that number of seconds has passed, the command will exit.
(The command will finish the work it's currently doing before stopping, so it may run slightly longer than specified. Allow a minute extra to be safe.)

.. admonition:: OCDS Helpdesk deployment

   Don't use this. A cron job runs this once per hour.
