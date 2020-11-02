transform-collections
=====================

This command runs the configured transformers for all collections.

It can be run multiple times on a collection, and data already transformed will not be retransformed.

You should only run one of these at once, as if two are run at once they may try and do the same work.

.. code-block:: shell

    python ocdskingfisher-process-cli transform-collections

By default, it will run with a single thread. You can have it run multiple threads at once. A single collection will still be processed at the same speed, but multiple collections will be processed at once.

.. code-block:: shell

    python ocdskingfisher-process-cli transform-collections --threads 10


Running from cron
-----------------

You can also pass a maximum number of seconds that the process should run for.

.. code-block:: shell

    python ocdskingfisher-process-cli transform-collections --runforseconds 60

Soon after that number of seconds has passed, the command will exit.
(The command will finish the transforming it's currently doing before stopping, so it may run slightly longer than specified. Allow a minute extra to be safe.)

You can use this option with a cron entry; set a cron entry for this command to run every hour and pass runforseconds as 3540 (60 seconds/minute * 59 minutes).

Then when new data appears in the system, there is no need for someone to run :doc:`transform-collection` by hand - the process run by cron will pick up the new data itself eventually.

The runforseconds option will make sure that only one of these cron processes runs at once.

.. admonition:: OCDS Helpdesk deployment

   Don't use this. A cron job runs this once per hour.
