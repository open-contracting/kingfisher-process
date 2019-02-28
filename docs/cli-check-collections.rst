Command line tool - check-collections option
===========================================

This command checks all data so far in all collections.

It can be run multiple times, and data already checked will not be rechecked.

You should only run one of these at once, as if two are run at once they may try and do the same work.

.. code-block:: shell-session

    python ocdskingfisher-process-cli check-collections

Running from cron
-----------------

You can also pass a maximum number of seconds that the process should run for.

.. code-block:: shell-session

    python ocdskingfisher-process-cli check-collections --runforseconds 60

Soon after that number of seconds has passed, the command will exit.
(The command will finish the check it's currently doing before stopping, so it may run slightly longer than specified. Allow a minute extra to be safe.)

You can use this option with a cron entry; set a cron entry for this command to run every hour and pass runforseconds as 3540 (60 seconds/minute * 59 minutes).

Then when new data appears in the system, there is no need for someone to run :doc:`cli-check-collection` by hand - the process run by cron will pick up the new data itself eventually.

The runforseconds option will make sure that only one of these cron processes runs at once.

Do not use on hosted Kingfisher
-------------------------------

Do not use this command on Hosted kingfisher - it is run automatically for you.

