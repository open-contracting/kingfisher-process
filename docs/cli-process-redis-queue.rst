Command line tool - process-redis-queue option
==============================================

This commands processes any data in the Redis queue.

It will keep running until you stop it manually.

It is safe to run more than one of these commands at once.

.. code-block:: shell-session

    python ocdskingfisher-process-cli process-redis-queue

Running from cron
-----------------

You can also pass a maximum number of seconds that the process should run for.

.. code-block:: shell-session

    python ocdskingfisher-process-cli process-redis-queue --runforseconds 60

Soon after that number of seconds has passed, the command will exit.
(The command will finish the work it's currently doing before stopping, so it may run slightly longer than specified. Allow a minute extra to be safe.)


Do not use on hosted Kingfisher
-------------------------------

Do not use this command on Hosted kingfisher - it is run automatically for you.

