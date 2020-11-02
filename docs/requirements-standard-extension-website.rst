Requirements on the Standard and Extension websites
===================================================

Part of this software checks data, and to do so it must download schema files `from the standard website <https://standard.open-contracting.org/latest/en/>`__ and any extension websites.

Thus it has a dependency on external websites.

If these websites are not currently available
---------------------------------------------

Checks should be stopped from running.

Do not run the following commands:

*   :doc:`cli/check-collection`
*   :doc:`cli/check-collections`
*   :doc:`cli/process-redis-queue`

Note these commands may be run from cron. Stop any currently running instances, and comment out any cron entries as needed.

If these websites were temporarily unavailable and are now available again
--------------------------------------------------------------------------

If any checks were run during the time the websites were unavailable, they will have failed and the error will be recorded in the database.

However this is not a useful error for analysts.

It is possible to clear these errors from the database and then the relevant data items will be rechecked.

First, look at the errors to make sure they are what you expect:

.. code-block:: shell

    SELECT DISTINCT(error) FROM release_check_error WHERE error LIKE 'HTTPSConnectionPool(host=%';
    SELECT DISTINCT(error) FROM record_check_error WHERE error LIKE 'HTTPSConnectionPool(host=%';

They should be error messages for the websites that were down.

If so, then delete those rows:

.. code-block:: shell

   DELETE FROM release_check_error WHERE error LIKE 'HTTPSConnectionPool(host=%';
   DELETE FROM record_check_error WHERE error LIKE 'HTTPSConnectionPool(host=%';

Then run the :doc:`cli/check-collections` command, and the checks will be redone. (This command may be set up on a cron, in which case it will run itself.)


