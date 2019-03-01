Command line tool - transform-collection option
===============================================

This command does any transform work for a collection.

It can be run multiple times on a collection, and data already transformed will not be retransformed.

Pass the ID of the collection you want the work done in. Use :doc:`cli-list-collections` to look up the ID you want.

.. code-block:: shell-session

    python ocdskingfisher-process-cli transform-collection 17


Do not use on hosted Kingfisher
-------------------------------

Do not use this command on Hosted kingfisher - it is run automatically for you.

