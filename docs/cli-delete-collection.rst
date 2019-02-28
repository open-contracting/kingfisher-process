Command line tool - delete-collection option
=============================================

This command deletes a collection in the system.

A collection can only be deleted if it doesn't have any transform.

Note this does not actually do the work of deleting - it simply marks that you want the deletion to be done.

Pass the ID of the collection you want the work done in. Use :doc:`cli-list-collections` to look up the ID you want.

.. code-block:: shell-session

    python ocdskingfisher-process-cli delete-collection 17

After marking it as deleted, you should run delete-collections to actually do the work. See :doc:`cli-delete-collections`
