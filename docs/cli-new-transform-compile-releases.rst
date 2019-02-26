Command line tool - new-transform-compile-releases option
==========================================================

This command takes an existing source collection that you give it, and creates a new destination collection with a transform that creates compiled releases.

Note this does not actually do the work of the transform - it simply marks that you want the work to be done, and creates the destination collection ready for the finished work to be put into.

The compile releases transform can only work when it has all the data! You can create the transform at any time, but the source collection must be completely stored before any work will be done on the transform.

Pass the ID of the source collection. Use :doc:`cli-list-collections` to look up the ID you want.

It will create a new destination collection to hold the compiled releases and return the ID of this to you.

.. code-block:: shell-session

    python ocdskingfisher-process-cli new-transform-compile-releases 17

After creating it, you should run transform-collection to actually do the work. See :doc:`cli-transform-collection`
