Command line tool - new-transform-upgrade-1-0-to-1-1 option
===========================================================

This command takes an existing source collection that you give it, and creates a new destination collection with a transform that upgrades 1.0 data to 1.1.

Note this does not actually do the work of the transform - it simply marks that you want the work to be done, and creates the destination collection ready for the finished work to be put into.

Pass the ID of the source collection. Use :doc:`cli-list-collections` to look up the ID you want.

It will create a new destination collection to hold the upgraded data and return the ID of this to you.

.. code-block:: shell-session

    python ocdskingfisher-process-cli new-transform-upgrade-1-0-to-1-1 17

After creating it, you should run transform-collection to actually do the work. See :doc:`cli-transform-collection`
