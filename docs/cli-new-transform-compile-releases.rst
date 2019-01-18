Command line tool - new-transform-compile-releases option
==========================================================

This command takes an existing source collection that you give it, and creates a new destination collection with a transform that creates compiled releases.

The compile releases transform can only work when it has all the data, therefore the source collection must be completely saved before you start this transform.

Pass the ID of the source collection. Use :doc:`cli-list-collections` to look up the ID you want.

It will create a new destination collection to hold the compiled releases and return the ID of this to you.

.. code-block:: shell-session

    python ocdskingfisher-process-cli new-transform-compile-releases 17

After creating it, you should run transform-collection to actually do the work. See :doc:`cli-transform-collection`
