new-transform-compile-releases
==============================

This command takes an existing source collection that you give it, and creates a new destination collection with a transformer that creates compiled releases.

Note this does not actually do the transforming - it simply marks that you want the work to be done, and creates the destination collection ready for the finished work to be put into.

The compile releases transformer can only work when it has all the data! You can create the transformed collection at any time, but the source collection must be completely stored before any work will be done by the transformer.

Pass the ID of the source collection. Use :doc:`list-collections` to look up the ID you want.

It will create a new destination collection to hold the compiled releases and return the ID of this to you.

.. code-block:: shell-session

    python ocdskingfisher-process-cli new-transform-compile-releases 17

You can specify 2 modes for this transform.

Use existing - in this mode if there are any existing compiled releases in the data, these will simply be used with no changes.

.. code-block:: shell-session

    python ocdskingfisher-process-cli new-transform-compile-releases --useexisting 17


Always compile - in this mode the transform will always compile a release itself, even when there are existing compiled releases in the data.

.. code-block:: shell-session

    python ocdskingfisher-process-cli new-transform-compile-releases --alwayscompile 17


After creating it, you should run transform-collection to actually do the work. See :doc:`transform-collection`
