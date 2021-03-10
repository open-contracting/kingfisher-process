new-collection
==============

This command creates a new collection in the system.

.. code-block:: shell

    python ocdskingfisher-process-cli new-collection my-own-source-id  "2019-01-20 10:00:12"
    python ocdskingfisher-process-cli new-collection my-own-source-id  "2019-01-20 10:00:12" --sample
    python ocdskingfisher-process-cli new-collection my-own-source-id  "2019-01-20 10:00:12" --sample --ocds-version 1.0


You may not need to run this; collections will be created at certain points automatically for you.
For instance, when data is pushed to the Web API.

But you may need to create a collection specially.
For instance, you may want to create a new collection to load some local files into. See :doc:`local-load`.
