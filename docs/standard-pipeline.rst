Default pre-processing pipeline
===============================

The default pre-processing pipeline operates on any new collection that is not a :ref:`transformed collection <transformed-collections>`; that is, any collection loaded via the :doc:`web/api-v1` or the :doc:`cli/local-load` command.

The pipeline uses transforms to:

* upgrade the collection's incoming data from OCDS 1.0 to OCDS 1.1
* merge the collection's upgraded releases into compiled releases

The pipeline is off by default. To turn it on, see :doc:`config`.
