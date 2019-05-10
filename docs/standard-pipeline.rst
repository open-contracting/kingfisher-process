Default pre-processing pipeline
===============================

The default pre-processing pipeline operates on any new collection that is not a `transform <../data-model/#transforms>`__; that is, any collection created via the :doc:`web/api-v1` or :doc:`cli/local-load`.

The pipeline uses transforms to:

* upgrade the collection's incoming data from OCDS 1.0 to OCDS 1.1
* merge the collection's upgraded data into compiled releases

The pipeline is off by default. To turn it on, see :doc:`config`.
