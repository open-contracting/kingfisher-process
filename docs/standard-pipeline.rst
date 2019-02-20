Standard Pipeline
=================

You can choose to have a standard pipeline run over all incoming data.

This is off by default and must be turned on - see :doc:`config`.

When on, any new collections that are not a Transform are assumed to be new data and processed. So this includes new collections created via the HTTP API, or via Local Load.

Transforms are created:
  *  from the new collection, a transform is created upgrading the incoming data to 1.1 data
  *  from the upgraded 1.1 data, a transform is created to compile releases

