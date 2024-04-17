OCDS Kingfisher Process
=======================

.. include:: ../README.rst

Typical usage
-------------

Kingfisher Process receives OCDS data either from Kingfisher Collect or the :ref:`cli-load` command. Data is stored in a PostgreSQL database and organized into "collections," as described in :doc:`database`.

The base collection can be tranformed into new collections: either by upgrading from OCDS 1.0 to 1.1 or by creating compiled releases. The base collection can also be checked for structural errors, using the same library as the `OCDS Data Review Tool <https://review.standard.open-contracting.org>`__. See the documentation for `Kingfisher Collect <https://kingfisher-collect.readthedocs.io/en/latest/kingfisher_process.html>`__ and the :ref:`cli-load` command for details.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   get-started
   cli
   database
   querying-data
   contributing/index
