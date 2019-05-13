OCDS Kingfisher - Process |release|
===================================

.. include:: ../README.rst

Typical usage
-------------

Kingfisher Process receives OCDS data either via the :ref:`web API <web-api>` (used by Kingfisher Scrape) or the :doc:`cli/local-load` command.

OCDS data are stored in a PostgreSQL database and organized into "collections", as described in the :doc:`data-model` and specified in the :doc:`database-structure`. Once incoming data are stored:

* If the :doc:`standard-pipeline` is enabled, the data are automatically pre-processed into new collections.
* If the :ref:`schema checks <schema-check-flags>` are enabled, the data are automatically checked for schema errors.

Once the data are stored, you can query the PostgreSQL database; refer to the :doc:`data-model` and :doc:`database-structure` for an orientation to the database tables.

A :doc:`cli/index` allows you to list collections, add notes to collections, run schema checks, and :ref:`transform collections <transformed-collections>`.

You can run the :ref:`web app <web-app>` to view metadata about collections and files.

And that's it! In short, Kingfisher Process accepts "raw" OCDS data, and then checks and pre-processes it, so that your data analysis can be more predictable and repeatable.

.. toctree::
   :maxdepth: 1

   requirements-install.rst
   config.rst
   data-model.rst
   database-structure.rst
   standard-pipeline.rst
   cli/index.rst
   web.rst
   development.rst
