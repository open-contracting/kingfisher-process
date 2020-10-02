OCDS Kingfisher Process
=======================

.. include:: ../README.rst

Typical usage
-------------

Kingfisher Process receives OCDS data either via the :ref:`web API <web-api>` (used by Kingfisher Collect) or the :doc:`cli/local-load` command.

OCDS data are stored in a PostgreSQL database and organized into "collections", as described in the :doc:`data-model` and specified in the :doc:`database-structure`. Once incoming data are stored:

* If the :doc:`standard-pipeline` is enabled, the data are automatically pre-processed into new collections.
* If the :ref:`schema checks <schema-check-flags>` are enabled, the data are automatically checked for schema errors.

OCDS data can be published as either `releases or records <https://standard.open-contracting.org/latest/en/getting_started/releases_and_records/>`__. A release is a point-in-time update about a contracting process, and a record provides an index to all releases for a contracting process.

Kingfisher Process can automatically transform a collection of releases into a collection of compiled releases, containing a single compiled release per contracting process. A compiled release contains the latest value of each field from its original releases.

Once the data are stored, you can query the PostgreSQL database; refer to the :doc:`data-model` and :doc:`database-structure` for an orientation to the database tables.

A :doc:`cli/index` allows you to list collections, add notes to collections, run schema checks, and :ref:`transform collections <transformed-collections>`.

You can run the :ref:`web app <web-app>` to view metadata about collections and files.

And that's it! In short, Kingfisher Process accepts "raw" OCDS data, and then checks and pre-processes it, so that your data analysis can be more predictable and repeatable.

.. toctree::
   :maxdepth: 1

   requirements-install.rst
   requirements-standard-extension-website.rst
   config.rst
   data-model.rst
   database-structure.rst
   querying-data.rst
   standard-pipeline.rst
   looking-for-collection-errors.rst
   logging.rst
   cli/index.rst
   web.rst
   transform/compile-release.rst
   development.rst
