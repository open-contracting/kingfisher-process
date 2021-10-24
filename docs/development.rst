Development
===========

Run tests
---------

**The tests drop and re-create the database**; you should specify a testing database with an environment variable. See :doc:`config`.

Run the tests with, for example:

    KINGFISHER_PROCESS_DB_URI='postgres:///ocdskingfisher-test' pytest

Create migrations
-----------------

#. Create a database migration with `Alembic <https://alembic.sqlalchemy.org/>`__, for example:

   .. code-block:: bash

      alembic --config=mainalembic.ini revision -m "A short description of what the migration does"

#. Fill in the migration
#. Add/update tables, indexes and/or constraints in ``database.py`` to match the migration
#. If a new table is created, update the ``delete_tables`` method

Note: Do not create simultaneous branches, each with its own migration(s). Instead, merge one branch, then create the next migration, to avoid `multiple heads <https://stackoverflow.com/questions/22342643/alembic-revision-multiple-heads-due-branching-error/>`__.
