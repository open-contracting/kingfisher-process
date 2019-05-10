Development
===========

Run tests
---------

Run ``py.test`` from the root directory.

**The tests will drop and re-create the database**; you should specify a testing database with an environment variable. See :doc:`config`.

Create migrations
-----------------

Create database migrations with `Alembic <https://alembic.sqlalchemy.org/>`__, for example::

    alembic --config=mainalembic.ini revision -m "message"

Add changes to the new migration, and update ``database.py`` table definitions and the ``delete_tables`` method.
