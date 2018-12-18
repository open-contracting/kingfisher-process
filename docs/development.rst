Development
===========

Run tests
---------

Run `py.test` from root directory.

The tests will drop and create the database, so you probably want to specify a special testing database with a environmental variable - see :doc:`config`.


Main Database - Postgresql
--------------------------

Create DB Migrations with Alembic - http://alembic.zzzcomputing.com/en/latest/

.. code-block:: shell-session

    alembic --config=mainalembic.ini revision -m "message"

Add changes to new migration, and make sure you update database.py table structures and delete_tables to.

