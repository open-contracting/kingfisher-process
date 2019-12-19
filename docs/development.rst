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

Updating Database tables graphic
--------------------------------

In https://kingfisher-process.readthedocs.io/en/latest/database-structure.html you will find a graphic of the schema.
If you change the database structure you will need to change this.

Download the database driver from https://jdbc.postgresql.org/ and SchemaSpy from https://github.com/schemaspy/schemaspy/releases .

First make sure your local database is up to date with all schema changes.

Then run SchemaSpy, something like:

    java -jar /bin/schemaspy.jar -t pgsql -dp /bin/postgresql.jar   -s public  -db ocdskingfisher  -u ocdskingfisher -p ocdskingfisher -host localhost -o /vagrant/schemaspy

In the folder of data that results, take the schemaspy/diagrams/summary/relationships.real.large.png file. Copy it over docs/_static/database-tables.png.

Finally, use a standard image editing programme like https://www.gimp.org/ to edit out the row counts.

