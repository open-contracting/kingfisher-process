Installation
============

Requirements
------------

- Python 3.5 or higher
- PostgresSQL 10 or higher

To run the web app:

- A web server that can run a WSGI Python app

Installation
------------

Create a virtual environment::

    virtualenv -p python3 .ve

Activate the virtual environment::

    source .ve/bin/activate

Install the requirements::

    pip install -r requirements.txt
    pip install -e .

Database
--------

Create a user, for example::

    sudo -u postgres createuser ocdskingfisher --pwprompt

Create a UTF8-encoded PostgreSQL database and give the user write access, for example::

    sudo -u postgres createdb ocdskingfisher -O ocdskingfisher --template template0 --encoding UTF8 --lc-collate en_US.UTF-8 --lc-ctype en_US.UTF-8

Set the tool's database connection setting, replacing at least ``PASSWORD`` in this example::

    export KINGFISHER_PROCESS_DB_URI='postgres://ocdskingfisher:PASSWORD@localhost/ocdskingfisher'

.. note::

   This configures the tool within your current command-line session only. For longer-term options, see :doc:`config`.

Create the tables in the database (more information at :doc:`cli/upgrade-database`)::

    python ocdskingfisher-process-cli upgrade-database

Next: :doc:`config`
