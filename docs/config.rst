Configuration
=============

Postgresql Configuration
------------------------

Postgresql Database settings can be set using a `~/.config/ocdskingfisher-process/config.ini` file. A sample one is included in the
main directory.


.. code-block:: ini

    [DBHOST]
    HOSTNAME = localhost
    PORT = 5432
    USERNAME = ocdsdata
    PASSWORD = FIXME
    DBNAME = ocdsdata


It will also attempt to load the password from a `~/.pgpass` file, if one is present.

You can also set the `KINGFISHER_PROCESS_DB_URI` environmental variable to use a custom PostgreSQL server, for example
`postgresql://user:password@localhost:5432/dbname`.

The order of precedence is (from least-important to most-important):

  -  config file
  -  password from `~/.pgpass`
  -  environmental variable

Web Configuration
-----------------

Version 1 of the Web API requires a key to access. Multiple keys can be set, seperated by a comma.

The key can be set in the `~/.config/ocdskingfisher-process/config.ini` file:


.. code-block:: ini


    [WEB]
    API_KEYS = 1234,5678


`1234` and `5678` will both be valid keys.

They can also be set in the `KINGFISHER_PROCESS_WEB_API_KEYS` environmental variable.

Collection Defaults Configuration
---------------------------------

When you create a new collection, certain flags are set on it automatically. You can configure what the default values for them are:

.. code-block:: ini

    [COLLECTION_DEFAULT]
    CHECK_DATA = true
    CHECK_OLDER_DATA_WITH_SCHEMA_1_1 = false


Logging Configuration
---------------------

This tool will provide additional logging information using the standard Python logging module, with loggers in the "ocdskingfisher"
namespace.

When using the command line tool, it can be configured by setting a `~/.config/ocdskingfisher-process/logging.json` file.
Sample ones are included in the main directory (one without debugging messages, and one with debugging messages).

Standard Pipeline
-----------------

This can be turned on in the `~/.config/ocdskingfisher-process/config.ini` file.

.. code-block:: ini

    [STANDARD_PIPELINE]
    RUN = true

Redis Configuration
-------------------

You need an Redis server if you want a background queue to process items immediately. If you aren't using the background queue, you don't need a Redis server.


.. code-block:: ini

    [REDIS]
    HOST = localhost
    PORT = 6379
    DATABASE = 0

Sentry Configuration
--------------------

This is optional - if you want to track crashes use https://sentry.io/welcome/

.. code-block:: ini

    [SENTRY]
    DSN = https://xxxxxxxxxxxxxxxxxxxxxxxxxx@sentry.io/xxxxxxx

