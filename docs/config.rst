Configuration
=============

Database Configuration
----------------------

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

TODO write up the API Key - notes: KINGFISHER_PROCESS_WEB_API_KEYS env var or [WEB] API_KEYS= in ini. Comma seperated.

Logging Configuration
---------------------

This tool will provide additional logging information using the standard Python logging module, with loggers in the "ocdskingfisher"
namespace.

When using the command line tool, it can be configured by setting a `~/.config/ocdskingfisher-process/logging.json` file.
A sample one is included in the main directory.

