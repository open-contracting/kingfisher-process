upgrade-database
================

This creates or upgrades the tables in the PostgreSQL database::

    python ocdskingfisher-process-cli upgrade-database

To drop tables (and clear the Redis queue, if used) before upgrading, use the ``--deletefirst`` flag::

    python ocdskingfisher-process-cli upgrade-database --deletefirst

.. admonition:: OCDS Helpdesk deployment

   Don't use this. It is run by the SaltStack scripts that upgrade the tool.
