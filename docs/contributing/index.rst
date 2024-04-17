Contributing
============

.. seealso::

   In the Software Development Handbook:

   -  `Django <https://ocp-software-handbook.readthedocs.io/en/latest/python/django.html>`__
   -  `RabbitMQ <https://ocp-software-handbook.readthedocs.io/en/latest/services/rabbitmq.html>`__

Setup
-----

#. Install PostgreSQL and RabbitMQ
#. Create a Python 3.11 virtual environment
#. Set up the git pre-commit hook:

   .. code-block:: bash

      pip install pre-commit
      pre-commit install

#. Install development dependencies:

   .. code-block:: bash

      pip install pip-tools
      pip-sync requirements_dev.txt

#. Run database migrations:

   .. code-block:: bash

      ./manage.py migrate

Development
-----------

Run the server (API):

.. code-block:: bash

   ./manage.py runserver

.. seealso::

   :ref:`cli-workers`

Testing
-------

.. code-block:: bash

   ./manage.py test

Environment variables
---------------------

See `OCP's approach to Django settings <https://ocp-software-handbook.readthedocs.io/en/latest/python/django.html#settings>`__. New variables are:

LOG_LEVEL
  The log level of the root logger
RABBIT_URL
  The `connection string <https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html#using-urlparameters>`__ for RabbitMQ
RABBIT_EXCHANGE_NAME
  The name of the RabbitMQ exchange. Follow the pattern ``kingfisher_process_{service}_{environment}`` like ``kingfisher_process_data_registry_production``
SCRAPYD_URL
  The base URL of Scrapyd, for example: ``http://localhost:6800``
SCRAPYD_PROJECT
  The project within Scrapyd
KINGFISHER_COLLECT_FILES_STORE
  The directory from which to read the files written by Kingfisher Collect. If Kingfisher Collect and Kingfisher Process share a filesystem, this will be the same value for both services.
ENABLE_CHECKER
  Whether to enable the ``checker`` worker

It is recommended to set ``REQUESTS_POOL_MAXSIZE`` to ``20``, to set the maximum number of connections to save in the `connection pool <https://urllib3.readthedocs.io/en/latest/advanced-usage.html#customizing-pool-behavior>`__ used by the `ocdsextensionregistry <https://ocdsextensionregistry.readthedocs.io/en/latest/changelog.html>`__ package. This is the same value as the `prefetch_count <https://www.rabbitmq.com/docs/consumer-prefetch>`__ used by RabbitMQ consumers.

PostgreSQL
----------

Concurrency
~~~~~~~~~~~

Kingfisher Process works concurrently. As such, it is important to understand `Transaction Isolation <https://www.postgresql.org/docs/current/transaction-iso.html>`__ and `Explicit Locking <https://www.postgresql.org/docs/current/explicit-locking.html>`__, to guarantee that work isn't duplicated or missed. As appropriate:

-  Use optimistic locking to not overwrite data, for example:

   .. code-block:: python

      updated = Collection.objects.filter(pk=collection.pk, completed_at=None).update(completed_at=Now())

-  Use optimistic locking to not repeat work, for example:

   .. code-block:: python

      updated = Collection.objects.filter(pk=collection.pk, compilation_started=False).update(compilation_started=True)
      if not updated:
          return

-  `Specify which fields to save <https://docs.djangoproject.com/en/4.2/ref/models/instances/#ref-models-update-fields>`__ on a ``Collection`` instance
-  `Lock rows using SELECT ... FOR UPDATE <https://docs.djangoproject.com/en/4.2/ref/models/querysets/#select-for-update>`__ on the ``collection`` table

.. note::

   Although OCP typically uses an ``en_US.UTF-8`` collation, the database has an ``en_GB.UTF-8`` collation, for `no particular reason <https://github.com/open-contracting/kingfisher-process/issues/239>`__.

Legacy database
~~~~~~~~~~~~~~~

Kingfisher Process was rewritten to use Django and RabbitMQ, instead of Flask and SQLAlchemy.

You can compare ``models.py`` to the output of:

.. code-block:: shell

   env DATABASE_URL=postgresql://user@host/dbname ./manage.py inspectdb

.. seealso::

   -  `Integrating Django with a legacy database <https://docs.djangoproject.com/en/4.2/howto/legacy-databases/>`__

.. _integration-patterns:

RabbitMQ
--------

`Enterprise Integration Patterns <https://en.wikipedia.org/wiki/Enterprise_Integration_Patterns>`__ describes many patterns used in this project and in RabbitMQ. We use:

-  `Process Manager <https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html>`__: The collection's configuration determines how messages are routed through a series of steps. See also `Routing Slip <https://www.enterpriseintegrationpatterns.com/patterns/messaging/RoutingTable.html>`__.
-  `Idempotent Receiver <https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html>`__: Each worker should be able to safely receive the same message multiple times.
-  `Claim Check <https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html>`__: Instead of putting OCDS data in messages, we write it to disk and put a claim check in messages.
-  `Splitter <https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html>`__: For example, one message to load a large file (e.g. record package) might lead to many messages to process each part of the file (e.g. record).
-  `Aggregator <https://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html>`__: For example, the step to merge releases from release packages needs to wait for loading to be completed.
