Contributing
============

.. seealso::

   In the Software Development Handbook:

   -  `Django <https://ocp-software-handbook.readthedocs.io/en/latest/python/django.html>`__
   -  `RabbitMQ <https://ocp-software-handbook.readthedocs.io/en/latest/services/rabbitmq.html>`__

PostgreSQL
----------

Kingfisher Process does a lot of work concurrently. As such, it is important to understand `Transaction Isolation <https://www.postgresql.org/docs/current/transaction-iso.html>`__ and `Explicit Locking <https://www.postgresql.org/docs/current/explicit-locking.html>`__, to guarantee that work isn't duplicated or missed.

.. note::

   Although OCP typically uses an ``en_US.UTF-8`` collation, the database has an ``en_GB.UTF-8`` collation, for `no particular reason <https://github.com/open-contracting/kingfisher-process/issues/239>`__.

Integration patterns
--------------------

`Enterprise Integration Patterns <https://en.wikipedia.org/wiki/Enterprise_Integration_Patterns>`__ describes many patterns used in this project and in RabbitMQ itself. In this project, we use, for example:

-  `Process Manager <https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html>`__: The collection's configuration determines how messages are routed through a series of steps. See also `Routing Slip <https://www.enterpriseintegrationpatterns.com/patterns/messaging/RoutingTable.html>`__.
-  `Idempotent Receiver <https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html>`__: Each worker should be able to safely receive the same message multiple times.
-  `Claim Check <https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html>`__: Instead of putting OCDS data in messages, we write it to disk and put a claim check in messages.
-  `Splitter <https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html>`__: For example, one message to load a large file (e.g. record package) might lead to many messages to process each part of the file (e.g. record).
-  `Aggregator <https://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html>`__: For example, the step to merge releases from release packages needs to wait for loading to be completed.

Legacy database
---------------

Kingfisher Process was rewritten to use Django and RabbitMQ, instead of Flask and SQLAlchemy.

You can compare ``models.py`` to the output of:

.. code-block:: shell

   env DATABASE_URL=postgresql://user@host/dbname ./manage.py inspectdb

To avoid an error when migrating from the SQLAlchemy-managed database to the Django-managed database, `run <https://docs.djangoproject.com/en/4.2/topics/migrations/#initial-migrations>`__:

.. code-block:: shell

   ./manage.py migrate --fake-initial

.. seealso::

   -  `Integrating Django with a legacy database <https://docs.djangoproject.com/en/4.2/howto/legacy-databases/>`__
