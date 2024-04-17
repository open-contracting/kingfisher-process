Contributing
============

Working on Kingfisher Process requires a good understanding of Django. It is recommended to be familiar with all  `Django tutorials <https://docs.djangoproject.com/en/4.2/intro/>`__.

Django
------

Documentation
~~~~~~~~~~~~~

`Full documentation <https://docs.djangoproject.com/>`__

Here are quick links to relevant pages, when needed. Each page has its table of contents in the sidebar.

-  `Topics <https://docs.djangoproject.com/en/4.2/topics/>`__ is the best way into a new concept.

   -  `Database access optimization <https://docs.djangoproject.com/en/4.2/topics/db/optimization/>`__ is useful, because it is more about not being slow than about being fast.

-  `How-to <https://docs.djangoproject.com/en/4.2/howto/>`__

   -  `Writing custom django-admin commands <https://docs.djangoproject.com/en/4.2/howto/custom-management-commands/>`__
   -  `Integrating Django with a legacy database <https://docs.djangoproject.com/en/4.2/howto/legacy-databases/>`__
   -  `Deploying Django <https://docs.djangoproject.com/en/4.2/howto/deployment/>`__

-  `Reference <https://docs.djangoproject.com/en/4.2/ref/>`__

   -  `Model field reference <https://docs.djangoproject.com/en/4.2/ref/models/fields/>`__
   -  `Model instance reference <https://docs.djangoproject.com/en/4.2/ref/models/instances/>`__
   -  `Model index reference <https://docs.djangoproject.com/en/4.2/ref/models/indexes/>`__
   -  `Constraints reference <https://docs.djangoproject.com/en/4.2/ref/models/constraints/>`__

Please feel free to add links to this section.

Tips & tricks
~~~~~~~~~~~~~

Legacy database
^^^^^^^^^^^^^^^

Kingfisher Process was rewritten to use Django and RabbitMQ. It was previously written using Flask and SQLAlchemy.

You can compare ``models.py`` to the output of:

.. code-block:: shell

   env DATABASE_URL=postgresql://user@host/dbname ./manage.py inspectdb

To avoid an error when migrating from the SQLAlchemy-managed database to the Django-managed database, `run <https://docs.djangoproject.com/en/4.2/topics/migrations/#initial-migrations>`__:

.. code-block:: shell

   ./manage.py migrate --fake-initial

Management commands
^^^^^^^^^^^^^^^^^^^

To access a Python shell with Django configured:

.. code-block:: shell

   ./manage.py shell

To access the default database:

.. code-block:: shell

   ./manage.py dbshell

Learn more about `manage.py <https://docs.djangoproject.com/en/4.2/ref/django-admin/>`__.

System checks
^^^^^^^^^^^^^

If we decide to add `system checks <https://docs.djangoproject.com/en/4.2/topics/checks/>`__ in a ``process/checks.py`` file, we need to add this to ``apps.py``:

.. code-block:: python

   def ready(self):
       import process.checks

RabbitMQ
--------

See the `Software Development Handbook <https://ocp-software-handbook.readthedocs.io/en/latest/services/rabbitmq.html>`__.

PostgreSQL
----------

Kingfisher Process does a lot of work concurrently. As such, it is important to understand `Transaction Isolation <https://www.postgresql.org/docs/current/transaction-iso.html>`__ and `Explicit Locking <https://www.postgresql.org/docs/current/explicit-locking.html>`__, to guarantee that work isn't duplicated or missed.

.. note::

   Although OCP typically uses an ``en_US.UTF-8`` collation, the database has an ``en_GB.UTF-8`` collation, for `no particular reason <https://github.com/open-contracting/kingfisher-process/issues/239>`__.

Integration patterns
--------------------

`Enterprise Integration Patterns <https://en.wikipedia.org/wiki/Enterprise_Integration_Patterns>`__ describes many patterns used in this project and in RabbitMQ itself. In this project, we use, for example:

-  `Process Manager <https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html>`__: The collection's configuration determines how messages are routed through a series of steps. See also `Routing Slip <https://www.enterpriseintegrationpatterns.com/patterns/messaging/RoutingTable.html>`__.
-  `Idempotent Receiver <https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html>`__: Each worker should be to safely receive the same message multiple times.
-  `Claim Check <https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html>`__: Instead of putting OCDS data in messages, we write it to disk and put a claim check in messages.
-  `Splitter <https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html>`__: For example, one message to load a large file might lead to many messages to process each part of the file.
-  `Aggregator <https://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html>`__: For example, the step to merge releases needs to wait for loading to be completed.
