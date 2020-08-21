Contributing
============

Working on Kingfisher Process requires a good understanding of Django and RabbitMQ. It is recommended to be familiar with all  `Django tutorials <https://docs.djangoproject.com/en/3.1/intro/>`__ and `RabbitMQ tutorials <https://www.rabbitmq.com/getstarted.html>`__.

Django
------

Documentation
~~~~~~~~~~~~~

`Full documentation <https://docs.djangoproject.com/>`__

Here are quick links to relevant pages, when needed. Each page has its table of contents in the sidebar.

-  `Topics <https://docs.djangoproject.com/en/3.1/topics/>`__ is the best way into a new concept.

   -  `Database access optimization <https://docs.djangoproject.com/en/3.1/topics/db/optimization/>`__ is useful, because it is more about not being slow than about being fast.

-  `How-to <https://docs.djangoproject.com/en/3.1/howto/>`__

   -  `Writing custom django-admin commands <https://docs.djangoproject.com/en/3.1/howto/custom-management-commands/>`__
   -  `Integrating Django with a legacy database <https://docs.djangoproject.com/en/3.1/howto/legacy-databases/>`__
   -  `Deploying Django <https://docs.djangoproject.com/en/3.1/howto/deployment/>`__

-  `Reference <https://docs.djangoproject.com/en/3.1/ref/>`__

   -  `Model field reference <https://docs.djangoproject.com/en/3.1/ref/models/fields/>`__
   -  `Model instance reference <https://docs.djangoproject.com/en/3.1/ref/models/instances/>`__
   -  `Model index reference <https://docs.djangoproject.com/en/3.1/ref/models/indexes/>`__
   -  `Constraints reference <https://docs.djangoproject.com/en/3.1/ref/models/constraints/>`__

Please feel free to add links to this section.

Tips & tricks
~~~~~~~~~~~~~

Management commands
^^^^^^^^^^^^^^^^^^^

To access a Python shell with Django configured:

.. code-block:: shell

   ./manage.py shell

To access the default database:

.. code-block:: shell

   ./manage.py dbshell

Learn more about `manage.py <https://docs.djangoproject.com/en/3.1/ref/django-admin/>`__.

System checks
^^^^^^^^^^^^^

If we decide to add `system checks <https://docs.djangoproject.com/en/3.1/topics/checks/>`__ in a ``process/checks.py`` file, we need to add this to ``apps.py``:

.. code-block:: python

   def ready(self):
       import process.checks

RabbitMQ
--------

The shortest explanation of RabbitMQ is the `AMQP 0-9-1 Model in Brief <https://www.rabbitmq.com/tutorials/amqp-concepts.html#amqp-model>`__:

-  Multiple *publishers* can publish messages to *exchanges*
-  An *exchange* routes the messages it receives to one or more *queues*, using rules called *bindings*
-  Multiple *consumers* can subscribe to one or more *queues*

We use the `pika <https://pika.readthedocs.io/en/stable/>`__ library to interact with RabbitMQ directly. We don't use Celery, because its abstractions adds inefficiencies, requiring `complex workarounds <http://blog.untrod.com/2015/03/how-celery-chord-synchronization-works.html>`__.

Documentation
~~~~~~~~~~~~~

`Full documentation <https://www.rabbitmq.com/documentation.html>`__

Here are quick links to relevant pages, if you are working on ``broker.py``.

-  `AMQP 0-9-1 Model Explained <https://www.rabbitmq.com/tutorials/amqp-concepts.html>`__
-  `Reliability Guide <https://www.rabbitmq.com/reliability.html>`__

Examples
^^^^^^^^

-  Pika examples `in documentation <https://pika.readthedocs.io/en/stable/examples.html>`__ and `on GitHub <https://github.com/pika/pika/tree/master/examples>`__

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
