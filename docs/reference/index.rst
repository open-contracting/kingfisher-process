Reference
=========

API
---

.. toctree::

   redoc
   swagger-ui

To view the API's documentation in development, :ref:`run the server<development>` and open http://127.0.0.1:8000/api/schema/swagger-ui/ or http://127.0.0.1:8000/api/schema/redoc/.

The API is used for managing collections (see `Kingfisher Collect <https://kingfisher-collect.readthedocs.io/en/latest/kingfisher_process.html>`__ and ``KINGFISHER_PROCESS_URL`` in the `Data Registry <https://ocp-data-registry.readthedocs.io/en/latest/reference/>`__).

.. _environment-variables:

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
  The directory from which to **read** the files written by Kingfisher Collect. If Kingfisher Collect and Kingfisher Process share a filesystem, this will be the same value for both services.
ENABLE_CHECKER
  Whether to enable the ``checker`` worker

It is recommended to set ``REQUESTS_POOL_MAXSIZE`` to ``20``, to set the maximum number of connections to save in the `connection pool <https://urllib3.readthedocs.io/en/latest/advanced-usage.html#customizing-pool-behavior>`__ used by the `ocdsextensionregistry <https://ocdsextensionregistry.readthedocs.io/en/latest/changelog.html>`__ package. This is the same value as the `prefetch_count <https://www.rabbitmq.com/docs/consumer-prefetch>`__ used by RabbitMQ consumers.

Message routing
---------------

.. seealso::

   `RabbitMQ design decisions <https://ocp-software-handbook.readthedocs.io/en/latest/services/rabbitmq.html#design-decisions>`__

In each :doc:`worker and command<../cli>`, the queue name and the routing key of published messages (with one exception) is set by a ``routing_key`` variable. The binding keys are set by a ``consume_routing_keys`` variable. Queue names and routing keys are prefixed by the exchange name, set by the ``RABBIT_EXCHANGE_NAME`` :ref:`environment variable<environment-variables>`.

.. list-table::
   :header-rows: 1

   * - Actor
     - Consumer routing keys (input)
     - Publisher routing keys (output)
     - Processing step
   * - ``load`` command
     - N/A
     - ``loader`` for **each** collection file
     - Create ``LOAD`` for **each** collection file
   * - ``addfiles`` command
     - N/A
     - ``loader`` for **each** collection file
     - Create ``LOAD`` for **each** collection file
   * - ``addchecks`` command
     - N/A
     - ``addchecks`` for **each** collection file with missing checks
     - Create ``CHECK`` for **each** collection file
   * - ``closecollection`` command
     - N/A
     - ``collection_closed`` for the original and derived collections
     - N/A
   * - ``close_collection`` API
     - N/A
     - ``collection_closed`` for the original and derived collections
     - N/A
   * - ``wipe_collection`` API
     - N/A
     - ``wiper`` for the collection
     - N/A
   * - ``api_loader`` worker
     - ``api``
     - ``api_loader`` for the collection file
     - Create ``LOAD`` for the collection file
   * - ``file_worker`` worker
     - -  ``api_loader``
       -  ``loader``
     - ``file_worker`` for the collection file in the original and upgraded collections
     - -  Delete ``LOAD`` for the collection file
       -  Create ``CHECK`` for the collection file in the original and upgraded collections, if the ``ENABLE_CHECKER`` environment variable is set
   * - ``checker`` worker
     - -  ``file_worker``
       -  ``addchecks``
     - ``checker`` for the collection file
     - Delete ``CHECK`` for the collection file
   * - ``compiler`` worker
     - -  ``file_worker``
       -  ``collection_closed``
     - -  ``compiler_record`` for **each** OCID among records in the collection file
       -  ``compiler_release`` for **each** OCID among releases in the entire collection
     - -  For release packages, do nothing if a ``LOAD`` remains
       -  Create ``COMPILE`` for **each** OCID
   * - ``record_compiler`` worker
     - ``compiler_record``
     - ``record_compiler`` for the OCID
     - Delete ``COMPILE`` for the OCID
   * - ``release_compiler`` worker
     - ``compiler_release``
     - ``release_compiler`` for the OCID
     - Delete ``COMPILE`` for the OCID
   * - ``finisher`` worker
     - -  ``file_worker``
       -  ``checker``
       -  ``record_compiler``
       -  ``release_compiler``
       -  ``collection_closed``
     - N/A
     - Do nothing if a step remains
   * - ``wiper`` worker
     - ``wiper``
     - N/A
     - N/A
