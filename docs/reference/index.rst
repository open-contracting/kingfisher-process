Reference
=========

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
  The directory from which to read the files written by Kingfisher Collect. If Kingfisher Collect and Kingfisher Process share a filesystem, this will be the same value for both services.
ENABLE_CHECKER
  Whether to enable the ``checker`` worker

It is recommended to set ``REQUESTS_POOL_MAXSIZE`` to ``20``, to set the maximum number of connections to save in the `connection pool <https://urllib3.readthedocs.io/en/latest/advanced-usage.html#customizing-pool-behavior>`__ used by the `ocdsextensionregistry <https://ocdsextensionregistry.readthedocs.io/en/latest/changelog.html>`__ package. This is the same value as the `prefetch_count <https://www.rabbitmq.com/docs/consumer-prefetch>`__ used by RabbitMQ consumers.

