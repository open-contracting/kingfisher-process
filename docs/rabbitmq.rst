Unused features
---------------

Multicast routing
~~~~~~~~~~~~~~~~~

In a direct exchange, it's possible for a queue to have multiple bindings to an exchange, in order to route messages with different routing keys to it. For now, each of our queues has a single binding, such that our exchange behaves like the `default exchange <https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-default>`__.

Topic exchanges
~~~~~~~~~~~~~~~

A `topic exchange <https://www.rabbitmq.com/tutorials/tutorial-five-python.html>`__ can be used to allow routing on multiple criteria. We don't have a clear use case for this yet.

A topic exchange could support collection-specific queues, but `priority queues <https://www.rabbitmq.com/priority.html>`__ appear to be a simpler way to prioritize collections.

Publisher confirms
~~~~~~~~~~~~~~~~~~

It's possible to `ensure message delivery <https://github.com/pika/pika/blob/master/docs/examples/blocking_publish_mandatory.rst>`__ by using `publisher confirms <https://www.rabbitmq.com/confirms.html#publisher-confirms>`__ and setting the `mandatory flag <https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish>`__.

However, for simplicity, we're using `pika <https://pika.readthedocs.io/>`__'s `BlockingConnection <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__, which would use a "publish-and-wait" strategy for publisher confirms, which is `officially discouraged <https://www.rabbitmq.com/publishers.html#publisher-confirm-strategies>`__, because it would wait for each message to be `persisted to disk <https://www.rabbitmq.com/confirms.html#when-publishes-are-confirmed>`__.

The cases that publisher confirms protect against are:

-  `pika.exceptions.UnroutableError <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html#pika.adapters.blocking_connection.BlockingChannel.basic_publish>`__: The message can't be routed to any queue.
-  `pika.exceptions.NackError <https://www.rabbitmq.com/confirms.html#server-sent-nacks>`__: An internal error occurs in the process responsible for the queue.
-  `More complex scenarios <https://www.rabbitmq.com/confirms.html#publisher-confirms-and-guaranteed-delivery>`__.

All these are unlikely. To ensure messages are routable, before publishing a message, we make sure a queue exists and is bound to the exchange such that the message goes to that queue.
