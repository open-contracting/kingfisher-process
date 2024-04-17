Querying data
=============

.. seealso::

    :doc:`database`

Since most analysis is much easier to perform on compiled releases, we recommend working with compiled release collections to begin with.

Get a list of compiled release collections from a particular source
-------------------------------------------------------------------

The following query returns a list of compiled release collections downloaded with the ``canada_montreal`` spider in Kingfisher Collect:

.. code-block:: sql

   SELECT
       *
   FROM
       collection -- the `collection` table contains a list of all collections in the database
   WHERE
       source_id = 'canada_montreal' -- filter by collections from the 'canada_montreal' source.
   AND
       cached_compiled_releases_count > 0 -- filter by collections containing compiled releases
   ORDER BY
       id DESC; -- collection ids are sequential, order by newest first

To find collections from a different source, change the ``source_id`` condition. The ``source_id`` in Kingfisher Process is based on the name of the spider in Kingfisher Collect.

See the list of spiders in the `Kingfisher Collect documentation <https://kingfisher-collect.readthedocs.io/en/latest/spiders.html>`__ for a list of possible sources.

Get the JSON data stored in a collection
----------------------------------------

Use the collection ``id`` returned by the previous query to restrict your analysis to a single collection.

The following query returns the full JSON data for the first 3 compiled releases in collection 584:

.. code-block:: sql

   SELECT
       data
   FROM
       data -- raw OCDS JSON data is stored as jsonb blobs in the `data` column of the `data` table
   JOIN
       compiled_release ON data.id = compiled_release.data_id -- join to the `compiled_release` table to filter data from a specific collection
   WHERE
       collection_id = 584
   LIMIT 3;

To get data from a different collection, change the ``collection_id`` condition.

To get data from a collection containing releases or records, join to the ``release`` or ``record`` tables rather than the ``compiled_release`` table.

.. admonition:: Rendering JSON using Redash
   :class: tip

   If you are using OCP's Redash instance, you can render the results of a query as pretty printed and collapsible JSON by clicking the '+ New Visualization' button, setting the visualization type to 'table' and setting the data column to display as JSON.

Calculate the total value of completed tenders in a collection
--------------------------------------------------------------

In OCDS, the tender value is stored in the ``tender.value`` `Value <https://standard.open-contracting.org/latest/en/schema/reference/#value>`__ object which consists of a numeric ``.amount`` field and a string ``.currency`` field. The tender status is stored in the ``tender.status`` field.

To access the properties of a JSON object use the PostgreSQL ``->`` operator. The ``->`` operator takes a JSONB object and a property's name as input, and returns the property's value as a JSONB value. The ``->>`` operator returns the value as text.

The following query calculates the total value of completed tenders in collection 584:

.. code-block:: sql

   SELECT
       sum((data -> 'tender' -> 'value' -> 'amount')::numeric) AS tender_value,
       data -> 'tender' -> 'value' ->> 'currency' AS currency
   FROM
       data
   JOIN
       compiled_release ON data.id = compiled_release.data_id
   WHERE
       collection_id = 584
   AND
       data -> 'tender' ->> 'status' = 'complete'
   GROUP BY
       currency;

.. admonition:: Filtering on status fields
   :class: tip

   The ``tender``, ``awards`` and ``contracts`` objects in OCDS all have a ``.status`` field.

   Consider which statuses you want to include or exclude from your analysis; for example, you might want to exclude pending and cancelled contracts when calculating the total value of contracts for each buyer.

   The `OCDS codelist documentation <https://standard.open-contracting.org/latest/en/schema/codelists/#>`__ describes the meaning of the statuses for each object.

Calculate the top 10 buyers by award value
------------------------------------------

Details of the buyer for a contracting process in OCDS are stored in the ``parties`` `section <https://standard.open-contracting.org/latest/en/schema/reference/#parties>`__ and referenced from the ``buyer`` `OrganizationReference <https://standard.open-contracting.org/latest/en/schema/reference/#organizationreference>`__ object.

Since a single contracting process can have many awards, e.g. when divided into lots, the ``awards`` `section <https://standard.open-contracting.org/latest/en/schema/reference/#award>`__ in OCDS is an array. The award value is stored in the ``awards.value`` object.

The following query calculates the top 10 buyers by the value of awards for collection 584.

The PostgreSQL ``jsonb_array_elements`` function used in this query expands the ``awards`` array to a set of JSONB blobs, one for each award.

The ``CROSS JOIN`` in this query joins each row of the data table with each result of the ``jsonb_array_elements`` function for that row.

.. code-block:: sql

   SELECT
       data -> 'buyer' ->> 'name' AS buyer_name,
       sum((awards -> 'value' -> 'amount')::numeric) AS award_value,
       awards -> 'value' ->> 'currency' AS currency
   FROM
       data
   JOIN
       compiled_release ON data.id = compiled_release.data_id
   CROSS JOIN
       jsonb_array_elements(data -> 'awards') AS awards
   WHERE
       collection_id = 584
   AND
       (awards -> 'value' -> 'amount')::numeric > 0 -- filter out awards with no value
   AND
       awards ->> 'status' = 'active'
   GROUP BY
       buyer_name,
       currency
   ORDER BY
       award_value DESC
   LIMIT 10;

Use the `PostgreSQL documentation <https://www.postgresql.org/docs/current/functions-json.html>`__ to learn more about operators and functions for working with JSON data.

.. admonition:: Organization identifiers
   :class: tip

   For simplicity, the above query groups by the ``buyer_name`` column. Using organization names as a dimension in your analysis can be unreliable, since spellings and abbreviations of the same organization name can differ.

   OCDS recommends that publishers provide `organization identifiers <https://standard.open-contracting.org/latest/en/schema/identifiers/#organization-ids>`__ so that the legal entities involved in a contracting process can be reliably identified.

   The identifier for an organization in OCDS is stored in the ``.identifier`` field of the entry in the ``parties`` section for the organization.

Querying other collections and fields
-------------------------------------

Coverage of the OCDS schema varies by publisher.

To identify the fields needed for your analysis and how to answer them, use the `OCDS schema documentation <https://standard.open-contracting.org/latest/en/schema/release/>`__ to understand the meaning, structure and format of the fields in OCDS.

To check whether the fields needed for your analysis are available for a particular collection, you can use the `field counts table <https://kingfisher-summarize.readthedocs.io/en/latest/database.html#field-counts>`__ from Kingfisher Summarize.

To learn more, refer to `Querying data in Kingfisher Summarize documentation <https://kingfisher-summarize.readthedocs.io/en/latest/querying-data.html#querying-other-collections-and-fields>`__.
