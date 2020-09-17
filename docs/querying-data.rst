Querying data
=============

Before you begin, read the :doc:`index` and :doc:`data-model` pages to learn about how data is stored in Kingfisher Process.

Since most analysis is much easier to perform on compiled releases, we recommend working with compiled release collections to begin with.

Get a list of compiled release collections from a particular source
-------------------------------------------------------------------

The following query returns a list of compiled release collections downloaded from the `State Procurement Agency of Georgia's OCDS API <https://odapi.spa.ge/>`__:

.. code-block:: sql

  select
      *
  from
      collection --the `collection` table contains a list of all collections in the database
  where
      source_id = 'georgia_releases' --filter by collections from the 'georgia_releases' source.
  and
      cached_compiled_releases_count > 0 --filter by collections containing compiled releases
  order by
      id desc; --collection ids are sequential, order by newest first

To find collections from a different source, change the ``source_id`` parameter. The ``source_id`` in Kingfisher Process is based on the name of the spider in Kingfisher Scrape.

See the list of spiders in the `Kingfisher Scrape Github Repository <https://github.com/open-contracting/kingfisher-scrape/tree/master/kingfisher_scrapy/spiders>`__ for a list of possible sources. Each ``.py`` file is a spider, and the part before the ``.py`` extension is the ``source_id``.

Get the JSON data stored in a collection
----------------------------------------

Use the collection ``id`` returned by the previous query to restrict your analysis to a single collection.

The following query returns the full JSON data for the first 3 compiled releases in collection 584:

.. code-block:: sql

  select
      data
  from
      data --raw OCDS JSON data is stored as jsonb blobs in the `data` column of the `data` table
  join
      compiled_release
  on
    data.id = compiled_release.data_id -- join to the `compiled_release` table to filter data from a specific collection
  where
      collection_id = 584
  limit 3;

To get data from a different collection, change the ``collection_id`` parameter.

To get data from a collection containing releases or records, join to the ``release`` or ``record`` tables rather than the ``compiled_release`` table.

.. tip:: Rendering JSON using Redash

  If you are using Redash, you can render the results of a query as pretty printed and collapsible JSON by clicking the '+ New Visualization' button, setting the visualization type to 'table' and setting the data column to display as JSON.

Calculate the total value of completed tenders in a collection
--------------------------------------------------------------

In OCDS, the tender value is stored in the ``tender.value`` object which consists of a numeric ``.amount`` property with an associated ``.currency``. The tender status is stored in the ``tender.status`` field.

To access the properties of a JSON object use the PostgreSQL ``->`` operator. The ``->`` operator takes a jsonb object and the name of a key as inputs and returns the value of the key as a jsonb value. The ``->>`` operator returns the value as a string.


The following query calculates the total value of completed tenders in collection 584:

.. code-block:: sql

  select
      sum((data -> 'tender' -> 'value' -> 'amount')::numeric) as tender_value,
      data -> 'tender' -> 'value' ->> 'currency' as currency
  from
      data
  join
      compiled_release
  on
      data.id = compiled_release.data_id
  where
      collection_id = 584
  and
      data -> 'tender' ->> 'status' = 'complete'
  group by
      currency;

.. tip:: Filtering on status fields

  The ``tender``, ``award`` and ``contract`` objects in OCDS all have a ``.status`` property.

  Consider which statuses you want to include or exclude from your analysis, for example you might wish to exclude pending and cancelled contracts when calculating the total value of contracts for each buyer.

  The `OCDS codelist documentation <https://standard.open-contracting.org/latest/en/schema/codelists/#>`__ describes the meaning of the statuses for each object.

Calculate the top 10 buyers by award value
------------------------------------------

Details of the buyer for a contracting process in OCDS are stored in the ``parties`` section and referenced from the ``buyer`` object.

Since a single contracting process can have many awards, for example where lots are used, the ``awards`` section in OCDS is an array. The award value is stored in the ``awards.value`` object.

The following query calculates the top 10 buyers by the value of awards for collection 584.

The PostgreSQL ``jsonb_array_elements`` function used in this query expands the ``awards`` array to a set of jsonb blobs, one for each award.

The ``cross join`` in this query acts like an inner join between each row of the data table and the results of the ``jsonb_array_elements`` function for that row.

.. code-block:: sql

  select
      data -> 'buyer' ->> 'name' as buyer_name,
      sum((awards -> 'value' -> 'amount')::numeric) as award_value,
      awards -> 'value' ->> 'currency' as currency
  from
      data
  join
      compiled_release on data.id = compiled_release.data_id
  cross join
      jsonb_array_elements(data -> 'awards') as awards
  where
      collection_id = 584
  and
      (awards -> 'value' -> 'amount')::numeric > 0 --filter out awards with no value
  and
      awards ->> 'status' = 'active'
  group by
      buyer_name,
      currency
  order by
      award_value desc
  limit
      10;

Use the `PostgreSQL documentation <https://www.postgresql.org/docs/current/functions-json.html>`__ to learn more about operators and functions for working with JSON data.

.. tip:: Organization identifiers

  For simplicity, the above query groups by the ``buyer.name`` field. Using organization names as a dimension in your analysis can be unreliable, since spellings and abbreviations of the same organization name can differ.

  OCDS recommends that publishers provide `organization identifiers <https://standard.open-contracting.org/latest/en/schema/identifiers/#organization-ids>`__ so that the legal entities involved in a contracting process can be reliably identified.

  The identifier for an organization in OCDS is stored in the ``.identifier`` property of the entry in the ``parties`` section for the organization.

Querying other collections and fields
-------------------------------------

Coverage of the OCDS schema varies by publisher.

To identify the fields needed for your analysis and how to answer them, use the `OCDS schema documentation <https://standard.open-contracting.org/latest/en/schema/release/>`__ to understand the meaning, structure and format of the fields in OCDS.

To check whether the fields needed for your analysis are available for a particular collection, you can use the `field counts table<https://kingfisher-views.readthedocs.io/en/latest/database.html#field-counts>`__ from Kingfisher Views.

To learn more, refer to the `querying data in Kingfisher Views documentation<https://kingfisher-views.readthedocs.io/en/latest/querying-data.html#querying-other-collections-and-fields>`__.
