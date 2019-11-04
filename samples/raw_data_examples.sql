-- Example queries: Analysing raw OCDS data
  -- Review the Kingfisher documentation before beginning: https://ocdskingfisher.readthedocs.io/en/latest/
  -- Highlight each query below and click 'Execute selected' or hit Ctrl+Enter to run it
  -- Export results to CSV by clicking the '...' button at the bottom of the screen, or create a visualization in Redash using the '+New Visualization' button


--get a list of datasets from Georgia, with the newest first
  --data in Kingfisher is organized into collections, where a collection represents one run of a scraper for a specific data source
    --for documentation of the kingfisher data model, see https://kingfisher-process.readthedocs.io/en/latest/data-model.html
  --OCDS data is published using releases or records which Kingfisher automatically transforms to create compiled release collections, containing the latest version of the data about each contracting process.
  --most analysis is much easier to perform on compiled releases, so we recommend working with compiled release collections to begin with
    --for documentation of the OCDS releases and records model, see https://standard.open-contracting.org/latest/en/getting_started/releases_and_records/
select
    *
from
    collection --the `collection` table contains a list of all collections in the database
where
    source_id = 'georgia_releases' --filter by collections from the 'georgia_releases' source. See https://kingfisher-scrape.readthedocs.io/en/latest/sources.html for a list of sources
and
    cached_compiled_releases_count > 0 --filter by collections containing compiled releases
order by
    id desc;


-- get the raw JSON data for the first 3 contracting processes in collection 584 (Georgia)
  -- to pretty print the JSON and add make the tree collapsible, click the '+ New Visualization' button, set the visualization type to 'table' and set the data column to display as JSON
select
    data
from
    data --raw OCDS JSON data is stored as jsonb blobs in the `data` column of the `data` table
join
    compiled_release_with_collection on data.id = compiled_release_with_collection.data_id -- join to the `compiled_release_with_collection` table to filter data from a specific collection
where
    collection_id = 584
limit 3;


--calculate the total value of completed tenders in the dataset
  --the '->' operator takes a jsonb object and the name of a key as inputs and returns the value of the key as a jsonb value, the '->>' operator returns the value as a string
  --for documentation of the postgresql JSON functions and operators, see https://www.postgresql.org/docs/current/functions-json.html
  --for documentation of the structure and format of the fields in OCDS data, refer to https://standard.open-contracting.org/latest/en/schema/release/
  --for documentation of codelist values to filter on, refer to https://standard.open-contracting.org/latest/en/schema/codelists/
select
    sum((data -> 'tender' -> 'value' -> 'amount')::numeric) as tender_value, --return the value of the `tender.value.amount` property as jsonb, cast to a numeric value and sum
    data -> 'tender' -> 'value' ->> 'currency' as currency --return the currency of the tender value as a string, values in OCDS have an amount and a currency, as datasets may contain values in multiple currencies
from
    data
join
    compiled_release_with_collection on data.id = compiled_release_with_collection.data_id
where
    collection_id = 584
and
    data -> 'tender' ->> 'status' = 'complete' --filter on completed tenders, for a list of possible tender statuses see https://standard.open-contracting.org/latest/en/schema/codelists/#tender-status
group by
    currency;


--calculate the top 10 buyers by award value
  --the awards section in OCDS is an array because there can be many awards for one contracting process
  --the jsonb_array_elements function in this query expands the awards array to a set of jsonb blobs, one for each award
  --the cross join in this query acts like an inner join between each row of the data table and the awards from that row
select
    data -> 'buyer' -> 'name' as buyer_name,
    sum((awards -> 'value' -> 'amount')::numeric) as award_value,
    awards -> 'value' ->> 'currency' as currency
from
    data
join
    compiled_release_with_collection on data.id = compiled_release_with_collection.data_id
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


--want to try these queries on data from a different publisher?
  --coverage of the OCDS schema varies by publisher
  --the `views.field_counts` table contains a list of the fields included in each collection and how frequently they occur
  --use this table to check if the fields you are interested in are available
select
    *
from
    views.field_counts
where
    collection_id = 584
