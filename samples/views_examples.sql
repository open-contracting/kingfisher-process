-- Example queries: Analysing summary OCDS data in Kingfisher views
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


--get a summary of the first 3 contracting processes in collection 584 (Georgia)
  --part-flattened, part-normalised and aggregated summary data is stored in the `views` schema, for a list of summary tables see https://kingfisher-views.readthedocs.io/en/latest/view-reference.html
select
    *
from
    views.release_summary --the `.release_summary` table contains a top level summary of each contracting process
where
    collection_id = 584
limit 3;


--calculate the total value of completed tenders in the dataset
select
    sum(tender_value_amount),
    tender_value_currency --return the currency of the tender value, values in OCDS have an amount and a currency, as datasets may contain values in multiple currencies
from
    views.tender_summary --summary data on tenders is stored in the views.tender_summary table, see https://kingfisher-views.readthedocs.io/en/latest/view-reference.html#tender-summary
where
    collection_id = 584 --use the id returned by the previous query for the most recent georgia collection
and
    tender_status = 'complete' --filter on completed tenders, for a list of possible tender statuses see https://standard.open-contracting.org/latest/en/schema/codelists/#tender-status
group by
    tender_value_currency;


--calculate the top 10 buyers by award value.
  --summary data on buyers and awards is stored in different tables
  --to join summary tables, use the `id` column which uniquely identifies a compiled release (a contracting process), see https://kingfisher-views.readthedocs.io/en/latest/views.html#structure
  --most summary tables include a column containing jsonb blobs of the source data for the summary
select
    buyer -> 'name' as buyer_name, --extract the buyer name from the `buyer` jsonb blob, since the buyer_summary table doesn't include a column for the buyer name
    sum(award_value_amount) as award_amount,
    award_value_currency
from
    views.awards_summary --summary data on awards is stored in the `views.awards_summary` table
join
    views.buyer_summary on views.awards_summary.id = views.buyer_summary.id --summary data on buyers is stored in the `views.buyer_summary` table, join on the `id` column
where
    views.awards_summary.collection_id = 584
and
    views.awards_summary.award_value_amount > 0 --filter out awards with no value
and
    views.awards_summary.award_status = 'active'
group by
    buyer_name,
    award_value_currency
order by
    award_amount desc
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
