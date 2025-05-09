import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
import logging
import sys
import os

os.environ['EXTRACT__WORKERS'] = '12'
os.environ['NORMALIZE__WORKERS'] = '4'
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '5000'

# Create a logger
logger = logging.getLogger('dlt')

client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=HeaderLinkPaginator(links_next_key="next")
)

def limit_pages(paginator, resource_name, limit=None):
    """Helper function to limit the number of pages returned from pagination

    If limit is None, returns all pages from the paginator.
    """
    page_count = 0
    for page in paginator:
        logger.info(f"Retrieved page {page_count + 1} for {resource_name}")
        yield page
        page_count += 1
        # Only check limit if it's not None
        if limit is not None and page_count >= limit:
            logger.info(f"Reached page limit of {limit} for {resource_name}")
            break

    logger.info(f"Total pages processed for {resource_name}: {page_count}")

@dlt.resource(table_name="customers",  parallelized=True)
def get_customers():
    logger.info("Starting extraction of customers data")
    paginator = client.paginate("customers", params={"page":1, "page_size":100})
    for page in paginator:
        yield page
    logger.info("Completed extraction of customers data")

@dlt.resource(table_name="orders",  parallelized=True)
def get_orders():
    logger.info("Starting extraction of orders data")
    paginator = client.paginate("orders", params={"page":1, "page_size":500, "start_date" : "2017-08-01T00:00:00"})
    for page in paginator:
      yield page
    logger.info("Completed extraction of orders data")

@dlt.resource(table_name="products",  parallelized=True)
def get_products():
    logger.info("Starting extraction of products data")
    paginator = client.paginate("products", params={"page":1, "page_size":100})
    for page in paginator:
        yield page
    logger.info("Completed extraction of products data")

@dlt.source
def jaffle_shop_source():
    logger.info("Initializing jaffle shop data source")
    return get_customers, get_orders, get_products

def main():
    try:
        pipeline = dlt.pipeline(
            pipeline_name="jaffle_shop_pipeline_v3",
            destination="duckdb",
            dataset_name="jaffle_shop",
            progress="log"
        )


        load_info = pipeline.run(jaffle_shop_source())

        print(f"{pipeline.last_trace}")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()