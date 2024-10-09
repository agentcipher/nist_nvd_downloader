import dlt
from dynaconf import Dynaconf
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig
from dlt.sources.sql_database import sql_table
from typing import Dict, Any, Iterator, Optional, Tuple

from pkg_resources import resources_stream

from utils import load_model_class
import traceback
from loguru import logger
from datetime import datetime, timezone, timedelta

"""
Data Pipeline Module

This module implements a configurable data pipeline using dlt (data load tool).
It fetches data from a REST API source and processes it according to specified
configurations and data models.

The pipeline is designed to be flexible and can be adapted to different data
sources by modifying the configuration file.
"""

# Load the configuration
settings = Dynaconf(
    settings_files=['config.yaml'],
    environments=True,
    load_dotenv=True,
)


def get_last_modified_date(pipeline) -> Optional[datetime]:
    """
    Retrieves the last modified date from the most recent record in the pipeline.

    Args:
        pipeline: The DLT pipeline instance

    Returns:
        datetime: The last modified date or None if no records exist
    """
    try:
        # Use sql_table to query the destination
        with pipeline.sql_client() as client:
            sql = f"SELECT MAX(last_modified) as last_modified FROM {settings.pipeline.dataset_name} LIMIT 1"
            result = client.execute_sql(sql)

            # Extract the last_modified value from the result
            if result:
                last_modified = result[0][0]  # Assuming the first row, first column is the date

                if last_modified:
                    return last_modified

        return None

    except Exception as e:
        logger.error(f"Failed to get last modified date: {e}")
        return None

def get_incremental_params(pipeline) -> Dict[str, str]:
    """
    Determines the parameters for incremental loading based on the last modified date.

    Args:
        pipeline: The DLT pipeline instance

    Returns:
        Dict[str, str]: Dictionary of API parameters
    """
    api_params = {}

    if not settings.pipeline.get('incremental_load', False):
        logger.info("Incremental loading is disabled. Performing full load.")
        return api_params

    last_mod_date = get_last_modified_date(pipeline)
    current_time = datetime.now(timezone.utc)

    if last_mod_date:
        api_params = {
            'lastModStartDate': last_mod_date.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'lastModEndDate': current_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        }
        logger.info(f"Incremental load from {api_params['lastModStartDate']} to {api_params['lastModEndDate']}")
    else:
        logger.info("No existing data found. Performing initial load.")

    return api_params


@dlt.source(name=settings.source.name)
def data_source(pipeline):
    """
    Creates and configures a data source for the pipeline.

    This function sets up a REST API source using the configuration specified
    in the settings. It creates a resource that can fetch and process data
    from the API.

    Returns:
        function: A dlt resource function that yields processed data items.
    """
    logger.debug("Initializing data source")

    # Get incremental loading parameters
    api_params = get_incremental_params(pipeline)

    if settings.resources[0].endpoint.params:
        settings.resources[0].endpoint.params.update(api_params)
    else:
        settings.resources[0].endpoint['params']=api_params

    # Define the source using RESTAPIConfig
    source = rest_api_source(
        RESTAPIConfig(
            client=settings.client,
            resources=settings.resources,
        )
    )
    logger.info(f"Created REST API source with base URL: {settings.client.base_url}")

    # Dynamically load the data model
    try:
        columns = load_model_class(settings.models.model)
        logger.debug(f"Loaded data model: {settings.models.model}")
    except Exception as e:
        logger.error(f"Failed to load data model: {e}")
        raise

    @dlt.resource(name=settings.pipeline.dataset_name, columns=columns, **settings.resource_defaults)
    def data_resource() -> Iterator[Dict[str, Any]]:
        """
        Processes data from the API source.

        This resource function fetches data from the configured API source,
        processes it using the specified response parser, and yields individual
        data items.

        Yields:
            Dict[str, Any]: Processed data items from the API.

        Raises:
            Exception: Any exception that occurs during data fetching or processing.
        """
        resource_name = settings.resources[0].name
        logger.info(f"Processing resource: {resource_name}")

        try:
            # Get the raw data from the API
            logger.debug("Fetching data from API")
            raw_data = source.resources[resource_name]

            # Load the correct class function
            logger.debug(f"Loading response parser: {settings.models.parse_data_function}")
            response_class = load_model_class(settings.models.parse_data_function)

            # Convert raw_data iterator to list and create response object
            raw_data_list = list(raw_data)
            logger.info(f"Fetched {len(raw_data_list)} raw data items")

            response_obj = response_class(data=raw_data_list)

            # Process the data
            data_items = list(response_obj.get_data())
            logger.info(f"Processed {len(data_items)} data items")

            for index, data_item in enumerate(data_items, 1):
                if index % 1000 == 0:  # Log progress every 1000 items
                    logger.debug(f"Yielding item {index} of {len(data_items)}")
                yield data_item

        except Exception as e:
            logger.error(f'Error in data_resource: {e}')
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    return data_resource


def run_pipeline():
    """
    Executes the data pipeline.

    This function creates and runs the dlt pipeline, processes the data
    through the configured source, and logs the results.

    Returns:
        None
    """
    logger.info("Initializing pipeline")

    # Create the DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name=settings.pipeline.name,
        destination=settings.pipeline.destination,
        dataset_name=settings.pipeline.dataset_name
    )
    logger.debug(f"Created pipeline: {settings.pipeline.name}")

    # Get the data source
    source = data_source(pipeline)

    try:
        # Run the pipeline with the resource data
        logger.info("Starting pipeline execution")
        load_info = pipeline.run(source)
        logger.info("Pipeline execution completed successfully")

        # Get row counts from the last pipeline trace
        row_counts = pipeline.last_trace.last_normalize_info

        logger.info(f"Processed row counts: {row_counts}")
        logger.info(f"Load info: {load_info}")

        return load_info

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    logger.info("Starting data pipeline script")
    try:
        run_pipeline()
        logger.info("Data pipeline script completed successfully")
    except Exception as e:
        logger.error(f"Data pipeline script failed: {e}")
        raise
