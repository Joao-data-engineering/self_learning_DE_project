import os
import utils
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from constants import table_info
import logging

def process_table_data(spark: SparkSession, table_name: str, file_path: str, url: str) -> None:
    """
        Processes data for a specified table. If the data file doesn't exist or if new data should be ingested,
        it calls the function to ingest new data. Otherwise, it prints that no updated data is available.

        Args:
            spark (SparkSession): The Spark session to use.
            table_name (str): The name of the table to process.
            file_path (str): The file path where the data is or will be stored.
            url (str): The URL to download the data from if needed.

        Returns:
            None
    """
    if os.path.exists(file_path) is False or should_ingest_new_data(spark, table_name):
        ingest_new_data(file_path, url)
    else:
        print(f'No updated data for {table_name}')

def should_ingest_new_data(spark: SparkSession, table_name: str) -> bool:
    """
        Determines whether new data should be ingested for a specified table.
        This is based on comparing the date of the latest data in the cache with the current date.

        Args:
            spark (SparkSession): The Spark session to use.
            table_name (str): The name of the table to check.

        Returns:
            bool: True if new data should be ingested, False otherwise.
    """
    latest_data_date = utils.get_latest_date_from_metadata(spark, table_name, stage = 'cache')
    if latest_data_date is None:
        return True
    else:
        current_date = datetime.now().date()
        logging.info(f'latest_data_date: {latest_data_date}, latest_data_date != current_date: {latest_data_date != current_date}')
        return latest_data_date != current_date

def ingest_new_data(file_path: str, url: str) -> None:
    """
        Ingest new data for a table. Downloads the data from the provided URL and saves it to the specified file path.

        Args:
            file_path (str): Local path where the data file will be downloaded.
            url (str): URL to download the data from.

        Returns:
            None
    """
    utils.download_file(url=url, file_path=file_path)
    logging.info(f'Ingested url {url} into file_path {file_path}') 

def main() -> None:
    """
        Main function to execute the ETL ingestion process from the imdb website. Initializes the Spark session, checks or creates the 
        metadata table, and processes data for each table defined in the `table_info` dictionary. It also updates 
        the metadata table with the status of the ingestion process.

        Returns:
            None

        Raises:
            Exception: Propagates any exception that occurs during the ingestion process and logs it.
    """
    spark: SparkSession = utils.spark_session_build(session_name = 'etl_ingestion')
        
    utils.check_or_create_metadata_table(spark = spark)

    for table_name, (file_path, url) in table_info.items():

        logging.info(f'Ingesting table: {table_name}') 

        try:
            process_table_data(spark, table_name, file_path, url)
            utils.update_metadata_table(spark=spark, stage='cache', table_name=table_name, date=datetime.now().date(), status = 'success')
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            utils.update_metadata_table(spark=spark, stage='cache', table_name=table_name, date=datetime.now().date(), status = 'unsuccessful')
            raise e

if __name__ == '__main__':
    
    utils.create_logging()
    main()