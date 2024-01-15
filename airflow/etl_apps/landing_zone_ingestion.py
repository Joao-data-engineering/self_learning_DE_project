from pyspark.sql import SparkSession
import utils
import os
from datetime import datetime
import pyspark.sql.functions as F
from delta.tables import *
import sys
from constants import tables_partition_key, tables_primary_key
from pyspark.sql import DataFrame

from pyspark.sql.types import StructType, StructField, IntegerType
import logging

def limit_data(df: DataFrame, table_name: str) -> DataFrame:
    if table_name == 'title_ratings':
        df = df.withColumn("numVotes", F.col("numVotes").cast(IntegerType()))
        df = df.filter(F.col("numVotes")>10000)
    elif table_name == 'title_principals':
        df = df.limit(500000)
    elif table_name == 'name_basics':
        df = df.withColumn("birthYear", F.col("birthYear").cast(IntegerType()))
    elif table_name == 'title_basics':
        df = df.withColumn("startYear", F.col("startYear").cast(IntegerType()))
    return df

def load_table_info(cache_directory: str, landing_zone_path: str) -> dict:

    """
        Load information about tables from the given directory. It lists all files in the cache directory and 
        creates a mapping of table names to their file paths in the landing zone.

        Args:
            cache_directory (str): Path to the cache directory containing table files.
            landing_zone_path (str): Path to the landing zone where the tables are located.

        Returns:
            dict: A dictionary mapping table names to their corresponding file paths.
    """

    table_names = [file_name.split('.', 1)[0] for file_name in os.listdir(cache_directory)]
    return {table_name: f'{landing_zone_path}/{table_name}' for table_name in table_names}

def process_table_data(spark: SparkSession, table_name: str, file_path: str) -> None:

    """
        Process data for a specific table. It checks if the table exists and whether it requires incremental 
        updates. Depending on these checks, it processes new or incremental data.

        Args:
            spark (SparkSession): The Spark session instance.
            table_name (str): Name of the table being processed.
            file_path (str): Path to the file containing the table's data.

        Returns:
            None
    """
    table_exists, incremental = table_needs_update(spark, table_name, file_path)

    if table_exists and not incremental:
        print(f'Table {table_name} already updated for today')
        return

    process_new_or_incremental_data(spark, table_name, file_path, table_exists = table_exists)

def table_needs_update(spark: SparkSession, table_name: str, file_path: str) -> tuple[bool, bool]:

    """
        Check if the table needs to be updated. Determines whether the table exists and if it requires incremental 
        updates based on the latest data date in metadata and the current date.

        Args:
            spark (SparkSession): The Spark session instance.
            table_name (str): Name of the table to check.
            file_path (str): Path to the file containing the table's data.

        Returns:
            tuple[bool, bool]: A tuple where the first element is a boolean indicating if the table exists, 
                                and the second element is a boolean indicating if an incremental update is needed.
    """
    table_exists = os.path.exists(file_path)

    if table_exists:
        latest_date_from_metadata = utils.get_latest_date_from_metadata(spark, table_name, stage='landing_zone')
        current_date = datetime.now().date()

        incremental = table_exists and latest_date_from_metadata != current_date
    else:
        incremental = False

    return table_exists, incremental

def process_new_or_incremental_data(spark: SparkSession, table_name: str, file_path: str, table_exists: bool) -> None:
    """
        Process new or incremental data for a table. If the table is new, it processes the entire data. If the 
        table exists, it processes incremental data.

        Args:
            spark (SparkSession): The Spark session instance.
            table_name (str): Name of the table being processed.
            file_path (str): Path to the file containing the table's data.
            table_exists (bool): Flag indicating if the table already exists.

        Returns:
            None
    """
    current_date = datetime.now().date()
    cache_file_path = f'/opt/storage/cache/{table_name}.tsv.gz'  # Use a different variable name

    logging.info(f'cache_file_path is {cache_file_path}')

    df = utils.spark_read_file(file_path = cache_file_path, file_format = 'tsv', spark = spark, header = True)

    df = limit_data(df = df, table_name = table_name)

    if table_exists is False:
        process_new_table_data(spark = spark, df = df, table_name = table_name, current_date = current_date)

    else:
        process_incremental_data(spark = spark, df = df, table_name = table_name, current_date = current_date, file_path = file_path)

def process_new_table_data(spark: SparkSession, df, table_name: str, current_date: datetime.date):
    """
        Process data for a new table. Adds the current date to the dataframe and writes it to the landing zone.

        Args:
            spark (SparkSession): The Spark session instance.
            df (DataFrame): The dataframe containing the new table's data.
            table_name (str): Name of the new table.
            current_date (datetime.date): The current date.

        Returns:
            None
    """
    df_with_date = df.withColumn("today_date", F.lit(current_date))

    partition_key = tables_partition_key[table_name]

    utils.write_dataframe(df=df_with_date, incremental=False, partition_column = partition_key, table_name = table_name, directory = f'landing_zone/{table_name}')

def process_incremental_data(spark: SparkSession, df, table_name: str, file_path: str, current_date: datetime.date):

    """
        Process incremental data for an existing table. Joins the new data with the existing table and writes 
        updates to the landing zone.

        Args:
            spark (SparkSession): The Spark session instance.
            df (DataFrame): The dataframe containing the incremental data.
            table_name (str): Name of the table being updated.
            current_date (datetime.date): The current date.
            file_path (str): Path to the file where the table's data is stored.

        Returns:
            None
    """
    logging.info(f'file_path here: {file_path}')
    current_table = DeltaTable.forPath(spark, file_path)
    df = current_table.toDF()
    updated_name_basics_df = df.withColumn("today_date", F.lit(current_date))
    if updated_name_basics_df.count() == 0:
        print("No information to add")
    else:
        primary_keys = tables_primary_key[table_name]
        if isinstance(primary_keys, list):
        # If there are multiple primary keys, concatenate them with ' AND '
            merge_condition = ' AND '.join([f"new_data.{key} = old_data.{key}" for key in primary_keys])
        else:
        # If there is a single primary key
            merge_condition = f"new_data.{primary_keys} = old_data.{primary_keys}"
        utils.write_dataframe(df = updated_name_basics_df, incremental = True, key_merge_condition = merge_condition, current_df = current_table, directory = f'landing_zone/{table_name}', table_name = table_name)

def main():
    
    """
        Main function to execute the landing zone ingestion process. Initializes the Spark session, loads table 
        information, and processes each table. Updates the metadata table with the status of each table's ingestion.

        Returns:
            None

        Raises:
            Exception: Propagates any exception that occurs during the ingestion process and logs it.
    """

    spark: SparkSession = utils.spark_session_build(session_name = 'landing_zone_ingestion')
    table_info = load_table_info('/opt/storage/cache', '/opt/storage/landing_zone')

    for table_name, file_path in table_info.items():
        logging.info(f'Ingesting {table_name} into landing_zone')
        try:
            process_table_data(spark = spark, table_name = table_name, file_path = file_path)
            utils.update_metadata_table(spark=spark, stage='landing_zone', table_name=table_name, date=datetime.now().date(), status = 'success')
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            try:
                utils.update_metadata_table(spark=spark, stage='landing_zone', table_name=table_name, date=datetime.now().date(), status = 'unsuccessful')
            except Exception as e2:
                logging.error(f"Error occurred updating unsuccessful run: {e2}")
                raise e2
            raise e

if __name__ == '__main__':

    utils.create_logging()
    main()
