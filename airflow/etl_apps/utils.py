from pyspark.sql.types import StructType, StringType, DateType, StructField
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import requests
from datetime import datetime, date
import delta
from delta.tables import *
import os
import logging

def spark_session_build(session_name: str) -> SparkSession:

    """
        Builds and returns a SparkSession with specific configurations, in this case it uses delta table format 
        and attributes 4g of memory to the driver and workers.

        Args:
        session_name (str): Name for the Spark session.

        Returns:
        SparkSession: A configured SparkSession object.
    """

    builder = SparkSession.builder \
        .master("local[*]") \
        .appName(session_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") 
    
    spark: SparkSession = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def spark_read_file(file_path: str, file_format: str, spark: SparkSession, schema: StructType = None, header: bool = True) -> DataFrame:
    
    """
        Reads a file into a Spark DataFrame based on the specified format.

        Args:
            file_path (str): Path to the file.
            file_format (str): Format of the file (e.g., 'csv', 'delta', 'parquet').
            spark (SparkSession): An active SparkSession.
            schema (StructType, optional): Schema to apply to the DataFrame. Defaults to None.
            header (bool, optional): Indicates if the first line of the file is a header. Defaults to True.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the file.
    """

    if file_format == 'tsv':
        if schema is None:
            df: DataFrame = spark.read.csv(file_path, sep="\t", header = header, inferSchema = True)
        else:
            df: DataFrame = spark.read.schema(schema).csv(file_path, sep="\t", header = header)
    elif file_format == 'delta':
        df: DataFrame = spark.read.format("delta").load(file_path)
    elif file_format == 'parquet':
        df: DataFrame = spark.read.parquet(file_path)
    else:
        raise ValueError("File format currently not supported")
    return df


def write_dataframe(df: DataFrame, directory: str, table_name: str, incremental: bool = False, key_merge_condition: str = None, current_df: DeltaTable = None, partition_column: str = None, overwrite_partition: bool = False):
    
    """
        Writes a DataFrame to a specified directory with various options like incremental update, partitioning and partition overwrite.

        Args:
            df (DataFrame): The DataFrame to write.
            directory (str): Directory to write the DataFrame.
            table_name (str): Name of the table.
            incremental (bool, optional): Flag for incremental update. Defaults to False.
            key_merge_condition (str, optional): Condition for merging keys in incremental update. Defaults to None.
            current_df (DeltaTable, optional): Current DeltaTable for incremental updates. Defaults to None.
            partition_column (str, optional): Column to partition the data. Defaults to None.
            overwrite_partition (bool, optional): Flag to overwrite partition. Defaults to False.
    """

    if incremental == True:
        (current_df.alias('old_data')
         .merge(df.alias('new_data'), key_merge_condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print(f'Table {directory} updated sucessfully')
    elif overwrite_partition:
        df.write.format("delta").mode('overwrite').option("replaceWhere", f"{partition_column} = '{table_name}'").save(f"/opt/storage/{directory}")
        print(f'Table {directory} updated sucessfully')
    elif partition_column is not None:
        df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").partitionBy(partition_column).save(f"/opt/storage/{directory}")
        print(f'Table {directory} written sucessfully with partitioning')
    else:
        df.write.format("delta").mode('overwrite').save(f"/opt/storage/{directory}")
        print(f'Table {directory} written sucessfully')

# Function to update metadata table
def update_metadata_table(spark: SparkSession, stage: str, table_name: str, date: DateType, status: str) -> None:

    """
        Updates the metadata table with new information.

        Args:
            spark (SparkSession): An active SparkSession.
            stage (str): The stage of processing (e.g., 'cache', 'landing_zone').
            table_name (str): Name of the table being updated.
            date (DateType): The date of the update.
            status (str): The status of the update (e.g., 'success', 'failure').
    """

    metadata_table_path = '/opt/storage/metadata'

    # Load the existing metadata table
    metadata_df = spark.read.format('delta').load(metadata_table_path).filter((F.col('table_name') == table_name) & (F.col('stage') == stage))
    metadata_df.show()
    new_row = spark.createDataFrame([(stage, table_name, date,status)], ["stage", "table_name", "date","status"])

    if metadata_df.count() == 0:
        metadata_df: DataFrame = spark.read.format('delta').load(metadata_table_path)
        updated_metadata_df: DataFrame = metadata_df.union(new_row)
        write_dataframe(df = updated_metadata_df, directory = 'metadata', partition_column = 'table_name', table_name = 'metadata')

    else:
        updated_metadata_df: DataFrame = metadata_df.union(new_row)
        # Overwrite the existing metadata table with the updated DataFrame
        write_dataframe(df = updated_metadata_df, directory = 'metadata', table_name = table_name, overwrite_partition = True, partition_column = 'table_name')

    print(f"Metadata table updated with {table_name}.")


def check_or_create_metadata_table(spark: SparkSession) -> None:

    """
        Checks if a metadata table exists; if not, it creates one.

        Args:
            spark (SparkSession): An active SparkSession.
    """

    metadata_table_path: str = '/opt/storage/metadata'  # Path for the metadata table

    # Check if metadata table exists
    if not os.path.exists(metadata_table_path):
        # Define the schema for the metadata table
        schema: StringType = StructType([
            StructField("stage", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("date", DateType(), True),
            StructField("status", StringType(), True)
        ])

        # Create an empty DataFrame with the defined schema
        metadata_df: DataFrame = spark.createDataFrame([], schema)
        # Write the DataFrame to a Parquet file
        write_dataframe(df = metadata_df, directory = 'metadata', table_name = 'metadata')
        print("Metadata table created")
    else:
        print("Metadata table already exists")

def download_file(url: str, file_path: str) -> None:

    """
        Downloads a file from a given URL and saves it to a specified path.

        Args:
            url (str): The URL of the file to be downloaded.
            file_path (str): The file_path used to save the file
    """
    
    try:
        response: requests.Response = requests.get(url)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code

        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"File saved as {file_path}")
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as e:
        print(f"Error occurred: {e}")

def get_latest_date_from_metadata(spark: SparkSession, table_name: str, stage: str) -> datetime.date:

    """
        Retrieves the latest date for a specific table and stage from the metadata table.

        Args:
            spark (SparkSession): An active SparkSession.
            table_name (str): The name of the table to query.
            stage (str): The stage of the process (e.g., 'cache', 'landing_zone').

        Returns:
            datetime.date: The latest date for the specified table and stage.
    """

    return spark_read_file(spark = spark, file_format='delta', file_path=f'/opt/storage/metadata').filter(
        (F.col('stage') == stage) &
        (F.col('table_name') == table_name) &
        (F.col('status') == 'success')
    ).select(F.max('date')).collect()[0][0]

def create_logging():

    """
        Configures the logging settings for the application.

        This function sets up the logging format and level.
    """

    return logging.basicConfig(level=logging.INFO, format='line:%(lineno)d level:%(levelname)s msg:%(message)s datetime: %(asctime)s', datefmt='%d-%b-%y %H:%M:%S')

def update_postgresql(df: DataFrame, dbtable: str) -> None:

    """
        Writes a DataFrame to a PostgreSQL database table.

        Args:
            df (DataFrame): The DataFrame to be written to the database.
            dbtable (str): The name of the target database table.

        Note:
            This function is configured for a specific PostgreSQL instance and requires relevant JDBC URL, driver, and credentials.
    """

    df.write.format('jdbc').options(
        url='jdbc:postgresql://imdb_postgres:5432/imdb',
        driver='org.postgresql.Driver',
        dbtable=dbtable,
        user='admin',
        password='password'
    ).mode('overwrite').save()