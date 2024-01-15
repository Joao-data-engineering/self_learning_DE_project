from pyspark.sql import SparkSession
import utils
from pyspark.sql import DataFrame
import logging

def main():
    """
        Main function to update etl pipeline status in PostgreSQL.

        The function performs the following operations:
        - Creates a Spark session.
        - Reads a DataFrame from a specified file path using Delta format.
        - Updates a PostgreSQL table with the data from the DataFrame.
        
        If an exception occurs during these operations, it logs the error and re-raises the exception.

        Exceptions:
        - Raises any exception that occurs during the execution of the ETL pipeline,
        and logs the error message.
    """

    try:
        spark: SparkSession = utils.spark_session_build(session_name = 'write_etl_pipeline_status')
        df: DataFrame = utils.spark_read_file(file_path = '/opt/storage/metadata', file_format = 'delta', spark = spark)
        utils.update_postgresql(df = df, dbtable = 'etl_pipeline_status')
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise e

if __name__ == "__main__":
    utils.create_logging()
    main()