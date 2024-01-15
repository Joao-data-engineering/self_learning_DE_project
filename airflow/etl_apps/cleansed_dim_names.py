from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ShortType, StringType
from pyspark.sql.functions import col
import utils
from delta.tables import *
from datetime import datetime
import logging

def cast(df: DataFrame) -> DataFrame:

    """
        Casts the columns of name basics to specific data types.

        This function modifies the data types of various columns in the input DataFrame:
        - 'nconst', 'primaryName', 'primaryProfession', and 'knownForTitles' are cast to StringType.
        - 'birthYear' and 'deathYear' are cast to ShortType.

        Args:
            df (DataFrame): A Spark DataFrame containing name basics data.

        Returns:
            DataFrame: The transformed DataFrame with columns cast to specified data types.
    """
    
    df_casted = df.withColumn("nconst", col("nconst").cast(StringType())) \
              .withColumn("primaryName", col("primaryName").cast(StringType())) \
              .withColumn("birthYear", col("birthYear").cast(ShortType())) \
              .withColumn("deathYear", col("deathYear").cast(ShortType())) \
              .withColumn("primaryProfession", col("primaryProfession").cast(StringType())) \
              .withColumn("knownForTitles", col("knownForTitles").cast(StringType()))
    
    return df_casted

def transformation(df: DataFrame) -> DataFrame:

    """
        Transforms name basics by adding, modifying, and renaming columns.

        The function performs the following operations:
        - Adds a 'is_alive' column, which is True if 'birthYear' is not null and 'deathYear' is null, False if both are not null, and None otherwise.
        - Drops 'birthYear' and 'deathYear' columns.
        - Replaces '\\N' with None in 'primaryProfession' and 'knownForTitles' columns.
        - Splits 'primaryProfession' and 'knownForTitles' into arrays by commas.
        - Renames columns based on a dictionary mapping ('primaryName' to 'name', 'primaryProfession' to 'profession', 'knownForTitles' to 'known_for_titles').

        Args:
            df (DataFrame): A Spark DataFrame to be transformed.

        Returns:
            DataFrame: The transformed DataFrame with added, modified, and renamed columns.
    """

    is_alive_col: col = F.when(
        (F.col("birthYear").isNotNull()) & (F.col("deathYear").isNotNull()), False
    ).when((F.col("birthYear").isNotNull()) & (F.col("deathYear").isNull()), True).otherwise(None)
    df: DataFrame = df.withColumn('is_alive', is_alive_col)

    df: DataFrame = df.drop('birthYear','deathYear')
    df: DataFrame = df.withColumn('primaryProfession', F.when(F.col('primaryProfession') == '\\N', F.lit(None)).otherwise(F.col('primaryProfession'))) 
    df: DataFrame = df.withColumn('knownForTitles', F.when(F.col('knownForTitles') == '\\N', F.lit(None)).otherwise(F.col('knownForTitles'))) 
    df: DataFrame = df.withColumn("primaryProfession", F.split(F.trim(df["primaryProfession"]), "\\s*,\\s*"))
    df: DataFrame = df.withColumn("knownForTitles", F.split(F.trim(df["knownForTitles"]), "\\s*,\\s*"))

    dict_new_column_names: dict = {'primaryName':'name','primaryProfession':'profession','knownForTitles':'known_for_titles'}
    for column in dict_new_column_names.keys():
        df: DataFrame = df.withColumnRenamed(column,dict_new_column_names[column])

    return df

def main():

    """
        Main function to execute the ETL process for the 'dim_names' dimension table.

        This function performs the following operations:
        - Initializes a Spark session.
        - Reads data from a specified file path, applying a filter on 'birthYear'.
        - Transforms the data by casting and applying transformations.
        - Displays a preview of the transformed data.
        - Writes the transformed data to a specified directory with the given table name.
        - Updates the metadata table to reflect the status of the ETL process.

        In case of an exception, it logs the error, attempts to update the metadata table with an 'unsuccessful' status,
        and re-raises the exception.

        Raises:
            Exception: Propagates any exception that occurs during the ETL process and logs it.
    """

    spark: SparkSession = utils.spark_session_build(session_name = 'etl_dim_names')
    file_path: str = "/opt/storage/landing_zone/name_basics"

    try:
        df_name_basics: DataFrame = utils.spark_read_file(file_path = file_path, file_format = 'delta', spark = spark).filter((F.col('birthYear')>1960) & (F.col('birthYear')<2010)) #filtering bacause of technical constraints
        df_name_basics: DataFrame = cast(df = df_name_basics)
        df_name_basics: DataFrame = transformation(df = df_name_basics)
        df_name_basics.show()
        utils.write_dataframe(df = df_name_basics, directory = 'cleansed_zone/dim_names', table_name = 'dim_names')
        utils.update_metadata_table(spark=spark, stage='cleansed_dim_names', table_name='dim_names', date=datetime.now().date(), status = 'success')
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        try: 
            utils.update_metadata_table(spark=spark, stage='cleansed_dim_names', table_name='dim_names', date=datetime.now().date(), status = 'unsuccessful')
        except Exception as e2:
            logging.error(f"Error occurred updating unsuccessful run: {e2}")
            raise e2
        raise e

if __name__ == "__main__":

    utils.create_logging()
    main()




