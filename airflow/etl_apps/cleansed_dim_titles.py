from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, ShortType, StringType
import utils
from delta.tables import *
from datetime import datetime
import logging

def cast(df: DataFrame) -> DataFrame:

    """
        Casts the columns of title basics to specific data types.

        This function modifies the data types of various columns in the input DataFrame:
        - 'tconst', 'titleType', 'primaryTitle', 'originalTitle', and 'genres' are cast to StringType.
        - 'isAdult', 'startYear', 'endYear', and 'runtimeMinutes' are cast to their respective numerical types.

        Args:
            df (DataFrame): A Spark DataFrame containing title basics data.

        Returns:
            DataFrame: The transformed DataFrame with columns cast to specified data types.
    """
    
    df_casted = df.withColumn("tconst", F.col("tconst").cast(StringType())) \
              .withColumn("titleType", F.col("titleType").cast(StringType())) \
              .withColumn("primaryTitle", F.col("primaryTitle").cast(StringType())) \
              .withColumn("originalTitle", F.col("originalTitle").cast(StringType())) \
              .withColumn("isAdult", F.col("isAdult").cast(IntegerType())) \
              .withColumn("startYear", F.col("startYear").cast(ShortType())) \
              .withColumn("endYear", F.col("endYear").cast(ShortType())) \
              .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast(ShortType())) \
              .withColumn("genres", F.col("genres").cast(StringType()))

    
    return df_casted

def transformation(df: DataFrame) -> DataFrame:
    """
        Transforms title basics by modifying, renaming, and applying conditional operations to columns.

        This function performs the following operations on the input DataFrame:
        - Drops the 'runtimeMinutes' column.
        - Replaces '\\N' with None in the 'genres' column.
        - Splits the 'genres' column into arrays by commas.
        - Converts the 'isAdult' column values: '0' becomes False, and any other value becomes True.
        - Renames columns based on a dictionary mapping ('titleType' to 'type', 'primaryTitle' to 'primary_title', 'originalTitle' to 'original_title', 'isAdult' to 'is_adult', 'startYear' to 'start_year', 'endYear' to 'end_year').

        Args:
            df (DataFrame): A Spark DataFrame to be transformed.

        Returns:
            DataFrame: The transformed DataFrame with modified columns and new column names.
    """

    df: DataFrame = df.drop('runtimeMinutes')
    df: DataFrame = df.withColumn('genres', F.when(F.col('genres') == '\\N', F.lit(None)).otherwise(F.col('genres'))) 
    df: DataFrame = df.withColumn("genres", F.split(F.trim(df["genres"]), "\\s*,\\s*"))
    df: DataFrame = df.withColumn('isAdult', F.when(F.col('isAdult') == '0', F.lit(False)).otherwise(F.lit(True))) 

    dict_new_column_names: dict = {'titleType':'type','primaryTitle':'primary_title','originalTitle':'original_title','isAdult':'is_adult','startYear':'start_year','endYear':'end_year'}
    for column in dict_new_column_names.keys():
        df_dim_titles: DataFrame = df.withColumnRenamed(column,dict_new_column_names[column])

    return df_dim_titles

def main():
    """
        Main function to execute the ETL process for the 'dim_titles' dimension table.

        This function performs the following operations:
        - Initializes a Spark session.
        - Reads data from a specified file path, applying a filter on 'startYear'.
        - Transforms the data by casting and applying transformations.
        - Displays a preview of the transformed data.
        - Writes the transformed data to a specified directory with the given table name.
        - Updates the metadata table to reflect the status of the ETL process.

        In case of an exception, it logs the error, attempts to update the metadata table with an 'unsuccessful' status,
        and re-raises the exception.

        Raises:
            Exception: Propagates any exception that occurs during the ETL process and logs it.
    """
    spark: SparkSession = utils.spark_session_build(session_name = 'etl_dim_titles')
    file_path: str = "/opt/storage/landing_zone/title_basics"

    try:
        df_title_basics: DataFrame = utils.spark_read_file(file_path = file_path, file_format = 'delta', spark = spark).filter((F.col('startYear')>1960) & (F.col('startYear')<2020))
        df_title_basics: DataFrame = cast(df_title_basics)
        df_title_basics: DataFrame = transformation(df = df_title_basics)
        df_title_basics.show()
        utils.write_dataframe(df = df_title_basics, directory = 'cleansed_zone/dim_titles', table_name = 'dim_titles')
        utils.update_metadata_table(spark=spark, stage='cleansed_dim_titles', table_name='dim_titles', date=datetime.now().date(), status = 'success')
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        try:
            utils.update_metadata_table(spark=spark, stage='cleansed_dim_titles', table_name='dim_titles', date=datetime.now().date(), status = 'unsuccessful')
        except Exception as e2:
                logging.error(f"Error occurred updating unsuccessful run: {e2}")
                raise e2
        raise e

if __name__ == "__main__":

    utils.create_logging()
    main()
