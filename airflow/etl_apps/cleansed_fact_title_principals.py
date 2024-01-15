from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType
import utils
from datetime import datetime
import logging

#title_principals
def cast_title_principals(df: DataFrame) -> DataFrame:

    """
        Casts the columns of title principals specific data types.

        This function modifies the data types of various columns in the input DataFrame:
        - 'tconst', 'nconst', 'category', 'job', and 'characters' are cast to StringType.
        - 'ordering' is cast to ShortType.

        Args:
            df (DataFrame): A Spark DataFrame containing title principals data.

        Returns:
            DataFrame: The transformed DataFrame with columns cast to specified data types.
    """
    
    df_casted = df.withColumn("tconst", F.col("tconst").cast(StringType())) \
              .withColumn("ordering", F.col("ordering").cast(ShortType())) \
              .withColumn("nconst", F.col("nconst").cast(StringType())) \
              .withColumn("category", F.col("category").cast(StringType())) \
              .withColumn("job", F.col("job").cast(StringType())) \
              .withColumn("characters", F.col("characters").cast(StringType()))
    
    return df_casted

def transformation_title_principals(df: DataFrame) -> DataFrame:

    """
        Transforms the title principals DataFrame by manipulating and renaming columns.

        The function performs the following operations:
        - Drops the 'job' and 'ordering' columns.
        - Replaces '\\N' in 'characters' column with None.
        - Removes square brackets and quotes from 'characters' column.
        - Splits 'characters' into an array by commas.
        - Renames 'category' column to 'role'.
        - Drops the 'today_date' column.

        Args:
            df (DataFrame): A Spark DataFrame containing title principals data.

        Returns:
            DataFrame: The transformed DataFrame.
    """

    df_title_principals: DataFrame = df.drop("job","ordering")
    df_title_principals: DataFrame = df_title_principals.withColumn('characters', F.when(F.col('characters') == '\\N', F.lit(None)).otherwise(F.col('characters'))) 
    df_title_principals: DataFrame = df_title_principals.withColumn("characters", F.regexp_replace(F.col("characters"), '[\[\]\"]', ""))
    df_title_principals: DataFrame = df_title_principals.withColumn("characters", F.split(F.trim(df_title_principals["characters"]), "\\s*,\\s*"))
    df_title_principals: DataFrame = df_title_principals.withColumnRenamed("category",'role')
    df_title_principals: DataFrame = df_title_principals.drop("today_date")

    return df_title_principals


def cast_title_ratings(df: DataFrame) -> DataFrame:

    """
        Casts the columns of title ratings to specific data types.

        This function changes the data types of the following columns:
        - 'tconst' is cast to StringType.
        - 'averageRating' is cast to DecimalType with precision 10 and scale 1.
        - 'numVotes' is cast to IntegerType.

        Args:
            df (DataFrame): A Spark DataFrame containing title ratings data.

        Returns:
            DataFrame: The DataFrame with columns casted to specified data types.
    """

    df_casted = df.withColumn("tconst", F.col("tconst").cast(StringType())) \
              .withColumn("averageRating", F.col("averageRating").cast(DecimalType(10, 1))) \
              .withColumn("numVotes", F.col("numVotes").cast(IntegerType()))
    
    return df_casted

def transformation_title_ratings(df: DataFrame) -> DataFrame:
    """
        Transforms the title ratings DataFrame by renaming and dropping specific columns.

        This function performs the following operations:
        - Renames columns based on a dictionary mapping: 'averageRating' is renamed to 'rating', 
        and 'numVotes' is renamed to 'num_votes'.
        - Drops the 'today_date' column.

        Parameters:
            df (DataFrame): A Spark DataFrame containing title ratings data.

        Returns:
            DataFrame: The transformed DataFrame with renamed and dropped columns.
    """

    dict_new_column_names: dict = {'averageRating':'rating','numVotes':'num_votes'}
    for column in dict_new_column_names.keys():
        df: DataFrame = df.withColumnRenamed(column,dict_new_column_names[column])

    df: DataFrame = df.drop("today_date")
    return df

def main():
    """
        Main function to execute the ETL process for fact title principals into cleansed zone.

        This function performs the following operations:
        - Initializes a Spark session.
        - Reads, filters, and transforms data from three different sources (title principals, title basics, and title ratings).
        - Joins the transformed data to create a fact table.
        - Writes the fact table to a specified location.
        - Updates the metadata table to reflect the status of the ETL process.

        In case of any exceptions during the process, it logs the error, attempts to update the metadata table with an 'unsuccessful' status, and re-raises the exception.

        Raises:
            Exception: Propagates any exception that occurs during the ETL process and logs it.
    """

    spark: SparkSession = utils.spark_session_build(session_name = 'etl_fact_principals')

    try:
        categories = ['actor','actress']
        file_path_title_principals: str = "/opt/storage/landing_zone/title_principals"
        df_title_principals: DataFrame = utils.spark_read_file(file_path = file_path_title_principals, file_format = 'delta', spark = spark).filter(F.col('category').isin(categories)) #filtering bacause of technical constraints
        df_title_principals: DataFrame = cast_title_principals(df_title_principals)
        df_title_principals: DataFrame = transformation_title_principals(df = df_title_principals)

        file_path_title_basics: str = "/opt/storage/landing_zone/title_basics"  
        run_time_minutes_df: DataFrame = utils.spark_read_file(file_path = file_path_title_basics, file_format = 'delta', spark = spark).filter((F.col('startYear')>1960) & (F.col('startYear')<2020)).select('tconst','runtimeMinutes') #filtering bacause of technical constraints
        run_time_minutes_df: DataFrame = run_time_minutes_df.withColumn("runtimeMinutes", F.col("runtimeMinutes").cast(IntegerType()))
        run_time_minutes_df: DataFrame = run_time_minutes_df.withColumnRenamed('runtimeMinutes','run_time_minutes')

        file_path_title_ratings: str = "/opt/storage/landing_zone/title_ratings"
        df_title_ratings: DataFrame = utils.spark_read_file(file_path = file_path_title_ratings, file_format = 'delta', spark = spark)
        df_title_ratings: DataFrame = cast_title_ratings(df_title_ratings)
        df_title_ratings: DataFrame = transformation_title_ratings(df = df_title_ratings)

        df_fact_titles: DataFrame = run_time_minutes_df.join(df_title_ratings, ['tconst'], how = 'left')
        df_fact_title: DataFrame = df_fact_titles.join(df_title_principals,'tconst',how = 'left')
        current_date = datetime.now().date()
        df_fact_title.withColumn("today_date", F.lit(current_date))

        df_fact_title.show()

        utils.write_dataframe(df = df_fact_title, table_name = 'fact_title_principals', directory = 'cleansed_zone/fact_title_principals', incremental = False, partition_column = 'role')
        utils.update_metadata_table(spark=spark, stage='cleansed_fact_title_principals', table_name='fact_title_principals', date=datetime.now().date(), status = 'success')
    
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        try:
            utils.update_metadata_table(spark=spark, stage='cleansed_fact_title_principals', table_name='fact_title_principals', date=datetime.now().date(), status = 'unsuccessful')
        except Exception as e2:
                logging.error(f"Error occurred updating unsuccessful run: {e2}")
                raise e2
        raise e
if __name__ == "__main__":

    utils.create_logging()
    main()
