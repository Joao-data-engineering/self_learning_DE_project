from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime
import utils
import logging

def main():

    """
        Main function to aggregate information for actors and actresses and store the results.

        This function performs the following operations:
        - Initializes a Spark session.
        - Reads data from specified Delta tables and applies filters and transformations.
        - Aggregates information for actors and actresses, including average rating, total run time minutes, and total titles as principals.
        - Writes the aggregated data to a specified directory with the given table name.
        - Updates a PostgreSQL database table with the aggregated data.
        - Updates the metadata table to reflect the status of the aggregation process.

        In case of an exception, it logs the error, attempts to update the metadata table with an 'unsuccessful' status,
        and re-raises the exception.

        Raises:
            Exception: Propagates any exception that occurs during the aggregation process and logs it.
    """

    spark: SparkSession = utils.spark_session_build(session_name = 'aggredated_info')

    try:
        df_fact_title_principals: DataFrame = utils.spark_read_file(file_format = 'delta', spark = spark, file_path = '/opt/storage/cleansed_zone/fact_title_principals').filter((F.col('role') == 'actor') | (F.col('role') == 'actress')).drop('num_votes')
        df_dim_names: DataFrame = utils.spark_read_file(spark = spark, file_format = 'delta', file_path = '/opt/storage/cleansed_zone/dim_names').select("nconst","name")

        df_final_table: DataFrame = df_dim_names.join(df_fact_title_principals, 'nconst', how = 'left')

        result_df: DataFrame = df_final_table.groupBy("nconst") \
        .agg(
            F.avg("rating").alias("average_rating"),
            F.sum("run_time_minutes").alias("total_run_time_minutes"),
            F.count(F.when(F.col("role").isNotNull(), True)).alias('total_titles_as_principal')
        )

        result_df = df_final_table.join(result_df, 'nconst', how = 'inner').select('nconst','role','name','average_rating','total_run_time_minutes', 'total_titles_as_principal').orderBy(F.desc("average_rating"))

        result_df = result_df.filter(F.col("role").isNotNull())
        
        utils.write_dataframe(df = result_df, directory = 'aggregated_zone/actors_actress_kpis', table_name = 'actors_actress_kpis')

        utils.update_postgresql(df = result_df, dbtable = 'actors_actress_kpis')

        utils.update_metadata_table(spark=spark, stage='aggregated_zone', table_name='actors_actress_kpis', date=datetime.now().date(), status = 'success')
    
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        try:
            utils.update_metadata_table(spark=spark, stage='aggregated_zone', table_name='actors_actress_kpis', date=datetime.now().date(), status = 'unsuccessful')
        except Exception as e2:
            logging.error(f"Error occurred updating unsuccessful run: {e2}")
            raise e2
        raise e
    
if __name__ == "__main__":
    utils.create_logging()
    main()