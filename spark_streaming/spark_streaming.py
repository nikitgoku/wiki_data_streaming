# ------------------------------------------------------------ #
#  Version    : 0.1    							               #
#  Created On : 12-Sept-2024 0:44:32                           #
#  Created By : Nikit Gokhale                                  #
#  Script     : Python                                         #
#  Notes      : Script to create read from the Kafka topic     #
#               use spark strucuted streaming to write into    #
#               PostgreSQL database                            #
# ------------------------------------------------------------ #

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import from_json, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from constants import load_postgres_config

import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_postgres_database():
    """
    This function creates database in the PostgreSQL database
    """
    commands = (
        """
            CREATE DATABASE wiki_data WITH ENCODING 'utf8' TEMPLATE template0
        """,
    )
    conn = None

    try:
        # Read the connection configurations
        params = load_postgres_config()
        # Connect to PostgreSQL server
        conn = psycopg2.connect(**params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        # Execute the command
        for command in commands:
            cursor.execute(command)
        # Close the communication with the PostgreSQL database server
        cursor.close()
        # Commit changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"ERROR: Cannot create PostgreSQL database due to exception {e}")
    finally:
        if conn is not None:
            conn.close()


def create_db_table():
    """
    This function creates tables in the PostgreSQL database
    """
    commands = (
        """
            CREATE TABLE public.recent_changes_data (
                title TEXT,
                username VARCHAR(25),
                change_type VARCHAR(10),
                pageId BIGINT,
                oldsize BIGINT,
                newsize BIGINT,
                revisionId BIGINT,
                old_revisionId BIGINT,
                timestamp TIMESTAMP WITHOUT TIMEZONE,
                uid INT
            );
        """,
    )
    conn = None

    try:
        # Read the connection configurations
        params = load_postgres_config()
        # Connect to PostgreSQL server
        conn = psycopg2.connect(**params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        # Execute the command
        for command in commands:
            cursor.execute(command)
        # Close the communication with the PostgreSQL database server
        cursor.close()
        # Commit changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"ERROR: Cannot create PostgreSQL database due to exception {e}")
    finally:
        if conn is not None:
            conn.close()


def generate_spark_connection():
    """
    This function generates a connection to spark session which can be used
    to streamline the work flow taking in all the required configuration
    Returns: A connection/cursor to Spark Session
    """
    # Spark Connection variable
    spark_conn = None

    # Use try statements to build a spark session
    try:
        spark_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
            .master("local[*]") \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("INFO: Spark Connection created successfully!")
    # Throw an exception if the connection is unsuccessful
    except Exception as e:
        logging.error(f"ERROR: Cannot create Spark Session due to exception {e}")

    return spark_conn


def connect_to_kafka(spark_conn):
    """
    This function acts as a consumer by subscribing to Kafka topics
    and loading the data into a dataframe.
    Returns: A kafka dataframe which was subscribed to the topic that contains the 
            wiki recent changes data.
    """
    kafka_df = None

    try:
        kafka_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'recent_changes') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info('INFO: Kafka dataframe created successfully!')
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created! because {e}")

    return kafka_df


def parse_dataframe_from_kafka(spark_dataframe):
    """
    This function defines the schema for the streaming data arrives from the Kafka topics
    and processes the data in the required format.
    Returns: A processed dataframe which has all the column headers and their type defined.
    """
    # Define the schema
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("time_stamp", StringType(), True),
        StructField("username", StringType(), True),
        StructField('change_type', StringType(), True),
        StructField('pageId', LongType(), True),
        StructField('oldsize', IntegerType(), True),
        StructField('newsize', IntegerType(), True),
        StructField('revisionId', LongType(), True),
        StructField('old_revisionId', LongType(), True)
    ])

    # Parsing value from binary to string
    kafka_df = spark_dataframe.selectExpr("CAST (value as STRING)")

    # Apply schema to the JSON value column and expand the column
    expanded_df = kafka_df.withColumn("value", from_json(kafka_df["value"], schema)).select("value.*")

    # Get the required columns from the expanded dataframe
    final_df = expanded_df \
        .select("title", "time_stamp", "username", "change_type", "pageId", "oldsize", "newsize", "revisionId", "old_revisionId") \
        .withColumn("uid", expr("uuid()")) \
        .withColumn("timestamp", to_timestamp("time_stamp")) \
        .drop("time_stamp")

    final_df.printSchema()
    return final_df

    
def write_to_postgres(dataframe, epoch_id):
    """
    This function writes the data into micro batches. The write mode is
    set to 'append' which ensures that the data is written incrementally into the database
    """   
    # Write the batch to postgreSQL
    dataframe.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/wiki_data") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public.wiki_recent_changes') \
        .option("user", 'postgres') \
        .option("password", 'postgres') \
        .save()


if __name__ == "__main__":
    # Create a PostgreSQL database named 'wiki_data'
    create_postgres_database()
    # Create a table within the 'wiki_data' database with name 'recent_changes_data'
    create_db_table()
    # Generate Spark Connection
    spark_connection = generate_spark_connection()
    if spark_connection is not None:
        logging.info("INFO: Spark Connection successful!")
        # Connect to Kafka and get the dataframe from Kafka
        kafka_dataframe = connect_to_kafka(spark_connection)
        # Get the processed dataframe
        processed_dataframe = parse_dataframe_from_kafka(kafka_dataframe)

        if kafka_dataframe is not None:
            logging.info("INFO: Streaming is being started...")

            streaming_query = kafka_dataframe.writeStream \
                            .trigger(processingTime = '10 seconds') \
                            .outputMode('update') \
                            .foreachBatch(write_to_postgres) \
                            .start()
            
            # console_query = kafka_dataframe.writeStream \
            #                 .trigger(processingTime='2 seconds') \
            #                 .outputMode("update") \
            #                 .option("truncate", "false")\
            #                 .option("checkpointLocation", "temp/spark-checkpoint") \
            #                 .format("console") \
            #                 .start()
                
            # Await Termination
            # console_query.awaitTermination()
            streaming_query.awaitTermination()
                       
            