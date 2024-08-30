
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

import logging

import constants


def generate_spark_connection():
    # Spark Connection variable
    spark_conn = None

    try:
        spark_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .config("spark.jars.package", "/Users/nikit/Documents/GitHub/wiki_data_streaming/jarfiles/spark-sql-kafka-0-10_2.12-3.5.2.jar").getOrCreate()
            # .config("spark.jars.package", "io.github.spark-redshift-community:spark-redshift_2.12:5.1.0") \
            # .config("spark.jars.package", "org.apache.spark:spark-hadoop-cloud_2.12:3.2.1") \
            # .config("spark.jars.package", "org.apache.spark:spark-avro_2.13:3.3.0") \
            # .config("spark.jars", "/Users/nikit/Documents/GitHub/wiki_data_streaming/jdbc_driver/redshift-jdbc42-2.1.0.30.jar") \
            # .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("INFO: Spark Connection created successfully!")
    except Exception as e:
        logging.error(f"ERROR: Cannot create Spark Session due to exception {e}")

    return spark_conn


def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.read \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'recent_changes') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info('INFO: Kafka dataframe created successfully!')
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created! because {e}")

    return spark_df


def select_dataframe_from_kafka(spark_dataframe):
    # Define the schema
    schema = StructType([
        StructField("title", StringType(), False),
        StructField("time_stamp", StringType(), False),
        StructField("username", StringType(), False),
        StructField('change_type', StringType(), False),
        StructField('pageId', LongType(), False),
        StructField('oldsize', IntegerType(), False),
        StructField('newsize', IntegerType(), False),
        StructField('revisionId', LongType(), False),
        StructField('old_revisionId', LongType(), False)
    ])

    select_ = spark_dataframe.selectExpr("CAST (value as STRING") \
        .select(from_json(col('value'), schema).alias('data')).select('data.*')

    print(select_)
    return select_



if __name__ == "__main__":
    # Generate Spark Connection
    spark_connection = generate_spark_connection()
    if spark_connection is not None:
        print("INFO: Spark Connection successful!")
        logging.info("INFO: Spark Connection successful!")
        # Connect to Kafka
        spark_dataframe = connect_to_kafka(spark_connection)
        #selected_dataframe = select_dataframe_from_kafka(spark_dataframe)

        # if selected_dataframe is not None:
        #     logging.info("INFO: Streaming is being started...")

            # streaming_query = selected_dataframe.write \
            #     .format("io.github.spark_redshift_community.spark.redshift") \
            #     .option("url", constants.REDSHIFT_JDBC_URL) \
            #     .option("dbtable", "public.wiki_data") \
            #     .option("tempdir", constants.AWS_S3_TEMP_DIR) \
            #     .option("aws_iam_role", constants.AWS_IAM_ROLE) \
            #     .mode("error") \
            #     .save()
            #
            # streaming_query.awaitTermination()