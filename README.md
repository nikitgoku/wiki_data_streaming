# End-to-End Data Engineering Project | Data Streaming

## Overview
Through this project I desingned a streaming data pipeline, which extracts the data from the WikiMedia API, specifically the API:RecentChanges, process it through the pipeline and loads the processed data into a database. This project utilised the following tech stack which includes Python and Shell scripting, Apache Kafka (Zookeeper and Confluent), Apache Spark (Spark Streaming), PostgreSQL and pgAdmin4, where everything is containerised using Docker.

## System Architecture

![image](https://github.com/user-attachments/assets/9cf933e5-5a33-44f8-b3f3-cc534ded7ef9)

The project is designed with the following architecture:
1. Data Source: I utilised the [MediaWiki Action API](https://www.mediawiki.org/wiki/API:Main_page) to source the data. Specifically I was focussed on the [recent changes](https://www.mediawiki.org/wiki/API:RecentChanges) done on Wikipedia which is usually a lot considering there are many edits done on the existing articles and much more articles are added every minute.
2. Apache Airflow: Utilised for orchestrating the streaming pipeline.
3. Apache Kafka: Utilised to stream the data from APIs to the processing engine and staging area.
4. Apache Spark: Spark Streaming was used for data processing and streaming it into a database.
5. PosgreSQL: Database where the processed data is stored and used further.

## Docker Compose Details

| Container | Image | port | 
|-|-|-|
| zookeeper | confluentinc/cp-zookeeper | 2181 |
| kafka-broker | confluentinc/cp-server | 9092 |
| schema-registry | confluentinc/cp-schema-registry | 8081 |
| control-center | confluentinc/cp-enterprise-control-center | 9021 |
| webserver | apache/airflow | 8080 |
| scheduler | apache/airflow | - |
| postgres | postgres | 5432 |
| pgadmin | dpage/pgadmin4 | 5050 |

## Quickstart

### Clone the repository

```
git clone https://github.com/amine-akrout/Spark_Stuctured_Streaming.git
```

### Running Docker Compose

To run docker compose simply run the following command:

```
docker-compose up -d
```

### PostgreSQL configuration

To acces the pgAdmin4 webpage navigate to http://localhost:5050 and using the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` you can login to the pgAdmin homepage. From there you can follow these simple steps to create a PostgreSQL server which is used to create the database and tables.
1. Click on `Add New Server` in pgAdmin homepage.
2. In the `Connection` tab, enter the following information: Host name/address -- `db` (The name of database service defined in `docker-compose`), Port -- `<DB_PORT>` (`5432` is the default PostgreSQL port number), Maintenance database -- `<POSTGRES_DB>`(The name of the database specified in .env file), Username -- `<POSTGRES_USER>`, Password -- `<POSTGRES_PASSWORD>`
3. Click `Save` to save the server configuration.
4. Your PostgreSQL server should now appear under the "Servers" section in homepage.

### Airflow and Kafka Producer

Now to start writing data into Kafka topics, the Airflow DAG needs to be started in order to fetch data from the API. The flow inside the DAG is simple, it first fetches or extracts the data from `RecentChanges` API, the formats the data accrodingly and then Kafka producer writes the data into a topic.

To start the DAG, first we need to access the the Airflow Webbrowser which can be reached by navigating to https://localhost:8080. To access the Airflow Webbrowser, you can use the `username` and `passowrd` set in the `script/entrypoint.sh` file which can be chaned accrodingly.

Now, when the DAG is run, you should be able to see the following screen:
![image](https://github.com/user-attachments/assets/461538d1-eee8-4d8d-b6fe-9d353387483e)

This dag will start extracting data from the API and post these data into Kafka Topic.

The Confluent Control Centre is the place where you'll be able to see whether the data is written into the topic. To access the Confluent Control Centre navigate to https://localhost:9021, where cluster information can be seen as shown below:
![Screenshot 2024-09-13 at 15 11 29](https://github.com/user-attachments/assets/d2fb970a-908e-42f5-88e1-48e1aac4f11b)

For the cluster if you navigate to `Topics` you will see all the messages written in to topics as follows:
![Screenshot 2024-09-13 at 15 12 32](https://github.com/user-attachments/assets/d0425e8e-a648-4f10-ab4b-1a8fdb38241f)

### Spark Streaming:
From the terminal window, run the `spark-submit`.
```
spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.6.2 --master spark://localhost:7077 spark_streaming.py
```
This command which allows Spark to:
- Construct a streaming dataframe that reads from Kafka topic subscribed.
- Execute some data processing on the streaming dataframe.
- Write processed data in PostgreSQL Database.
- Optionally : Write batches in the console for debugging

> [NOTE]
> The `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2` and `org.postgresql:postgresql:42.6.2` are the required dependencies which are needed in order for the Spark to subscribe to Kafka topic and to write data into the PostgreSQL database from the streaming dataframe. These dependencies can be found in the Maven Repository [PostgreSQL JDBC Driver](https://mvnrepository.com/artifact/org.postgresql/postgresql) and [Spark SQL Kafka connector](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.5.2).
You can refer the [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) article from Spark's own documentation for more information on this topic.


### Database
Now, you can check if the data is being populated in PostgreSQL database, in the table `public.wiki_recent_changes`. The `spark_streaming.py`script will create a database `wiki_data` with a table `wiki_recent_changes` using the `psycopg2` library, which will be automatically populated.

![Screenshot 2024-09-14 at 00 29 39](https://github.com/user-attachments/assets/67ee777d-6fbc-4ddb-9ca0-12e680d3d8fd)


### Dashboard
A dashboard can be created which can be fed with the live streaming data.
