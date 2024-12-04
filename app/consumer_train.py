import os
#os.environ['PYSPARK_PYTHON'] = 'C:/Users/pearu/.conda/envs/slim/python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import time

from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cassandra.cluster import Cluster
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, FMClassifier
from pyspark.ml.classification import RandomForestClassifier,  LogisticRegression , NaiveBayes, MultilayerPerceptronClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, IndexToString, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

kafka_topic_name = "demo20"
kafka_bootstrap_servers = 'localhost:9092'


if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

        
    orders_schema_string = '''ID STRING ,QUARTER INT, MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT ,
            OP_UNIQUE_CARRIER STRING,
            ORIGIN STRING,
            DEST STRING, DISTANCE DOUBLE,
            CRS_DEP_TIME DOUBLE,
            LABEL DOUBLE'''

    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    
    
    orders_df3.printSchema()
    



    predicted2 = orders_df3.select('ID' ,'QUARTER' , 'MONTH','DAY_OF_MONTH' , 'DAY_OF_WEEK' ,
            'OP_UNIQUE_CARRIER', 
            'ORIGIN',
            'DEST', 'DISTANCE',
            'CRS_DEP_TIME',
            'LABEL')
    print("oke")





    def write_to_cassandra(batch_df, batch_id):
        def save_partition(iterator):
            cluster = Cluster()
            session = cluster.connect('k1')
            
            # Log bắt đầu lưu dữ liệu vào Cassandra
            print(f"Batch {batch_id}: Start saving data to Cassandra.")
            
            query = """
            INSERT INTO train_data (id, quarter, month, day_of_month, day_of_week, op_unique_carrier, origin, dest, distance, crs_dep_time, label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            for row in iterator:
                try:
                    session.execute(query, (
                        row.ID, row.QUARTER, row.MONTH, row.DAY_OF_MONTH, row.DAY_OF_WEEK,
                        row.OP_UNIQUE_CARRIER, row.ORIGIN, row.DEST, row.DISTANCE, row.CRS_DEP_TIME, row.LABEL
                    ))
                    # Log thành công khi lưu từng dòng
                    print(f"Batch {batch_id}: Successfully saved row ID {row.ID} to Cassandra.")
                except Exception as e:
                    # Log lỗi khi lưu từng dòng
                    print(f"Batch {batch_id}: Error saving row ID {row.ID} - {e}")
            
            # Log hoàn thành lưu batch
            print(f"Batch {batch_id}: Finished saving data to Cassandra.")
            
            session.shutdown()
            cluster.shutdown()
             
            return []

        batch_df.rdd.mapPartitions(save_partition).collect()


    # Thiết lập writeStream để lưu vào Cassandra
    orders_agg_write_stream_cassandra = predicted2 \
        .writeStream \
        .trigger(processingTime="5 seconds") \
        .foreachBatch(write_to_cassandra) \
        .start()

    orders_agg_write_stream = predicted2 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    print("oke2")

    orders_agg_write_stream_cassandra.awaitTermination()
    orders_agg_write_stream.awaitTermination()
    print("Stream Data Processing Application Completed.")  