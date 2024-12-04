import os
#os.environ['PYSPARK_PYTHON'] = 'C:/Users/pearu/.conda/envs/slim/python.exe'
#os.environ['SPARK_HOME'] = 'C:/spark-3.5.3-bin-hadoop3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time
from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, FMClassifier
from pyspark.ml.classification import RandomForestClassifier,  LogisticRegression , NaiveBayes, MultilayerPerceptronClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, IndexToString, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

kafka_topic_name = "demo22"
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


        
    persistedModel = PipelineModel.load("/home/snowfox/Documents/kafka/Model")
    print("Chua transform")
    prediction1 = persistedModel.transform(orders_df3)
    print("Da transform")
    predicted1 = prediction1.select('LABEL', "prediction",'timestamp')
    print("Khong  van de 1")
    predicted2 = prediction1.select('ID' ,'QUARTER' , 'MONTH','DAY_OF_MONTH' , 'DAY_OF_WEEK' ,
            'OP_UNIQUE_CARRIER', 
            'ORIGIN',
            'DEST', 'DISTANCE',
            'CRS_DEP_TIME',
            'LABEL', "prediction")
    print("Khong  van de 2")
    
    # Tạo thư mục riêng cho từng stream
    checkpoint_path1 = "/home/snowfox/Documents/kafka/kafka_stream_test_out/chk1"
    checkpoint_path2 = "/home/snowfox/Documents/kafka/kafka_stream_test_out/chk2"
    os.makedirs(checkpoint_path1, exist_ok=True)
    os.makedirs(checkpoint_path2, exist_ok=True)

    # Stream 1: Ghi vào CSV
    orders_agg_write_stream1 = predicted2 \
        .writeStream \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .option("path", "/home/snowfox/Documents/kafka/output/") \
        .option("checkpointLocation", checkpoint_path1) \
        .option("cleanSource", "delete") \
        .format("csv") \
        .start()

    # Stream 2: Ghi ra console
    orders_agg_write_stream = predicted1 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_path2) \
        .option("failOnDataLoss", "false") \
        .format("console") \
        .start()

    # Đợi cả 2 stream kết thúc
    from pyspark.sql.streaming import StreamingQuery
    import time

    def await_termination(queries):
        for q in queries:
            try:
                q.awaitTermination()
            except Exception as e:
                print(f"Query failed: {str(e)}")
                
        print("All queries terminated")

    # Đợi cả 2 stream
    try:
        orders_agg_write_stream1.awaitTermination()
        orders_agg_write_stream.awaitTermination()
    except Exception as e:
        print(f"Lỗi streaming: {str(e)}")
        # Cleanup
        if orders_agg_write_stream1 is not None:
            orders_agg_write_stream1.stop()
        if orders_agg_write_stream is not None:
            orders_agg_write_stream.stop()

    print("Stream Data Processing Application Completed.")

    # Đảm bảo thư mục tồn tại trước khi chạy
    os.makedirs("/home/snowfox/Documents/kafka/output", exist_ok=True) 
