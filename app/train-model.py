import os
import pyspark

# Đảm bảo biến môi trường PYSPARK_PYTHON và PYSPARK_DRIVER_PYTHON được cài đặt
#os.environ["PYSPARK_PYTHON"] = "C:\\Users\\pearu\\.conda\\envs\\slim\\python.exe"
#os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\pearu\\.conda\\envs\\slim\\python.exe"

import pandas as pd 
import numpy as np 
import time

import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from IPython.core.display import display
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql import functions as f
from pyspark.sql import Row
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, RegexTokenizer, Tokenizer, CountVectorizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pprint import pprint
from sklearn.metrics import classification_report
import pyspark as ps
cluster = Cluster()
session = cluster.connect('k1')
conf = ps.SparkConf().setMaster("local[*]") \
    .setAppName("FinalProject") \
    .set("spark.driver.memory", "8g") \
    .set("spark.executor.memory", "8g") \
    .set("spark.memory.offHeap.enabled", "true") \
    .set("spark.memory.offHeap.size", "4g") \
    .set("spark.driver.maxResultSize", "4g") \
    .set("spark.sql.shuffle.partitions", "200") \
    .set("spark.default.parallelism", "200") \
    .set("spark.memory.fraction", "0.8") \
    .set("spark.memory.storageFraction", "0.2") \
    .set("spark.executor.heartbeatInterval", "3600s") \
    .set("spark.storage.memoryFraction", "0.3") \
    .set("spark.shuffle.memoryFraction", "0.5") \
    .set("spark.shuffle.spill.compress", "true") \
    .set("spark.rdd.compress", "true")

spark = SparkSession.builder.appName('FinalProject') \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()   
if __name__ == "__main__": 
    rows = session.execute("Select * from train_data")
    # rows_2 = session.execute("Select * from stream_data")

    list_ID = []
    list_QUARTER=[]	
    list_MONTH = []									
    list_DAY_OF_MONTH = []
    list_DAY_OF_WEEK = []
    list_OP_UNIQUE_CARRIER= []
    list_ORIGIN = []
    list_DEST = []
    list_CRS_DEP_TIME = []
    list_DISTANCE = []
    list_OUTPUT = []

    for row in rows:
        list_ID.append(str(row.id))
        list_QUARTER.append(row.quarter)
        list_MONTH.append(row.month)
        list_DAY_OF_MONTH.append(row.day_of_month)
        list_DAY_OF_WEEK.append(row.day_of_week)
        # list_FL_DATE.append(str(row.fl_date))
        list_OP_UNIQUE_CARRIER.append(row.op_unique_carrier)
        # list_OP_CARRIER_FL_NUM_NOR.append(row.op_carrier_fl_num_nor)
        list_ORIGIN.append(row.origin)
        list_DEST.append(row.dest)
        list_CRS_DEP_TIME.append(row.crs_dep_time)
        list_DISTANCE.append(row.distance)
        list_OUTPUT.append(row.label)


    # for row in rows_2:
    #     list_ID.append(str(row.id))
    #     list_QUARTER.append(row.quarter)
    #     list_MONTH.append(row.month)
    #     list_DAY_OF_MONTH.append(row.day_of_month)
    #     list_DAY_OF_WEEK.append(row.day_of_week)
    #     list_OP_UNIQUE_CARRIER.append(row.op_unique_carrier)
    #     list_ORIGIN.append(row.origin)
    #     list_DEST.append(row.dest)
    #     list_CRS_DEP_TIME.append(row.crs_dep_time)
    #     list_DISTANCE.append(row.distance)
    #     list_OUTPUT.append(row.label)


    df = pd.DataFrame(list(zip(list_ID,list_QUARTER,list_MONTH,list_DAY_OF_MONTH, \
                            list_DAY_OF_WEEK,list_OP_UNIQUE_CARRIER, \
                            list_ORIGIN,list_DEST, list_DISTANCE,\
                            list_CRS_DEP_TIME,list_OUTPUT)))


    # adding column name to the respective columns
    df.columns =['ID', 'QUARTER','MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', \
                'OP_UNIQUE_CARRIER','ORIGIN', \
                'DEST','DISTANCE','CRS_DEP_TIME','LABEL']   


    schema = '''ID STRING, QUARTER INT,MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT,
                OP_UNIQUE_CARRIER STRING,
                ORIGIN STRING,
                DEST STRING,DISTANCE DOUBLE,
                CRS_DEP_TIME DOUBLE, LABEL DOUBLE
                '''
    


            
    Train = spark.createDataFrame(df, schema=schema)

    Test = spark.read.csv('/home/snowfox/Documents/kafka/Dataset/stream_balanced.csv', header=True, schema=schema)

    OP_UNIQUE_CARRIER_indexer = StringIndexer(inputCol='OP_UNIQUE_CARRIER',outputCol='OP_UNIQUE_CARRIERIndex', handleInvalid="keep" )
    OP_UNIQUE_CARRIER_encoder = OneHotEncoder(inputCol='OP_UNIQUE_CARRIERIndex',outputCol='OP_UNIQUE_CARRIERVec')
    ORIGIN_indexer = StringIndexer(inputCol='ORIGIN',outputCol='ORIGINIndex', handleInvalid="keep")
    ORIGIN_encoder = OneHotEncoder(inputCol='ORIGINIndex',outputCol='ORIGINVec')
    DEST_indexer = StringIndexer(inputCol='DEST',outputCol='DESTIndex', handleInvalid="keep")
    DEST_encoder = OneHotEncoder(inputCol='DESTIndex',outputCol='DESTVec')

    assembler = VectorAssembler(inputCols=['QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIERVec', 'ORIGINVec', 
                                        'DESTVec', 'DISTANCE', 'CRS_DEP_TIME'], outputCol='features')#maxDepth=16
    LR = LogisticRegression(featuresCol='features',labelCol='LABEL')
    pipeline = Pipeline(stages=[OP_UNIQUE_CARRIER_indexer, 
                                OP_UNIQUE_CARRIER_encoder, 
                                ORIGIN_indexer, 
                                ORIGIN_encoder, 
                                DEST_indexer, 
                                DEST_encoder,  
                                assembler, LR])
    
    
    import logging
    # Cấu hình logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Giả sử các bước xử lý dữ liệu trước đó đã hoàn tất
    logger.info("Chuẩn bị chạy pipeline.fit trên dữ liệu Train")

    Train = Train.repartition(200)  # Tăng số partition để xử lý song song
    Train.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)  # Cache với cả memory và disk

    # Thêm checkpoint để tránh stack overflow với DAG quá dài
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")
    Train.checkpoint()

    # Khi train model, có thể chia nhỏ dữ liệu
    train_batches = Train.randomSplit([0.2, 0.2, 0.2, 0.2, 0.2])
    final_model = None
####vì cấu hình máy của team không đủ lớn để train toàn bộ dataset, ở đây chỉ train 20% dataset,
# chia batch chỉ là giả lập, 
# Trong thực tế có thể cần logic để merge các model######## 
    for i, batch in enumerate(train_batches):
        logger.info(f"Training batch {i+1}/5")
        if i == 0:
            model = pipeline.fit(batch)
            final_model = model
        else:
            # Cập nhật model với batch mới
            model = pipeline.fit(batch)
            final_model = model 
###############################################################################
    # Giải phóng bộ nhớ
    Train.unpersist()

    # Thêm log sau khi quá trình fit hoàn tất
    logger.info("Đã hoàn tất quá trình pipeline.fit")

    # Log trước khi lưu model
    logging.info("Bắt đầu lưu model")
    try:
        final_model.write().overwrite().save("/home/snowfox/Documents/kafka/Model")
        logging.info("Hoàn tất lưu model tại /home/snowfox/Documents/kafka/Model")
    except Exception as e:
        logging.error("Lỗi khi lưu model: %s", e)
        raise



    ############################# test model ###############################
    from pyspark.ml.pipeline import PipelineModel
    model2 = PipelineModel.load("/home/snowfox/Documents/kafka/Model")
    # Cấu hình logging
    logging.basicConfig(level=logging.INFO)

    # Log trước khi chạy model.transform(Test)
    logging.info("Bắt đầu chạy model.transform(Test)")

    # Thực hiện transform và log kết quả
    try:
        Tested = model2.transform(Test)
        logging.info("Hoàn tất chạy model.transform(Test)")
    except Exception as e:
        logging.error("Lỗi khi chạy model.transform(Test): %s", e)
        raise

    # Log trước khi chuyển đổi sang pandas DataFrame
    logging.info("Chuyển đổi kết quả sang pandas DataFrame")
    try:
        predicted_score = Tested.select('LABEL', 'prediction').toPandas()
        logging.info("Hoàn tất chuyển đổi sang pandas DataFrame")
    except Exception as e:
        logging.error("Lỗi khi chuyển đổi sang pandas DataFrame: %s", e)
        raise

                

    from sklearn.metrics import f1_score
    print(classification_report(predicted_score.LABEL, predicted_score.prediction))
    evaluator = MulticlassClassificationEvaluator(labelCol='LABEL', metricName='accuracy')
    accuracy_test = evaluator.evaluate(Tested)
    f1_mic = f1_score(predicted_score['LABEL'], 
                    predicted_score['prediction'], 
                    average = 'micro')
    f1_mac = f1_score(predicted_score['LABEL'], 
                    predicted_score['prediction'], 
                    average = 'macro')



    print('='*15+'Testing Data'+'='*15)
    print(f'Accuracy: {accuracy_test*100:.5f}%')
    print(f'F1-Micro: {f1_mic*100:.5f}%')
    print(f'F1-Macro: {f1_mac*100:.5f}%')


print('train model completed') 