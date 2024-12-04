from kafka import KafkaProducer
from datetime import datetime
import time
import random
import pandas as pd 
import numpy as np


data_predict = pd.read_csv("/home/snowfox/Documents/kafka/Dataset/train_balanced.csv")

 
KAFKA_TOPIC_NAME_CONS = "demo20"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))

    message_list = []
    message = None
    
    massage_total = []
    """     for i in range(min(600000, len(data_predict))): """
    for i in range(len(data_predict)):
        message_fields_value_list = []
        message_fields_value_list.append(str(data_predict.iloc[i, 0]))
        message_fields_value_list.append(str(data_predict.iloc[i, 1]))
        message_fields_value_list.append(str(data_predict.iloc[i, 2]))
        message_fields_value_list.append(str(data_predict.iloc[i, 3]))
        message_fields_value_list.append(str(data_predict.iloc[i, 4]))
        message_fields_value_list.append(str(data_predict.iloc[i, 5]))
        message_fields_value_list.append(str(data_predict.iloc[i, 6]))
        message_fields_value_list.append(str(data_predict.iloc[i, 7]))
        message_fields_value_list.append(str(data_predict.iloc[i, 8]))
        message_fields_value_list.append(str(data_predict.iloc[i, 9]))
        message_fields_value_list.append(str(data_predict.iloc[i, 10]))

        message = ",".join(message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)

    print("Kafka Producer Application Completed. ")
    