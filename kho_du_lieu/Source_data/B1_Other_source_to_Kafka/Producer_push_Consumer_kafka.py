from kafka import KafkaProducer
# from kafka import KafkaConsumer  # liabray to get data from Producer
import csv,json

# Kafka producer object -> send messages -> Kafka Topic
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092', 
    value_serializer=lambda v:json.dumps(v).encode('utf-8')
)
# Bootstrap_server -> kafka server
# alue_serializer=lambda v:json.dumps(v).encode('utf-8') --> convert the message value(bytes Json) before sending it to Kafka

csv_path = 'G:/Kho Du Lieu/superstore_dataset2011-2015.csv'
with open(csv_path, 'r') as file: 
    reader = csv.DictReader(file) # class in csv -> read each row -> return under dictionary format
    for row in reader:
        producer.send('SuperstoreData', row)  # SuperstoreData is topic is sent 
producer.flush()
    