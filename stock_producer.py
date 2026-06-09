from kafka import KafkaProducer
import pandas as pd
import json
from time import sleep

# Establishes connection to Kafka server
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Loads 10 million+ stock records from CSV
df = pd.read_csv("indexProcessed.csv")
print("Producer started. Sending 5 sample records...")

for i in range(5):  # Just send 5 records for demo
    # Loads 10 million+ stock records from CSV
    sample = df.sample(1).to_dict(orient="records")[0]
    # Sends the stock data to Kafka topic 'stock_prices'
    producer.send('stock_prices', value=sample)
    print(f"Sent: {sample}")
    # Waits 1 second between sends (simulates real-time streaming)
    sleep(1)

producer.flush()
print("Done! Check consumer.")