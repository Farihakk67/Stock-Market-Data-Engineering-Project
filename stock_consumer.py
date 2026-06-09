from kafka import KafkaConsumer
import json
import os
import boto3
from datetime import datetime

# AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAQCOH3G6BIJE7LEMT'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'rh/PfYq2cEk6YQQvkm6KpLZxM7+MMZ5I6ZY0w3TH'

# S3 client
s3_client = boto3.client('s3', region_name='us-east-1')

# YOUR BUCKET NAME - AB YEH USE KARO 👇
BUCKET_NAME = 'aws-s3-stock-bucket'

# Kafka consumer
consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='stock-consumer-group',
    enable_auto_commit=True
)

print("Consumer started. Waiting for messages...")
print(f"📤 S3 Bucket: {BUCKET_NAME}")
print("-" * 50)

message_count = 0
for message in consumer:
    message_count += 1
    data = message.value
    print(f"Received #{message_count}: {data}")
    
    # S3 UPLOAD - AB KAM KAREGA!
    try:
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        index_name = data.get('Index', 'unknown')
        filename = f"stock-data/{index_name}_{timestamp}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        print(f"✅ Uploaded to S3: s3://{BUCKET_NAME}/{filename}")
        
    except Exception as e:
        print(f"❌ S3 Upload failed: {str(e)}")
    
    print("-" * 50)
    
    # Demo ke liye 5 messages
    if message_count >= 5:
        break

print(f"\n✅ Demo complete! Received {message_count} messages.")
print(f"📊 Check your S3 bucket: https://s3.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}")