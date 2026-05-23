import json
import random
import uuid
import time
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'ecommerce-app-fast',
    
    'security.protocol': 'SSL',
    'ssl.ca.location': 'secrets/ca.crt',
    
    'enable.ssl.certificate.verification': False,
    'ssl.endpoint.identification.algorithm': 'none',
    'broker.address.family': 'v4',
    
    'linger.ms': 50,
    'batch.size': 65536,
    'queue.buffering.max.messages': 500000
}

topic_name = 'raw_transactions_v2' 

def setup_topic():
    admin = AdminClient(conf)
    new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=3)
    
    print(f"Attempting to create topic '{topic_name}' with 3 partitions...")
    fs = admin.create_topics([new_topic])
    
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Success: Topic '{topic}' created optimally for 3 brokers.")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Note: Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic: {e}")

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(5.0, 2500.0), 2),
        "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == '__main__':
    setup_topic()
    
    producer = Producer(conf)
    
    message_count = 0
    
    print("\nStarting high-speed data generation... Press Ctrl+C to stop.")
    
    try:
        while True:
            data = generate_transaction()
            payload = json.dumps(data).encode('utf-8')
            
            producer.produce(topic=topic_name, key=data["transaction_id"], value=payload)
            
            message_count += 1
            if message_count % 10000 == 0:
                print(f"Sent {message_count} messages across the cluster...")
                
            producer.poll(0)

            time.sleep(0.01) 

    except KeyboardInterrupt:
        print(f"\nStopping! Flushing remaining messages in buffer...")
    finally:
        producer.flush()
        print(f"Safely closed. Total generated: {message_count}")