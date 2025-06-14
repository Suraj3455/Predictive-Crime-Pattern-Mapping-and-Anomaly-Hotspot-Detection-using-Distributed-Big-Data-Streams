from kafka import KafkaProducer
import pandas as pd
import json
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load CSV data
df = pd.read_csv("C:/Users/User/OneDrive/Desktop/CrimePatternProject/data/crime_data.csv")

# Iterate and send each row to Kafka topic
for index, row in df.iterrows():
    data = {
        'id': int(index + 1),
        'date_reported': str(row['Date Rptd']),
        'area': str(row['AREA NAME']),
        'crime': str(row['Crm Cd Desc']),
        'weapon': str(row['Weapon Desc'])
    }
    
    producer.send('crime_topic', value=data)
    print(f"✅ Sent: {data}")
    time.sleep(1)  # Optional: control the message flow speed

# Flush and close producer connection
producer.flush()
producer.close()
