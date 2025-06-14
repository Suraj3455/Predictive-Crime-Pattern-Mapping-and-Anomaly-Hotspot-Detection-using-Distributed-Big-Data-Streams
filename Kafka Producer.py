from kafka import KafkaProducer
import pandas as pd
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'crime_reports'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load CSV file
csv_file = 'crime_data.csv'
df = pd.read_csv(csv_file)

# Check required columns
required_cols = {'Date Rptd', 'AREA NAME', 'Weapon Desc', 'Crm Cd Desc'}
if not required_cols.issubset(df.columns):
    raise ValueError(f"CSV file missing required columns: {required_cols - set(df.columns)}")

# Add incremental id column in code
df.insert(0, 'id', range(1, len(df) + 1))

# Convert dataframe to list of dicts
records = df.to_dict(orient='records')

# Batch settings
batch_size = 1000
batch = []

# Send records in batches
for i, record in enumerate(records, 1):
    data = {
        'id': record['id'],
        'date_reported': record['Date Rptd'],
        'area': record['AREA NAME'],
        'crime': record['Crm Cd Desc'],
        'weapon': record['Weapon Desc']
    }
    producer.send(topic_name, data)
    batch.append(data)

    if i % batch_size == 0:
        producer.flush()
        print(f"✅ Sent batch of {batch_size} records (Total sent: {i})")
        batch = []

# Send remaining records
if batch:
    producer.flush()
    print(f"✅ Sent final batch of {len(batch)} records (Total sent: {len(records)})")

# Close the producer
producer.close()
print("🎉 All records sent successfully.")
