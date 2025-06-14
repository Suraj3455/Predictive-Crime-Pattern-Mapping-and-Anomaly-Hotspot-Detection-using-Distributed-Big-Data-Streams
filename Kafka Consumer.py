from kafka import KafkaConsumer
import json
import mysql.connector
import pandas as pd

db = mysql.connector.connect(
    host="database-1.cds2mw260a4s.ap-south-1.rds.amazonaws.com",
    user="admin",
    password="dUVrohpx4MdG1xqdmdW8",
    database="crime_db"
)
cursor = db.cursor()

consumer = KafkaConsumer(
    'crime_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

batch_size = 1000
insert_query = """
    INSERT INTO crime_reports (id, date_reported, area, crime, weapon)
    VALUES (%s, %s, %s, %s, %s)
"""

print("✅ Consumer started, waiting for data...")

while True:
    raw_msgs = consumer.poll(timeout_ms=1000, max_records=batch_size)
    batch_data = []

    for topic_partition, messages in raw_msgs.items():
        for message in messages:
            crime = message.value
            date_reported = pd.to_datetime(crime['date_reported'], errors='coerce')
            data_tuple = (
                crime['id'],
                date_reported.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(date_reported) else None,
                crime['area'],
                crime['crime'],
                crime['weapon']
            )
            batch_data.append(data_tuple)

    if batch_data:
        try:
            cursor.executemany(insert_query, batch_data)
            db.commit()
            print(f"✅ Inserted {len(batch_data)} records")
        except Exception as e:
            print("❌ Error inserting batch:", e)
            db.rollback()
