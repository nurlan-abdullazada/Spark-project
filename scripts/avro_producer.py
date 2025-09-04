import json
import struct
import io
import avro.schema
import avro.io
from kafka import KafkaProducer
import time

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
TOPIC_NAME = 'user-topic'
SCHEMA_ID = 1

# Avro schema (matching what we registered)
SCHEMA_JSON = """
{
  "type": "record",
  "name": "User", 
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
"""

def serialize_avro_with_confluent_header(schema_id, schema_str, record):
    """Serialize record with Confluent wire format: magic_byte + schema_id + avro_data"""
    # Parse schema
    schema = avro.schema.parse(schema_str)
    
    # Serialize record to Avro binary
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(record, encoder)
    avro_data = bytes_writer.getvalue()
    
    # Confluent wire format: magic byte (0x0) + 4-byte schema ID + avro data
    magic_byte = b'\x00'
    schema_id_bytes = struct.pack('>I', schema_id)  # Big-endian 4-byte int
    
    return magic_byte + schema_id_bytes + avro_data

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=None  # We handle serialization manually
    )
    
    # Sample user records
    users = [
        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 25},
        {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 30},
        {"id": 3, "name": "Carol Brown", "email": "carol@example.com", "age": 28},
        {"id": 4, "name": "David Wilson", "email": "david@example.com", "age": 35},
        {"id": 5, "name": "Emma Davis", "email": "emma@example.com", "age": 22}
    ]
    
    print(f"Producing {len(users)} Avro messages to topic '{TOPIC_NAME}'...")
    
    for user in users:
        # Serialize with Confluent wire format
        avro_bytes = serialize_avro_with_confluent_header(SCHEMA_ID, SCHEMA_JSON, user)
        
        # Send to Kafka
        producer.send(TOPIC_NAME, value=avro_bytes)
        print(f"Sent Avro message: {user}")
        time.sleep(1)  # Small delay between messages
    
    producer.flush()
    producer.close()
    print("All Avro messages sent successfully!")

if __name__ == "__main__":
    main()
