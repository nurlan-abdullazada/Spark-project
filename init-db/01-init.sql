-- Create the table that will receive streaming data from Spark
CREATE TABLE IF NOT EXISTS kafka_messages (
    id SERIAL PRIMARY KEY,
    message_key VARCHAR(255),
    message_value TEXT,
    topic VARCHAR(100),
    partition_id INTEGER,
    offset_value BIGINT,
    timestamp_value TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_kafka_messages_topic ON kafka_messages(topic);
CREATE INDEX IF NOT EXISTS idx_kafka_messages_timestamp ON kafka_messages(timestamp_value);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE kafka_messages TO sparkuser;
GRANT USAGE, SELECT ON SEQUENCE kafka_messages_id_seq TO sparkuser;
