-- scripts/cassandra-init.cql
CREATE KEYSPACE IF NOT EXISTS gabes_air
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE gabes_air;

CREATE TABLE IF NOT EXISTS pollution_alerts (
    alert_id UUID PRIMARY KEY,
    sensor_id TEXT,
    alert_timestamp TIMESTAMP,
    pollutant TEXT,
    value DOUBLE,
    threshold DOUBLE,
    alert_level TEXT,
    location_lat DOUBLE,
    location_lon DOUBLE
);