# Roadmap

1. **Ingestion into Kafka** (done). aisstream.io WebSocket client with reconnection, AIS
   normalisation (sentinels, padding, dimensions), idempotent keyed producer, compacted
   ships topic, dead-letter topic, replay and synthetic traffic, Docker Compose, tests on a
   real broker.
2. **Flink SQL.** Flink 1.20 with the Kafka connector: event-time windows with watermarks
   (ships per port area every 5 minutes, average speed per corridor), port arrivals and
   departures from geofences, results to PostgreSQL through the JDBC connector.
3. **Anomalies.** Ships going dark (AIS silence longer than their usual rate, session
   windows), speed or position jumps that are physically impossible, loitering.
4. **Map and metrics.** A live map of the ships and port activity (Grafana geomap on the
   Flink outputs), consumer lag and throughput.
5. **Schemas.** Avro with a schema registry, so producers and Flink agree on the records
   and evolve them safely.
