# Roadmap

1. **Ingestion into Kafka** (done). aisstream.io WebSocket client with reconnection, AIS
   normalisation (sentinels, padding, dimensions), idempotent keyed producer, compacted
   ships topic, dead-letter topic, replay and synthetic traffic, Docker Compose, tests on a
   real broker.
2. **Flink SQL** (done). Flink 1.20 with the Kafka and JDBC connectors: event-time windows
   with watermarks and idle-partition handling, ships per port area every 5 minutes, one
   row per ship and hour, upserted into PostgreSQL; end-to-end test in CI.
3. **Anomalies** (done). Ships going dark (15-minute session windows, and the end of the
   silence by pattern matching), physically impossible position jumps (`MATCH_RECOGNIZE`,
   over 1 nm at over 60 knots), port arrivals and departures (pattern matching on the port
   areas); a spoofed position in the synthetic traffic. Left for later: loitering, and a
   silence threshold adapted to each ship's own reporting rate.
4. **Map and metrics** (done). Grafana provisioned as code: a geomap of the latest position
   of each ship with its anomalies, port activity and events; Prometheus with a Kafka
   exporter and Flink's reporter for throughput, consumer lag and watermark delay;
   screenshots and a GIF from an accelerated replay.
5. **Schemas.** Avro with a schema registry, so producers and Flink agree on the records
   and evolve them safely.
