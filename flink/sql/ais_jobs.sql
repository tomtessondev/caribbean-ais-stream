-- Flink SQL jobs on the AIS position stream, submitted by the `flink-jobs` Compose service.
-- Both run in one streaming job (statement set) reading ais.positions once.

-- Window boundaries are written as UTC wall-clock times.
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'ais-port-activity';
-- Checkpoints commit Kafka offsets and flush the JDBC sinks: a restart resumes from there.
SET 'execution.checkpointing.interval' = '30 s';
-- A partition with no traffic (three partitions, a handful of ships) would hold the
-- watermark back forever and no window would ever close: after 30 s of silence a partition
-- stops counting.
SET 'table.exec.source.idle-timeout' = '30 s';

CREATE TABLE positions (
    mmsi        BIGINT,
    ts          TIMESTAMP_LTZ(6),
    lat         DOUBLE,
    lon         DOUBLE,
    sog_kn      DOUBLE,
    cog_deg     DOUBLE,
    heading_deg INT,
    nav_status  INT,
    ship_name   STRING,
    -- Event time is when the ship sent the position, not when Kafka received it.
    event_time AS CAST(ts AS TIMESTAMP_LTZ(3)),
    -- Positions may arrive up to 30 s out of order (radio, relays, reconnections).
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ais.positions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-ais',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Port areas as boxes around each harbour. A CASE expression rather than a join with a
-- table of ports: a regular join would drop the event-time attribute that windows need.
CREATE TEMPORARY VIEW positions_in_port AS
SELECT
    *,
    CASE
        WHEN lat BETWEEN 16.190 AND 16.270 AND lon BETWEEN -61.580 AND -61.480 THEN 'Pointe-a-Pitre'
        WHEN lat BETWEEN 15.955 AND 16.035 AND lon BETWEEN -61.780 AND -61.680 THEN 'Basse-Terre'
        WHEN lat BETWEEN 15.257 AND 15.337 AND lon BETWEEN -61.440 AND -61.340 THEN 'Roseau'
        WHEN lat BETWEEN 14.550 AND 14.640 AND lon BETWEEN -61.120 AND -61.020 THEN 'Fort-de-France'
        WHEN lat BETWEEN 13.975 AND 14.055 AND lon BETWEEN -61.045 AND -60.945 THEN 'Castries'
    END AS port
FROM positions;

CREATE TABLE port_activity (
    port         STRING,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    ships        INT,
    moored       INT,
    positions    INT,
    PRIMARY KEY (port, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'port_activity',
    'username' = 'ais',
    'password' = 'ais'
);

CREATE TABLE ship_hourly (
    mmsi         BIGINT,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    ship_name    STRING,
    positions    INT,
    avg_sog_kn   DOUBLE,
    max_sog_kn   DOUBLE,
    last_lat     DOUBLE,
    last_lon     DOUBLE,
    PRIMARY KEY (mmsi, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'ship_hourly',
    'username' = 'ais',
    'password' = 'ais'
);

EXECUTE STATEMENT SET
BEGIN

-- Ships in each port area, every 5 minutes (tumbling event-time windows).
-- nav_status 1 = at anchor, 5 = moored.
INSERT INTO port_activity
SELECT
    port,
    window_start,
    window_end,
    CAST(COUNT(DISTINCT mmsi) AS INT),
    CAST(COUNT(DISTINCT CASE WHEN nav_status IN (1, 5) THEN mmsi END) AS INT),
    CAST(COUNT(*) AS INT)
FROM TABLE(TUMBLE(TABLE positions_in_port, DESCRIPTOR(event_time), INTERVAL '5' MINUTES))
WHERE port IS NOT NULL
GROUP BY port, window_start, window_end;

-- One row per ship and hour: how much it reported, how fast it went, where it ended up.
INSERT INTO ship_hourly
SELECT
    mmsi,
    window_start,
    window_end,
    LAST_VALUE(ship_name),
    CAST(COUNT(*) AS INT),
    AVG(sog_kn),
    MAX(sog_kn),
    LAST_VALUE(lat),
    LAST_VALUE(lon)
FROM TABLE(TUMBLE(TABLE positions, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
GROUP BY mmsi, window_start, window_end;

END;
