-- Flink SQL jobs on the AIS position stream, submitted by the `flink-jobs` Compose service.
-- All run in one streaming job (statement set) reading ais.positions once.

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

CREATE TABLE ship_sessions (
    mmsi          BIGINT,
    session_start TIMESTAMP(3),
    last_seen     TIMESTAMP(3),
    ship_name     STRING,
    last_lat      DOUBLE,
    last_lon      DOUBLE,
    PRIMARY KEY (mmsi, session_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'ship_sessions',
    'username' = 'ais',
    'password' = 'ais'
);

CREATE TABLE ais_gaps (
    mmsi           BIGINT,
    silent_from    TIMESTAMP(3),
    silent_until   TIMESTAMP(3),
    silent_minutes DOUBLE,
    from_lat       DOUBLE,
    from_lon       DOUBLE,
    to_lat         DOUBLE,
    to_lon         DOUBLE,
    ship_name      STRING,
    PRIMARY KEY (mmsi, silent_from) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'ais_gaps',
    'username' = 'ais',
    'password' = 'ais'
);

CREATE TABLE position_jumps (
    mmsi             BIGINT,
    from_time        TIMESTAMP(3),
    to_time          TIMESTAMP(3),
    from_lat         DOUBLE,
    from_lon         DOUBLE,
    to_lat           DOUBLE,
    to_lon           DOUBLE,
    distance_nm      DOUBLE,
    implied_speed_kn DOUBLE,
    ship_name        STRING,
    PRIMARY KEY (mmsi, to_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'position_jumps',
    'username' = 'ais',
    'password' = 'ais'
);

CREATE TABLE port_events (
    mmsi       BIGINT,
    event_time TIMESTAMP(3),
    port       STRING,
    event      STRING,
    ship_name  STRING,
    sog_kn     DOUBLE,
    PRIMARY KEY (mmsi, event_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'port_events',
    'username' = 'ais',
    'password' = 'ais'
);

CREATE TABLE ship_positions (
    mmsi        BIGINT,
    ts          TIMESTAMP(3),
    lat         DOUBLE,
    lon         DOUBLE,
    sog_kn      DOUBLE,
    cog_deg     DOUBLE,
    heading_deg INT,
    nav_status  INT,
    ship_name   STRING,
    port        STRING,
    PRIMARY KEY (mmsi) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/ais',
    'table-name' = 'ship_positions',
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

-- Ships going dark: a session window per ship closes after 15 minutes without a position,
-- so its row is the alert, written 15 minutes after the last report. Ships under way report
-- every few seconds and moored ones every 3 minutes: 15 minutes is at least five missed
-- reports. Window deduplication keeps the session's last position (an aggregation could
-- not: session windows merge, and LAST_VALUE cannot be merged).
INSERT INTO ship_sessions
SELECT mmsi, window_start, CAST(event_time AS TIMESTAMP(3)), ship_name, lat, lon
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY mmsi, window_start, window_end ORDER BY event_time DESC
        ) AS rn
    FROM TABLE(SESSION(TABLE positions PARTITION BY mmsi, DESCRIPTOR(event_time), INTERVAL '15' MINUTES))
)
WHERE rn = 1;

-- The end of a silence: two consecutive positions of a ship more than 15 minutes apart,
-- written as soon as the ship reports again. (The session it starts is only written when
-- it ends, possibly days later.)
INSERT INTO ais_gaps
SELECT mmsi, silent_from, silent_until, silent_minutes, from_lat, from_lon, to_lat, to_lon, ship_name
FROM positions
MATCH_RECOGNIZE (
    PARTITION BY mmsi
    ORDER BY event_time
    MEASURES
        CAST(A.event_time AS TIMESTAMP(3)) AS silent_from,
        CAST(B.event_time AS TIMESTAMP(3)) AS silent_until,
        CAST(TIMESTAMPDIFF(SECOND, A.event_time, B.event_time) AS DOUBLE) / 60 AS silent_minutes,
        A.lat AS from_lat,
        A.lon AS from_lon,
        B.lat AS to_lat,
        B.lon AS to_lon,
        B.ship_name AS ship_name
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST B
    PATTERN (A B)
    -- Same rule as the session window, to the millisecond: every closed session gets its gap.
    DEFINE B AS B.event_time > A.event_time + INTERVAL '15' MINUTE
);

-- Physically impossible jumps (spoofed or corrupted positions): two consecutive positions
-- more than 1 nm apart at an implied speed above 60 knots, faster than any ship in the area
-- (fast ferries do about 40). The 1 nm floor keeps GPS noise over short intervals out.
-- Distance in nautical miles, flat-earth approximation (exact enough over these distances):
-- 1 degree of latitude = 60 nm, a degree of longitude shrinks with cos(latitude). DEFINE
-- cannot refer to MEASURES, so the formula is written twice. The elapsed time is at least
-- 1 s, for two positions with the same timestamp. SKIP TO LAST B lets a spoofed position
-- be the end of one jump and the start of the next (out, then back).
INSERT INTO position_jumps
SELECT
    mmsi, from_time, to_time, from_lat, from_lon, to_lat, to_lon,
    distance_nm, distance_nm * 3600 / elapsed_s, ship_name
FROM positions
MATCH_RECOGNIZE (
    PARTITION BY mmsi
    ORDER BY event_time
    MEASURES
        CAST(A.event_time AS TIMESTAMP(3)) AS from_time,
        CAST(B.event_time AS TIMESTAMP(3)) AS to_time,
        A.lat AS from_lat,
        A.lon AS from_lon,
        B.lat AS to_lat,
        B.lon AS to_lon,
        SQRT(
            POWER((B.lat - A.lat) * 60, 2)
            + POWER((B.lon - A.lon) * 60 * COS(RADIANS((A.lat + B.lat) / 2)), 2)
        ) AS distance_nm,
        CAST(GREATEST(TIMESTAMPDIFF(SECOND, A.event_time, B.event_time), 1) AS DOUBLE) AS elapsed_s,
        B.ship_name AS ship_name
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST B
    PATTERN (A B)
    DEFINE B AS
        SQRT(
            POWER((B.lat - A.lat) * 60, 2)
            + POWER((B.lon - A.lon) * 60 * COS(RADIANS((A.lat + B.lat) / 2)), 2)
        ) > 1
        AND SQRT(
            POWER((B.lat - A.lat) * 60, 2)
            + POWER((B.lon - A.lon) * 60 * COS(RADIANS((A.lat + B.lat) / 2)), 2)
        ) * 3600 / GREATEST(TIMESTAMPDIFF(SECOND, A.event_time, B.event_time), 1) > 60
);

-- Port arrivals and departures: a ship's position moving from outside every port area into
-- one (arrival, at the first position inside) or out of one (departure, at the first
-- position outside). A ship first seen inside a port gets no arrival.
INSERT INTO port_events
SELECT mmsi, event_time, port, 'arrival', ship_name, sog_kn
FROM positions_in_port
MATCH_RECOGNIZE (
    PARTITION BY mmsi
    ORDER BY event_time
    MEASURES
        CAST(INSIDE.event_time AS TIMESTAMP(3)) AS event_time,
        INSIDE.port AS port,
        INSIDE.ship_name AS ship_name,
        INSIDE.sog_kn AS sog_kn
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST INSIDE
    PATTERN (OUTSIDE INSIDE)
    DEFINE
        OUTSIDE AS OUTSIDE.port IS NULL,
        INSIDE AS INSIDE.port IS NOT NULL
)
UNION ALL
SELECT mmsi, event_time, port, 'departure', ship_name, sog_kn
FROM positions_in_port
MATCH_RECOGNIZE (
    PARTITION BY mmsi
    ORDER BY event_time
    MEASURES
        CAST(OUTSIDE.event_time AS TIMESTAMP(3)) AS event_time,
        INSIDE.port AS port,
        OUTSIDE.ship_name AS ship_name,
        OUTSIDE.sog_kn AS sog_kn
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST OUTSIDE
    PATTERN (INSIDE OUTSIDE)
    DEFINE
        INSIDE AS INSIDE.port IS NOT NULL,
        OUTSIDE AS OUTSIDE.port IS NULL
);

-- The latest position of each ship, for the map: an upsert of every position. The JDBC
-- sink keeps the last row per ship between flushes (every second), so PostgreSQL sees at
-- most one write per ship and second. A position arriving out of order can show an older
-- point until the ship's next report, seconds later: harmless on a map.
INSERT INTO ship_positions
SELECT
    mmsi, CAST(event_time AS TIMESTAMP(3)), lat, lon, sog_kn, cog_deg, heading_deg,
    nav_status, ship_name, port
FROM positions_in_port;

END;
