-- Results written by the Flink jobs (flink/sql/ais_jobs.sql). Each table has the primary
-- key the job upserts on, so a restarted job rewrites a window instead of duplicating it.

create table port_activity (
    port         text        not null,
    window_start timestamptz not null,
    window_end   timestamptz not null,
    ships        integer     not null,  -- distinct ships seen in the port area
    moored       integer     not null,  -- of which reporting "moored" or "at anchor"
    positions    integer     not null,
    primary key (port, window_start)
);

create table ship_hourly (
    mmsi         bigint      not null,
    window_start timestamptz not null,
    window_end   timestamptz not null,
    ship_name    text,
    positions    integer     not null,
    avg_sog_kn   double precision,
    max_sog_kn   double precision,
    last_lat     double precision,
    last_lon     double precision,
    primary key (mmsi, window_start)
);

-- A ship's reporting session: positions no more than 15 minutes apart. A row is written 15
-- minutes after the last position, which is the "went dark" alert.
create table ship_sessions (
    mmsi          bigint      not null,
    session_start timestamptz not null,
    last_seen     timestamptz not null,  -- last position before the silence
    ship_name     text,
    last_lat      double precision,
    last_lon      double precision,
    primary key (mmsi, session_start)
);

-- Two consecutive positions of a ship more than 15 minutes apart, written when the ship
-- reports again.
create table ais_gaps (
    mmsi           bigint      not null,
    silent_from    timestamptz not null,
    silent_until   timestamptz not null,
    silent_minutes double precision not null,
    from_lat       double precision,
    from_lon       double precision,
    to_lat         double precision,
    to_lon         double precision,
    ship_name      text,
    primary key (mmsi, silent_from)
);

-- Every silence: when it started and where, and when it ended (null while still dark).
create view dark_periods as
select
    s.mmsi,
    s.ship_name,
    s.last_seen as silent_from,
    g.silent_until,
    g.silent_minutes,
    s.last_lat,
    s.last_lon
from ship_sessions s
left join ais_gaps g on g.mmsi = s.mmsi and g.silent_from = s.last_seen;

-- Two consecutive positions of a ship too far apart for the time between them (> 1 nm at
-- an implied speed > 60 knots): a spoofed or corrupted position.
create table position_jumps (
    mmsi             bigint      not null,
    from_time        timestamptz not null,
    to_time          timestamptz not null,
    from_lat         double precision,
    from_lon         double precision,
    to_lat           double precision,
    to_lon           double precision,
    distance_nm      double precision not null,
    implied_speed_kn double precision not null,
    ship_name        text,
    primary key (mmsi, to_time)
);

-- A ship entering a port area (arrival) or leaving it (departure).
create table port_events (
    mmsi       bigint      not null,
    event_time timestamptz not null,
    port       text        not null,
    event      text        not null check (event in ('arrival', 'departure')),
    ship_name  text,
    sog_kn     double precision,
    primary key (mmsi, event_time)
);
