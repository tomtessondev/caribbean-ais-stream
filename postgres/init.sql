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
