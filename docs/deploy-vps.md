# Deploying the live pipeline on a VPS with Coolify

`docker-compose.vps.yml` runs the live pipeline only: Kafka (KRaft), Schema Registry, the
aisstream.io producer, Flink and its job submitter, PostgreSQL, the site's endpoint,
Prometheus with the Kafka exporter, and Grafana. No demo producer, no Kafka UI, and no port
published on the host: Coolify's Traefik reaches `grafana` and `api` over the proxy network
Coolify attaches, and serves them over HTTPS with Let's Encrypt.

| URL | Service | Container port |
| --- | --- | --- |
| `https://ais.horizon-studio.dev` | `grafana` (anonymous read-only viewer) | 3000 |
| `https://ais.horizon-studio.dev/v1/...` | `api` (`/v1/ships`, `/v1/anomalies`) | 8000 |

## Coolify settings

1. Project **Démos**, environment `production`: **+ New** → **Public/Private Repository**
   (the GitHub app or the public URL of this repository), branch `main`.
2. Build pack: **Docker Compose**. Base directory `/`, Docker Compose location
   `/docker-compose.vps.yml`. Save: Coolify parses the file and lists its services.
3. Domains, per service (the port after the domain tells Traefik which container port to
   use; it is not opened on the host):
   - `grafana`: `https://ais.horizon-studio.dev:3000`
   - `api`: `https://ais.horizon-studio.dev:8000/v1`
   - every other service: no domain.
4. On `api`, leave **Strip Prefixes** off: the endpoint serves `/v1/ships`, so it must
   receive the path unchanged. Traefik gives the longer rule (host + `/v1`) priority over
   Grafana's host-only rule.
5. Environment variables: Coolify generates the `SERVICE_PASSWORD_*` values itself on the
   first deployment (`POSTGRES`, `GRAFANADB`, `SITEREADER`, `GRAFANAADMIN`) and keeps them;
   do not set them. The one variable to enter is **`AISSTREAM_API_KEY`** (the aisstream.io
   key; mark it as secret so it is not shown in the build logs).
6. Deploy. The first build takes a few minutes (the Flink image downloads its connector
   jars). `topics`, `topic-retention` and `db-schema` run once and exit with code 0; that
   is expected, they run again on every deployment.

The Grafana admin password is `SERVICE_PASSWORD_GRAFANAADMIN` (user `admin`), shown in the
resource's environment variables. Grafana only reads it when its database is created: after
that, change it in Grafana or remove the `grafana-data` volume.

## Memory budget

Every container has a limit; the JVM heaps are set to fit inside it.

| Service | Limit | Inside |
| --- | --- | --- |
| kafka | 768 MB | heap 448 MB |
| schema-registry | 384 MB | heap 128 to 256 MB |
| flink-jobmanager | 640 MB | Flink process 576 MB |
| flink-taskmanager | 1 GB | Flink process 960 MB, managed memory 64 MB |
| flink-jobs | 320 MB | a shell loop; the SQL client (heap 192 MB) only while it submits |
| postgres | 256 MB | |
| grafana | 256 MB | about 150 MB idle |
| prometheus | 192 MB | |
| live, api | 128 MB each | |
| kafka-exporter | 64 MB | |
| topics, topic-retention, db-schema | 128, 256, 64 MB | one-shot, exited after start |

That is about 4.1 GB of limits for the long-running services; the steady use is well
below, about 2 GB measured on a local run with the live feed (Kafka about 490 MB, the Flink
task manager about 500 MB, the job manager about 430 MB).

Disk: `ais.positions` and `ais.dlq` are kept 3 days (set by `topic-retention`, the code
creates them with a week and two weeks), and every deleting partition is capped at 256 MB
by the broker; `ais.ships` is compacted (one record per ship). Prometheus keeps 7 days,
at most 1 GB.

## After a reboot

Docker restarts every long-running service (`restart: unless-stopped`). The Flink job
manager keeps no job across a restart, so `flink-jobs` is not a one-shot submission: it
checks the cluster every minute and submits `flink/sql/ais_jobs.sql` again when no job is
alive. The job re-reads `ais.positions` from the earliest offset; the sinks upsert, so the
tables come out the same. The SQL file carries the local password of the JDBC sinks
(`ais`); the submitter swaps in `SERVICE_PASSWORD_POSTGRES` in a copy inside its
container and keeps the client output out of the logs.

## Checks on the VPS

Nothing of this stack should listen on a public interface:

```bash
sudo ss -tlnp
```

Only Traefik (80, 443), SSH and Coolify's own ports should appear; no 3000, 5432, 8000,
8081, 9090 or 9092.

```bash
curl -s https://ais.horizon-studio.dev/v1/ships | head -c 300
curl -s -o /dev/null -w '%{http_code}\n' https://ais.horizon-studio.dev/api/health
docker stats --no-stream
```

## Backup

The tables are derived from Kafka, but a dump keeps the history beyond the 3 days of
retention. For the nightly backup script (the container name is in `docker ps`, it ends
with the resource's UUID):

```bash
docker exec <postgres container> pg_dump -U ais -Fc ais > ais-$(date +%F).dump
```
