# Images

Screenshots of the Grafana dashboards and the GIF of the map, used in the README and on the
project page. They come from an accelerated replay: at the real pace a 24-knot ferry barely
moves at the scale of the archipelago.

## Redo them

1. `make fast-replay`: a fresh stack (Kafka, Flink, PostgreSQL, Prometheus, Grafana), the
   Flink job, then six hours of synthetic traffic ending now, replayed sixty times faster
   (six minutes).
2. While it plays, capture the map panel alone every second or so, as `f0000.png`,
   `f0001.png`, ... (any headless browser; 960×640, light theme):
   `http://localhost:3002/d-solo/ais-traffic?orgId=1&panelId=1&from=now-6h&to=now&theme=light`.
   Drop the first frames taken before the data arrived.
3. `scripts/frames-to-gif.sh <frames dir> docs/images/ships.gif 3 10 720` (needs ffmpeg).
4. Dashboards, light theme, kiosk mode:
   - `ais-traffic.png` (1600×1060) about 2 min 35 s into the replay, while BLUE MERIDIAN is
     dark: `http://localhost:3002/d/ais-traffic?orgId=1&from=now-6h&to=now&kiosk&theme=light`
   - `stream-health.png` (1600×900) about 5 min 30 s in:
     `http://localhost:3002/d/stream-health?orgId=1&from=now-10m&to=now&kiosk&theme=light`
