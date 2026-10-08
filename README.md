# bunny-prometheus-exporter

Prometheus exporter for [bunny.net](https://bunny.net). Exposes Magic Containers apps, pull zones and storage zones as metrics on `/metrics`. Every scrape calls the bunny API directly.

## Running

```sh
docker run -p 9877:9877 -e BUNNY_API_KEY=... ghcr.io/modrinth/bunny-prometheus-exporter:latest
```

| Variable        | Default  | Description                           |
| --------------- | -------- | ------------------------------------- |
| `BUNNY_API_KEY` | required | Account API key                       |
| `LISTEN_ADDR`   | `:9877`  | Address the metrics server listens on |

All Magic Containers apps, pull zones and storage zones on the account are discovered automatically.
