# Prometheus Mesos exporter

This is an exporter for Prometheus to get Mesos data.

## Provided metrics

The metrics provided are based on the following Mesos endpoints:

- `/metrics/snapshot` for master nodes
- For slave nodes:
    - `/metrics/snapshot`
    - `/monitor/statistics.json`

For system metrics you can use the [node_exporter](https://github.com/prometheus/node_exporter)

## Building and running

    make
    ./mesos_exporter <flags>

### Flags

Name                           | Description
-------------------------------|------------
web.listen-address             | Address to listen on for web interface and telemetry.
web.telemetry-path             | Path under which to expose metrics.
exporter.discovery             | Enable auto disovery of elected master and active slaves.
exporter.discovery.master-url  | URL to a Mesos master.
exporter.local-url             | URL to the local slave if not using discovery.
exporter.interval              | Interval at which to fetch the Mesos slave metrics.
log.level                      | Only log messages with the given severity or above. Valid levels: debug, info, warn, error, fatal, panic.


### Modes
mesos_exporter can operate in two modes: discovery and local.

In local mode only the IP specified with the commandline flag `-exporter.local-address` will be queried and exported.
This mode is to facilitate having one exporter per Mesos slave.

In discovery mode (`-exporter.discovery`) the Mesos slaves are discovered using the `-exporter.discovery.master` flag. The exporter will fetch
all slave metrics and export them. 
This mode lets you have one exporter per Mesos cluster. But you can also have one exporter for each Mesos master
for failover, and it will automatically use the current elected master.

### Docker

A Docker container is available at
https://registry.hub.docker.com/u/prom/mesos-exporter

If you want to use it with your own configuration, you can mount it as a
volume:

    docker run -d -p 4000:4000 prom/mesos-exporter

It's also possible to use in your own Dockerfile:

    FROM prom/mesos-exporter

---
