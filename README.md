# Prometheus Mesos exporter

This is an exporter for Prometheus to get Mesos data. 

**This is deprecated, https://github.com/mesosphere/mesos_exporter is recommended instead.**

## Building and running

    make
    ./mesos_exporter <flags>

### Flags

Name                           | Description
-------------------------------|------------
web.listen-address             | Address to listen on for web interface and telemetry.
web.telemetry-path             | Path under which to expose metrics.
exporter.discover-interval     | Interval at which to update available slaves from a Mesos Master. Only used if exporter.scrape-mode=discover.
exporter.interval              | Interval at which to fetch the Mesos slave metrics. Only used if exporter.scrape-mode=discover.
exporter.scrape-mode           | The mode in which to run the exporter: 'discover', 'master' or 'slave'.
exporter.url                   | The URL of a Mesos Slave, if exporter.scrape-mode=slave. The URL of a Mesos Master, if exporter.scrape-mode=discover or exporter.scrape-mode=master.
log.level                      | Only log messages with the given severity or above. Valid levels: debug, info, warn, error, fatal, panic.

### Scrape Modes

mesos_exporter can operate in three different modes: `discover`, `master` or `slave`.
The scrape mode is specified by setting the flag `-exporter.scrape-mode=<MODE>`.

#### discover

Scrape metrics of tasks running on Mesos Slaves exposed via the `/monitor/statistics.json` endpoint as well as metrics
exposed by each Mesos Slave itself via the `/metrics/snapshot` endpoint.

mesos_exporter will periodically call a Mesos Master to discover newly registered Mesos Slaves and remove Mesos Slaves
that have deregistered.

**Note:** When starting mesos_exporter in this mode, the process can run anywhere and does not need to run alongside a
Mesos Master process.

##### Example

    ./mesos_exporter -exporter.discover-interval=60s -exporter.interval=15s -exporter.scrape-mode=discover -exporter.url=http://mesos.master:5050

#### master

Scrape metrics exposed by one Mesos Master via the `/metrics/snapshot` endpoint and the `/master/state.json` endpoint.

**Note:** When starting mesos_exporter in this mode, the process should run on the same node as the Mesos Master it
queries.

##### Example

    ./mesos_exporter -exporter.scrape-mode=master -exporter.url=http://127.0.0.1:5050

#### slave

Scrape metrics of tasks running on one Mesos Slave (via the `/monitor/statistics.json` endpoint) as well as metrics
exposed by the Mesos Slave itself via the `/metrics/snapshot` endpoint.

**Note:** When starting mesos_exporter in this mode, the process should run on the same node as the Mesos Slave it
queries.

##### Example

    ./mesos_exporter -exporter.scrape-mode=slave -exporter.url=http://127.0.0.1:5051

### Docker

A Docker container is available at
https://registry.hub.docker.com/u/prom/mesos-exporter

If you want to use it with your own configuration, you can mount it as a
volume:

    docker run -d -p 4000:4000 prom/mesos-exporter

It's also possible to use in your own Dockerfile:

    FROM prom/mesos-exporter

---
