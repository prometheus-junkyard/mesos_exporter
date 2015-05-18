# Prometheus Mesos exporter

This is an exporter for Prometheus to get Mesos data.

## Building and running

    make
    ./mesos_exporter <flags>

### Flags

Name                       | Description
---------------------------|------------
web.listen-address         | Address to listen on for web interface and telemetry.
web.telemetry-path         | Path under which to expose metrics.
exporter.discovery         | Enable auto disovery of elected master and active slaves.
exporter.discovery.master  | URL to a Mesos master.
exporter.local-address     | Address to connect to the local slave if not using discovery.
exporter.interval          | Interval at which to fetch the Mesos slave metrics.


The mesos_exporter uses the [glog](https://godoc.org/github.com/golang/glog) library for logging. With the default
parameters, nothing will be logged. Use `-logtostderr` to enable logging to
stderr and `--help` to see more options about logging.

### Modes
mesos_exporter can operate in two modes: discovery and local.

In local mode only the IP specified with the commandline flag `-exporter.local-address` will be queried and exported.
This mode is to facilitate having one exporter per Mesos slave.

In discovery mode the Mesos slaves are discovered using the `-exporter.discovery.master` flag. The exporter will fetch
all slave metrics and export them. 
This mode lets you have one exporter per Mesos cluster.


### Docker

A Docker container is available at
https://registry.hub.docker.com/u/antonlindstrom/mesos-exporter/

If you want to use it with your own configuration, you can mount it as a
volume:

    docker run -d -p 4000:4000 antonlindstrom/mesos-exporter

It's also possible to use in your own Dockerfile:

    FROM antonlindstrom/mesos-exporter

---
