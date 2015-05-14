# Prometheus Mesos exporter

This is an exporter for Prometheus to get Mesos data.
The mesos_exporter gets all active slaves from the Mesos master every 10 minutes. 

## Building and running

    make
    ./mesos_exporter <flags>

### Flags

Name                   | Description
-----------------------|------------
web.listen-address     | Address to listen on for web interface and telemetry.
web.telemetry-path     | Path under which to expose metrics.
exporter.mesos-master  | URL to a Mesos master. 
exporter.interval      | Interval at which to fetch the Mesos slave metrics.

The mesos_exporter uses the [glog](https://godoc.org/github.com/golang/glog) library for logging. With the default
parameters, nothing will be logged. Use `-logtostderr` to enable logging to
stderr and `--help` to see more options about logging.


### Docker

A Docker container is available at
https://registry.hub.docker.com/u/antonlindstrom/mesos-exporter/

If you want to use it with your own configuration, you can mount it as a
volume:

    docker run -d -v /root/config.json:/config.json -p 4000:4000 antonlindstrom/mesos-exporter

It's also possible to use in your own Dockerfile:

    FROM antonlindstrom/mesos-exporter
    ADD config.json /config.json

---
