# Prometheus Mesos exporter

This is an exporter for Prometheus to get Mesos data.

The configuration, `config.json` should contain all Mesos slaves you want to
monitor.

Prometheus configuration to add this exporter:

    job: {
      name: "mesos-exporter"
      scrape_interval: "5s"

      target_group: {
        target: "http://mesosexporter.example.com/metrics"
      }
    }

Where `http://mesosexporter.example.com/metrics` is the URL to this exporter.

Building the binary can be done by running `make`.

Run the binary with the following command:

    ./bin/mesos_exporter -config-file=config.json

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
