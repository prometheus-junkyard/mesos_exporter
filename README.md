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
