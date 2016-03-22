package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/antonlindstrom/mesos_stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const concurrentFetch = 100

// Commandline flags.
var (
	addr                 = flag.String("web.listen-address", ":9105", "Address to listen on for web interface and telemetry")
	autoDiscoverInterval = flag.Duration("exporter.discover-interval", 10*time.Minute, "Interval at which to update available slaves from a Mesos Master. Only used if exporter.scrape-mode=discover.")
	queryURL             = flag.String("exporter.url", "", "The URL of a Mesos Slave, if mode=slave. The URL of a Mesos Master, if mode=discover or mode=master.")
	metricsPath          = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics")
	scrapeInterval       = flag.Duration("exporter.interval", 60*time.Second, "Scrape interval duration")
	scrapeMode           = flag.String("exporter.scrape-mode", "", "The mode in which to run the exporter: 'discover', 'master' or 'slave'.")
)

var (
	variableLabels = []string{"task", "slave", "framework_id", "framework_name", "task_name"}

	cpuLimitDesc = prometheus.NewDesc(
		"mesos_task_cpu_limit",
		"Fractional CPU limit.",
		variableLabels, nil,
	)
	cpuSysDesc = prometheus.NewDesc(
		"mesos_task_cpu_system_seconds_total",
		"Cumulative system CPU time in seconds.",
		variableLabels, nil,
	)
	cpuUsrDesc = prometheus.NewDesc(
		"mesos_task_cpu_user_seconds_total",
		"Cumulative user CPU time in seconds.",
		variableLabels, nil,
	)
	memLimitDesc = prometheus.NewDesc(
		"mesos_task_memory_limit_bytes",
		"Task memory limit in bytes.",
		variableLabels, nil,
	)
	memRssDesc = prometheus.NewDesc(
		"mesos_task_memory_rss_bytes",
		"Task memory RSS usage in bytes.",
		variableLabels, nil,
	)

	frameworkLabels = []string{"id", "name"}

	frameworkResourcesUsedCPUs = prometheus.NewDesc(
		"mesos_framework_resources_used_cpus",
		"Fractional CPUs used by a framework.",
		frameworkLabels, nil,
	)

	frameworkResourcesUsedDisk = prometheus.NewDesc(
		"mesos_framework_resources_used_disk_bytes",
		"Disk space used by a framework.",
		frameworkLabels, nil,
	)

	frameworkResourcesUsedMemory = prometheus.NewDesc(
		"mesos_framework_resources_used_memory_bytes",
		"Memory used by a framework.",
		frameworkLabels, nil,
	)

	frameworkActiveTasks = prometheus.NewDesc(
		"mesos_framework_active_tasks",
		"Active tasks launched by a framework.",
		frameworkLabels, nil,
	)

	erroredTasksLabels = []string{"host", "source", "reason"}

	masterTaskErrors = []masterTaskErrorMetric{
		masterTaskErrorMetric{
			prefix: "master/task_failed/",
			desc: prometheus.NewDesc(
				"mesos_master_task_failed_total",
				"Number of tasks failed by reasons.",
				erroredTasksLabels, nil,
			),
		},
		masterTaskErrorMetric{
			prefix: "master/task_killed/",
			desc: prometheus.NewDesc(
				"mesos_master_task_killed_total",
				"Number of tasks killed by reasons.",
				erroredTasksLabels, nil,
			),
		},
		masterTaskErrorMetric{
			prefix: "master/task_lost/",
			desc: prometheus.NewDesc(
				"mesos_master_task_lost_total",
				"Number of tasks lost by reasons.",
				erroredTasksLabels, nil,
			),
		},
	}

	masterMetricsLabels = []string{"host"}

	// http://mesos.apache.org/documentation/latest/monitoring/
	masterMetrics = []snapshotMetric{
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_cpus_fraction",
				"Fraction of allocated CPUs.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/cpus_percent",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_cpus_revocable_fraction",
				"Fraction of allocated revocable CPUs.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/cpus_revocable_percent",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_cpus_revocable",
				"Number of revocable CPUs.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/cpus_revocable_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_cpus_revocable_used",
				"Number of allocated revocable CPUs.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/cpus_revocable_used",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_cpus",
				"Number of CPUs.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/cpus_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_cpus_used",
				"Number of allocated CPUs.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/cpus_used",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_disk_fraction",
				"Fraction of allocated disk space.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/disk_percent",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_disk_revocable_fraction",
				"Fraction of allocated revocable disk space.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/disk_revocable_percent",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_disk_revocable_bytes",
				"Revocable disk space in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/disk_revocable_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_disk_revocable_used_bytes",
				"Allocated revocable disk space in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/disk_revocable_used",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_disk_bytes",
				"Disk space in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/disk_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_disk_used_bytes",
				"Allocated disk space in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/disk_used",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_dropped_messages",
				"Number of dropped messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/dropped_messages",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_elected",
				"Whether this is the elected master.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/elected",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_event_queue_dispatches",
				"Number of dispatches in the event queue.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/event_queue_dispatches",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_event_queue_http_requests",
				"Number of HTTP requests in the event queue.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/event_queue_http_requests",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_event_queue_messages",
				"Number of messages in the event queue.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/event_queue_messages",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_frameworks_active",
				"Number of active frameworks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/frameworks_active",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_frameworks_connected",
				"Number of connected frameworks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/frameworks_connected",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_frameworks_disconnected",
				"Number of disconnected frameworks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/frameworks_disconnected",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_frameworks_inactive",
				"Number of inactive frameworks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/frameworks_inactive",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_invalid_executor_to_framework_messages_total",
				"Number of invalid executor to framework messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/invalid_executor_to_framework_messages",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_invalid_framework_to_executor_messages_total",
				"Number of invalid framework to executor messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/invalid_framework_to_executor_messages",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_invalid_status_update_acknowledgements_total",
				"Number of invalid status update acknowledgements.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/invalid_status_update_acknowledgements",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_invalid_status_updates",
				"Number of invalid status updates.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/invalid_status_updates_total",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_mem_fraction",
				"Fraction of allocated memory.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/mem_percent",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_mem_revocable_fraction",
				"Fraction of allocated revocable memory.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/mem_revocable_percent",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_mem_revocable_bytes",
				"Revocable memory in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/mem_revocable_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_mem_revocable_used_bytes",
				"Allocated revocable memory in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/mem_revocable_used",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_mem_bytes",
				"Memory in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/mem_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_master_mem_used_bytes",
				"Allocated memory in bytes.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/mem_used",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_authenticate_total",
				"Number of authentication messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_authenticate",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_deactivate_framework_total",
				"Number of framework deactivation messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/deactivate_framework",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_decline_offers_total",
				"Number of offers declined.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_decline_offers",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_executor_to_framework_total",
				"Number of executor to framework messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/executor_to_framework",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_exited_executor_total",
				"Number of terminated executor messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/exited_executor",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_framework_to_executor_total",
				"Number of messages from a framework to an executor.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/framework_to_executor",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_kill_task_total",
				"Number of kill task messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_kill_task",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_launch_tasks_total",
				"Number of launch task messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_launch_tasks",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_reconcile_tasks_total",
				"Number of reconcile task messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_reconcile_tasks",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_register_framework_total",
				"Number of framework registration messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_register_framework",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_register_slave_total",
				"Number of slave registration messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_register_slave",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_reregister_framework_total",
				"Number of framework re-registration messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_reregister_framework",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_reregister_slave_total",
				"Number of slave re-registration messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_reregister_slave",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_resource_request_total",
				"Number of resource request messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_resource_request",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_revive_offers_total",
				"Number of offer revival messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_revive_offers",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_status_update_total",
				"Number of status update messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_status_update",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_status_update_acknowledgement_total",
				"Number of status update acknowledgement messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_status_update_acknowledgement",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_suppress_offers_total",
				"Number of suppress offer messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_suppress_offers",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_unregister_framework_total",
				"Number of framework unregistration messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_unregister_framework",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_unregister_slave_total",
				"Number of slave unregistration messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_unregister_slave",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_messages_update_slave_total",
				"Number of update slave messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/messages_update_slave",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_mater_outstanding_offers",
				"Number of outstanding resource offers.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/outstanding_offers",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_mater_outstanding_offers",
				"Number of outstanding resource offers.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/outstanding_offers",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_recovery_slave_removals_total",
				"Number of slaves not re-registered during master failover.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/recovery_slave_removals",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_registrations_total",
				"Number of slaves that were able to cleanly re-join the cluster and connect back to the master after the master is disconnected.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_registrations",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_removals_total",
				"Number of slave removed for various reasons, including maintenance.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_removals",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_removals_reason_registered_total",
				"Number of slaves removed when new slaves registered at the same address.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_removals/reason_registered",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_removals_reason_unhealthy_total",
				"Number of slaves failed due to failed health checks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_removals/reason_unhealthy",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_removals_reason_unregistered_total",
				"Number of slaves unregistered.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_removals/reason_unregistered",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_reregistrations_total",
				"Number of slave re-registrations.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_reregistrations",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_shutdowns_canceled_total",
				"Number of cancelled slave shutdowns. This happens when the slave removal rate limit allows for a slave to reconnect and send a PONG to the master before being removed.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_shutdowns_canceled",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_shutdowns_completed_total",
				"Number of slaves that failed their health check. These are slaves which were not heard from despite the slave-removal rate limit, and have been removed from the master's slave registry.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_shutdowns_completed",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slave_shutdowns_scheduled_total",
				"Number of slaves which have failed their health check and are scheduled to be removed. They will not be immediately removed due to the Slave Removal Rate-Limit, but master/slave_shutdowns_completed will start increasing as they do get removed.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slave_shutdowns_scheduled",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slaves_active",
				"Number of active slaves.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slaves_active",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slaves_connected",
				"Number of connected slaves.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slaves_connected",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slaves_disconnected",
				"Number of disconnected slaves.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slaves_disconnected",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_slaves_inactive",
				"Number of inactive slaves.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/slaves_inactive",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_error_total",
				"Number of tasks that were invalid.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_error",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_failed_total",
				"Number of failed tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_failed",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_finished_total",
				"Number of finished tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_finished",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_killed_total",
				"Number of killed tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_killed",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_lost_total",
				"Number of lost tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_lost",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_running",
				"Number of running tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_running",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_staging",
				"Number of staging tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_staging",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_tasks_starting",
				"Number of starting tasks.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/tasks_starting",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_uptime_secsonds",
				"Uptime in seconds.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/uptime_secs",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_valid_executor_to_framework_messages_total",
				"Number of invalid executor to framework messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/valid_executor_to_framework_messages",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_valid_framework_to_executor_messages_total",
				"Number of invalid framework to executor messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/valid_framework_to_executor_messages",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_valid_status_update_acknowledgements_total",
				"Number of invalid status update acknowledgements.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/valid_status_update_acknowledgements",
			valueType:   prometheus.CounterValue,
		},
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_master_valid_status_updates_total",
				"Number of valid status update messages.",
				masterMetricsLabels, nil,
			),
			snapshotKey: "master/valid_status_updates",
			valueType:   prometheus.CounterValue,
		},
	}

	slaveMetricsLabels = []string{"host"}

	slaveMetrics = []snapshotMetric{
		snapshotMetric{
			desc: prometheus.NewDesc(
				"mesos_slave_cpus",
				"CPUs advertised by a Mesos Slave.",
				slaveMetricsLabels, nil,
			),
			snapshotKey: "slave/cpus_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_slave_disk_bytes",
				"Disk space advertised by a Mesos Slave.",
				slaveMetricsLabels, nil,
			),
			snapshotKey: "slave/disk_total",
			valueType:   prometheus.GaugeValue,
		},
		snapshotMetric{
			convertFn: megabytesToBytes,
			desc: prometheus.NewDesc(
				"mesos_slave_memory_bytes",
				"Memory advertised by a Mesos Slave.",
				slaveMetricsLabels, nil,
			),
			snapshotKey: "slave/mem_total",
			valueType:   prometheus.GaugeValue,
		},
	}
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

type exporterOpts struct {
	autoDiscoverInterval time.Duration
	interval             time.Duration
	mode                 string
	queryURL             string
}

type exporterTaskInfo struct {
	FrameworkName string
	TaskName      string
}

type framework struct {
	ID            string
	Name          string
	UsedResources *resources `json:"used_resources"`
	Tasks         []interface{}
}

type masterState struct {
	Frameworks []*framework
	Hostname   string
	Slaves     []*slave
}

type snapshotMetric struct {
	convertFn   func(float64) float64
	desc        *prometheus.Desc
	snapshotKey string
	valueType   prometheus.ValueType
}

type masterTaskErrorMetric struct {
	prefix string
	desc   *prometheus.Desc
}

type metricsSnapshot map[string]float64

type periodicExporter struct {
	sync.RWMutex
	errors   *prometheus.CounterVec
	metrics  []prometheus.Metric
	opts     *exporterOpts
	queryURL *url.URL
	slaves   struct {
		sync.Mutex
		urls []string
	}
}

type resources struct {
	CPUs float64
	Disk float64
	Mem  float64
}

type slave struct {
	Active   bool
	Hostname string
	PID      string
}

type slaveState struct {
	Frameworks []*slaveStateFramework
}

type slaveStateExecutor struct {
	Tasks []*slaveStateTask
}

type slaveStateFramework struct {
	Name      string
	Executors []*slaveStateExecutor
}

type slaveStateTask struct {
	FrameworkID string `json:"framework_id"`
	ID          string
	Name        string
}

func newMesosExporter(opts *exporterOpts) *periodicExporter {
	e := &periodicExporter{
		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mesos_exporter",
				Name:      "slave_scrape_errors_total",
				Help:      "Current total scrape errors",
			},
			[]string{"slave"},
		),
		opts: opts,
	}

	if opts.queryURL == "" {
		log.Fatal("Flag '-exporter.url' not set")
	}

	switch opts.mode {
	case "discover":
		log.Info("starting mesos_exporter in scrape mode 'discover'")

		e.queryURL = parseMasterURL(opts.queryURL)

		// Update nr. of mesos slaves.
		e.updateSlaves()
		go runEvery(e.updateSlaves, e.opts.autoDiscoverInterval)

		// Fetch slave metrics every interval.
		go runEvery(e.scrapeSlaves, e.opts.interval)
	case "master":
		log.Info("starting mesos_exporter in scrape mode 'master'")
		e.queryURL = parseMasterURL(opts.queryURL)
	case "slave":
		log.Info("starting mesos_exporter in scrape mode 'slave'")
		e.slaves.urls = []string{opts.queryURL}
	default:
		log.Fatalf("Invalid value '%s' of flag '-exporter.mode' - must be one of 'discover', 'master' or 'slave'", opts.mode)
	}

	return e
}

func (e *periodicExporter) Describe(ch chan<- *prometheus.Desc) {
	switch e.opts.mode {
	case "master":
		e.scrapeMaster()
	case "slave":
		e.scrapeSlaves()
	}

	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m.Desc()
		}
	})
	e.errors.MetricVec.Describe(ch)
}

func (e *periodicExporter) Collect(ch chan<- prometheus.Metric) {
	switch e.opts.mode {
	case "master":
		e.scrapeMaster()
	case "slave":
		e.scrapeSlaves()
	}

	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m
		}
	})
	e.errors.MetricVec.Collect(ch)
}

func (e *periodicExporter) fetch(urlChan <-chan string, metricsChan chan<- prometheus.Metric, wg *sync.WaitGroup) {
	defer wg.Done()

	for u := range urlChan {
		u, err := url.Parse(u)
		if err != nil {
			log.Warn("could not parse slave URL: ", err)
			continue
		}

		host, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			log.Warn("could not parse network address: ", err)
			continue
		}

		taskInfo := map[string]exporterTaskInfo{}
		var state slaveState
		stateURL := fmt.Sprintf("%s/state.json", u)

		err = getJSON(&state, stateURL)
		if err != nil {
			log.Warn(err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		for _, fw := range state.Frameworks {
			for _, ex := range fw.Executors {
				for _, t := range ex.Tasks {
					taskInfo[t.ID] = exporterTaskInfo{fw.Name, t.Name}
				}
			}
		}

		monitorURL := fmt.Sprintf("%s/monitor/statistics.json", u)
		var stats []mesos_stats.Monitor

		err = getJSON(&stats, monitorURL)
		if err != nil {
			log.Warn(err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		for _, stat := range stats {
			tinfo, ok := taskInfo[stat.Source]
			if !ok {
				continue
			}

			metricsChan <- prometheus.MustNewConstMetric(
				cpuLimitDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.CpusLimit),
				stat.Source, host, stat.FrameworkId, tinfo.FrameworkName, tinfo.TaskName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				cpuSysDesc,
				prometheus.CounterValue,
				float64(stat.Statistics.CpusSystemTimeSecs),
				stat.Source, host, stat.FrameworkId, tinfo.FrameworkName, tinfo.TaskName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				cpuUsrDesc,
				prometheus.CounterValue,
				float64(stat.Statistics.CpusUserTimeSecs),
				stat.Source, host, stat.FrameworkId, tinfo.FrameworkName, tinfo.TaskName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				memLimitDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.MemLimitBytes),
				stat.Source, host, stat.FrameworkId, tinfo.FrameworkName, tinfo.TaskName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				memRssDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.MemRssBytes),
				stat.Source, host, stat.FrameworkId, tinfo.FrameworkName, tinfo.TaskName,
			)
		}

		metricsSnapshotURL := fmt.Sprintf("%s/metrics/snapshot", u)
		var ms metricsSnapshot

		err = getJSON(&ms, metricsSnapshotURL)
		if err != nil {
			log.Warn(err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		for _, mm := range slaveMetrics {
			metricValue, ok := ms[mm.snapshotKey]
			if !ok {
				continue
			}

			if mm.convertFn != nil {
				metricValue = mm.convertFn(metricValue)
			}

			metricsChan <- prometheus.MustNewConstMetric(
				mm.desc, mm.valueType, metricValue, host,
			)
		}
	}
}

func (e *periodicExporter) rLockMetrics(f func()) {
	e.RLock()
	defer e.RUnlock()
	f()
}

func (e *periodicExporter) setMetrics(ch chan prometheus.Metric) {
	metrics := []prometheus.Metric{}
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	e.Lock()
	e.metrics = metrics
	e.Unlock()
}

func (e *periodicExporter) scrapeMaster() {
	stateURL := fmt.Sprintf("%s://%s/master/state.json", e.queryURL.Scheme, e.queryURL.Host)

	log.Debugf("Scraping master at %s", stateURL)

	var state masterState

	err := getJSON(&state, stateURL)
	if err != nil {
		log.Warn(err)
		return
	}

	metrics := []prometheus.Metric{}

	for _, fw := range state.Frameworks {
		metrics = append(metrics, prometheus.MustNewConstMetric(
			frameworkResourcesUsedCPUs,
			prometheus.GaugeValue,
			fw.UsedResources.CPUs,
			fw.ID, fw.Name,
		))
		metrics = append(metrics, prometheus.MustNewConstMetric(
			frameworkResourcesUsedDisk,
			prometheus.GaugeValue,
			megabytesToBytes(fw.UsedResources.Disk),
			fw.ID, fw.Name,
		))
		metrics = append(metrics, prometheus.MustNewConstMetric(
			frameworkResourcesUsedMemory,
			prometheus.GaugeValue,
			megabytesToBytes(fw.UsedResources.Mem),
			fw.ID, fw.Name,
		))
		metrics = append(metrics, prometheus.MustNewConstMetric(
			frameworkActiveTasks,
			prometheus.GaugeValue,
			float64(len(fw.Tasks)),
			fw.ID, fw.Name,
		))
	}

	snapshotURL := fmt.Sprintf("%s://%s/metrics/snapshot", e.queryURL.Scheme, e.queryURL.Host)

	var ms metricsSnapshot

	err = getJSON(&ms, snapshotURL)
	if err != nil {
		log.Warn(err)
		return
	}

	for _, mm := range masterMetrics {
		metricValue, ok := ms[mm.snapshotKey]
		if !ok {
			continue
		}

		if mm.convertFn != nil {
			metricValue = mm.convertFn(metricValue)
		}

		metrics = append(metrics, prometheus.MustNewConstMetric(
			mm.desc, mm.valueType, metricValue, state.Hostname,
		))
	}

	for k, v := range ms {
		for _, mm := range masterTaskErrors {
			if !strings.HasPrefix(k, mm.prefix) {
				continue
			}

			p := strings.SplitN(strings.TrimPrefix(k, mm.prefix), "/", 2)
			if len(p) != 2 {
				continue
			}

			metrics = append(metrics, prometheus.MustNewConstMetric(
				mm.desc,
				prometheus.CounterValue,
				v,
				state.Hostname, p[0], p[1],
			))
		}
	}

	e.Lock()
	e.metrics = metrics
	e.Unlock()
}

func (e *periodicExporter) scrapeSlaves() {
	e.slaves.Lock()
	urls := make([]string, len(e.slaves.urls))
	copy(urls, e.slaves.urls)
	e.slaves.Unlock()

	urlCount := len(urls)
	log.Debugf("active slaves: %d", urlCount)

	urlChan := make(chan string)
	metricsChan := make(chan prometheus.Metric)
	go e.setMetrics(metricsChan)

	poolSize := concurrentFetch
	if urlCount < concurrentFetch {
		poolSize = urlCount
	}

	log.Debugf("creating fetch pool of size %d", poolSize)

	var wg sync.WaitGroup
	wg.Add(poolSize)
	for i := 0; i < poolSize; i++ {
		go e.fetch(urlChan, metricsChan, &wg)
	}

	for _, url := range urls {
		urlChan <- url
	}
	close(urlChan)

	wg.Wait()
	close(metricsChan)
}

func (e *periodicExporter) updateSlaves() {
	log.Debug("discovering slaves...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s://%s/master/redirect", e.queryURL.Scheme, e.queryURL.Host)
	rReq, err := http.NewRequest("GET", redirectURL, nil)
	if err != nil {
		panic(err)
	}

	tr := http.Transport{
		DisableKeepAlives: true,
	}
	rresp, err := tr.RoundTrip(rReq)
	if err != nil {
		log.Warn(err)
		return
	}
	defer rresp.Body.Close()

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		log.Warnf("%d response missing Location header", rresp.StatusCode)
		return
	}

	log.Debugf("current elected master at: %s", masterLoc)

	// Starting from 0.23.0, a Mesos Master does not set the scheme in the "Location" header.
	// Use the scheme from the master URL in this case.
	var stateURL string
	if strings.HasPrefix(masterLoc, "http") {
		stateURL = fmt.Sprintf("%s/master/state.json", masterLoc)
	} else {
		stateURL = fmt.Sprintf("%s:%s/master/state.json", e.queryURL.Scheme, masterLoc)
	}

	var state masterState

	// Find all active slaves
	err = getJSON(&state, stateURL)
	if err != nil {
		log.Warn(err)
		return
	}

	var slaveURLs []string
	for _, slave := range state.Slaves {
		if slave.Active {
			// Extract slave port from pid
			_, port, err := net.SplitHostPort(slave.PID)
			if err != nil {
				port = "5051"
			}
			url := fmt.Sprintf("http://%s:%s", slave.Hostname, port)

			slaveURLs = append(slaveURLs, url)
		}
	}

	log.Debugf("%d slaves discovered", len(slaveURLs))

	e.slaves.Lock()
	e.slaves.urls = slaveURLs
	e.slaves.Unlock()
}

func getJSON(data interface{}, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(data); err != nil {
		return err
	}

	return nil
}

func megabytesToBytes(v float64) float64 { return v * 1024 * 1024 }

func parseMasterURL(masterURL string) *url.URL {
	parsedMasterURL, err := url.Parse(masterURL)
	if err != nil {
		log.Fatalf("unable to parse master URL '%s': ", masterURL, err)
	}
	if strings.HasPrefix(parsedMasterURL.Scheme, "http") == false {
		log.Fatalf("invalid scheme '%s' in master url - use 'http' or 'https'", parsedMasterURL.Scheme)
	}

	return parsedMasterURL
}

func runEvery(f func(), interval time.Duration) {
	for _ = range time.NewTicker(interval).C {
		f()
	}
}

func main() {
	flag.Parse()

	opts := &exporterOpts{
		autoDiscoverInterval: *autoDiscoverInterval,
		interval:             *scrapeInterval,
		mode:                 *scrapeMode,
		queryURL:             strings.TrimRight(*queryURL, "/"),
	}
	exporter := newMesosExporter(opts)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, *metricsPath, http.StatusMovedPermanently)
	})

	log.Info("starting mesos_exporter on ", *addr)

	log.Fatal(http.ListenAndServe(*addr, nil))
}
