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
	"github.com/bolcom/mesos_metrics"
)

const concurrentFetch = 100

// Commandline flags.
var (
	addr = flag.String("web.listen-address", ":9105", "Address to listen on for web interface and telemetry")
	autoDiscover = flag.Bool("exporter.discovery", false, "Discover all Mesos slaves")
	localURL = flag.String("exporter.local-url", "http://127.0.0.1:5051", "URL to the local Mesos slave")
	masterURL = flag.String("exporter.discovery.master-url", "http://mesos-master.example.com:5050", "Mesos master URL")
	metricsPath = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics")
	scrapeInterval = flag.Duration("exporter.interval", (60 * time.Second), "Scrape interval duration")
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

type exporterOpts struct {
	autoDiscover bool
	interval     time.Duration
	localURL     string
	masterURL    string
}

const (
	MasterNode = 1
	SlaveNode = 2
)

type node struct {
	url  string
	Type int
}

type periodicExporter struct {
	sync.RWMutex
	errors  *prometheus.CounterVec
	metrics []prometheus.Metric
	opts    *exporterOpts

	nodes   struct {
				sync.Mutex
				nodes []node
			}
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
	e.nodes.nodes = []node{node{url: e.opts.localURL, Type: SlaveNode}}

	if e.opts.autoDiscover {
		log.Info("auto discovery enabled from command line flag.")

		// Update nr. of mesos nodes every 10 minutes
		e.updateNodes()
		go runEvery(e.updateNodes, 10*time.Minute)
	}

	// Fetch node metrics every interval
	e.scrapeNodes()
	go runEvery(e.scrapeNodes, e.opts.interval)

	return e
}

func (e *periodicExporter) Describe(ch chan <- *prometheus.Desc) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m.Desc()
		}
	})
	e.errors.MetricVec.Describe(ch)
}

func (e *periodicExporter) Collect(ch chan <- prometheus.Metric) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m
		}
	})
	e.errors.MetricVec.Collect(ch)
}

func (e *periodicExporter) fetchMasterMetrics(urlChan <-chan string, metricsChan chan <- prometheus.Metric, wg *sync.WaitGroup) {
	defer wg.Done()

	for u := range urlChan {
		u, err := url.Parse(u)
		if err != nil {
			log.Error("could not parse master URL: ", err)
			continue
		}

		host, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			log.Error("could not parse network address: ", err)
			continue
		}
		e.fetchMasterMetricsSnapshot(u, host, metricsChan)
	}
}

func (e *periodicExporter) fetchSlaveMetrics(urlChan <-chan string, metricsChan chan <- prometheus.Metric, wg *sync.WaitGroup) {
	defer wg.Done()

	for u := range urlChan {
		u, err := url.Parse(u)
		if err != nil {
			log.Error("could not parse slave URL: ", err)
			continue
		}

		host, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			log.Error("could not parse network address: ", err)
			continue
		}
		e.fetchNetworkStatistics(u, host, metricsChan)
		e.fetchSlaveMetricsSnapshot(u, host, metricsChan)
	}
}

func (e *periodicExporter) fetchNetworkStatistics(url *url.URL, host string, metricsChan chan <- prometheus.Metric) {
	monitorURL := fmt.Sprintf("%s/monitor/statistics.json", url)
	resp, err := httpClient.Get(monitorURL)
	if err != nil {
		log.Warn(err)
		e.errors.WithLabelValues(host).Inc()
		return
	}
	defer resp.Body.Close()

	var stats []mesos_stats.Monitor
	if err = json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		log.Warn("failed to deserialize response: ", err)
		e.errors.WithLabelValues(host).Inc()
		return
	}
	labels := []string{"task", "slave", "framework_id"}
	for _, stat := range stats {
		labelValues := []string{stat.Source, host, stat.FrameworkId}
		e.gauge(metricsChan, labels, labelValues, "mesos_task_cpu_limit", float64(stat.Statistics.CpusLimit), "Fractional CPU limit.")
		e.counter(metricsChan, labels, labelValues, "mesos_task_cpu_system_seconds_total", float64(stat.Statistics.CpusSystemTimeSecs), "Cumulative system CPU time in seconds.")
		e.counter(metricsChan, labels, labelValues, "mesos_task_cpu_user_seconds_total", float64(stat.Statistics.CpusUserTimeSecs), "Cumulative system CPU time in seconds.")
		e.gauge(metricsChan, labels, labelValues, "mesos_task_memory_limit_bytes", float64(stat.Statistics.MemLimitBytes), "Task memory limit in bytes.")
		e.gauge(metricsChan, labels, labelValues, "mesos_task_memory_rss_bytes", float64(stat.Statistics.MemRssBytes), "Task memory RSS usage in bytes.")
	}
}

func (e *periodicExporter) fetchSlaveMetricsSnapshot(url *url.URL, host string, metricsChan chan <- prometheus.Metric) {
	monitorURL := fmt.Sprintf("%s/metrics/snapshot", url)
	resp, err := httpClient.Get(monitorURL)
	if err != nil {
		log.Warn(err)
		e.errors.WithLabelValues(host).Inc()
		return
	}
	defer resp.Body.Close()

	var stats mesos_metrics.SlaveMetrics
	if err = json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		log.Warn("failed to deserialize response: ", err)
		e.errors.WithLabelValues(host).Inc()
		return
	}
	labels := []string{"node"}
	labelValues := []string{host}
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_cpus_available", float64(stats.CpuTotal), "Amount of CPUs available as resource")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_cpus_used", stats.CpuUsed, "Amount of CPU resources in use")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_cpus_used_percentage", stats.CpuPercentage, "Percentage of CPU resources in use")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_disk_available_bytes", float64(stats.DiskTotal), "Available bytes of disk space as resource")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_disk_used_bytes", float64(stats.DiskUsed), "Bytes of disk space resources in use")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_disk_used_percentage", float64(stats.DiskPercentage), "Percentage of disk space resources in use")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_mem_total_bytes", float64(stats.MemoryTotal), "Available bytes of memory available as resource")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_mem_used_bytes", float64(stats.MemoryUsed), "Bytes of memory resources in use")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_resource_mem_used_percentage", float64(stats.MemoryPercentage), "Percentage of memory resources in use")

	e.gauge(metricsChan, labels, labelValues, "mesos_slave_executors_registering", float64(stats.ExecutorsRegistering), "Amount of executors that are registering")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_executors_running", float64(stats.ExecutorsRunning), "Amount of executors that are running")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_executors_terminated_total", float64(stats.ExecutorsTerminated), "Total of terminated executors")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_executors_terminating", float64(stats.ExecutorsTerminating), "Amount of executors that are terminating")

	e.gauge(metricsChan, labels, labelValues, "mesos_slave_frameworks_active", float64(stats.FrameworksActive), "Amount of active frameworks")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_invalid_framework_messages_total", float64(stats.InvalidFrameworkMessages), "Total of invalid framework messages")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_invalid_status_updates_total", float64(stats.InvalidStatusUpdates), "Total of invalid status updates")

	e.counter(metricsChan, labels, labelValues, "mesos_slave_recovery_errors_total", float64(stats.RecoveryErrors), "Amount of errors during slave recovery")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_registered", float64(stats.Registered), "Is slave registered with the master")

	e.counter(metricsChan, labels, labelValues, "mesos_slave_tasks_failed_total", float64(stats.TasksFailed), "Total of failed tasks")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_tasks_finished_total", float64(stats.TasksFinished), "Total of finished tasks")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_tasks_killed_total", float64(stats.TasksKilled), "Total of killed tasks")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_tasks_lost_total", float64(stats.TasksLost), "Total of lost tasks")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_tasks_running", float64(stats.TasksRunning), "Amount of running tasks")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_tasks_staging", float64(stats.TasksStaging), "Amount of staging tasks")
	e.gauge(metricsChan, labels, labelValues, "mesos_slave_tasks_starting", float64(stats.TasksStarting), "Amount of starting tasks")

	e.counter(metricsChan, labels, labelValues, "mesos_slave_uptime_seconds", stats.UptimeSecs, "Slave uptime in seconds")

	e.counter(metricsChan, labels, labelValues, "mesos_slave_valid_framework_messages_total", float64(stats.ValidFrameworkMessages), "Amount of valid framework messages")
	e.counter(metricsChan, labels, labelValues, "mesos_slave_valid_status_updates_total", float64(stats.ValidStatusUpdates), "Amount of valid status updates")
}

func (e *periodicExporter) fetchMasterMetricsSnapshot(url *url.URL, host string, metricsChan chan <- prometheus.Metric) {
	monitorURL := fmt.Sprintf("%s/metrics/snapshot", url)
	resp, err := httpClient.Get(monitorURL)
	if err != nil {
		log.Warn(err)
		e.errors.WithLabelValues(host).Inc()
		return
	}
	defer resp.Body.Close()

	var stats mesos_metrics.MasterMetrics
	if err = json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		log.Warn("failed to deserialize response: ", err)
		e.errors.WithLabelValues(host).Inc()
		return
	}
	labels := []string{"node"}
	labelValues := []string{host}
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_cpus_available", float64(stats.CpuTotal), "Amount of CPUs available as resource in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_cpus_used", stats.CpuUsed, "Amount of CPU resources in use in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_cpus_used_percentage", stats.CpuPercentage, "Percentage of CPU resources in use in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_disk_available_bytes", float64(stats.DiskTotal), "Available bytes of disk space as resource in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_disk_used_bytes", float64(stats.DiskUsed), "Bytes of disk space resources in use in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_disk_used_percentage", float64(stats.DiskPercentage), "Percentage of disk space resources in use in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_mem_total_bytes", float64(stats.MemoryTotal), "Available bytes of memory available as resource in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_mem_used_bytes", float64(stats.MemoryUsed), "Bytes of memory resources in use in the whole cluster")
	e.gauge(metricsChan, labels, labelValues, "mesos_cluster_resource_mem_used_percentage", float64(stats.MemoryPercentage), "Percentage of memory resources in use in the whole cluster")

	e.counter(metricsChan, labels, labelValues, "mesos_master_dropped_messages_total", float64(stats.DroppedMessages), "Total of messages dropped")

	e.gauge(metricsChan, labels, labelValues, "mesos_master_elected", float64(stats.Elected), "Is master elected as the cluster master?")

	e.counter(metricsChan, labels, labelValues, "mesos_master_event_queue_dispatches_total", float64(stats.EventQueueDispatches), "Total of event queue dispatches")
	e.counter(metricsChan, labels, labelValues, "mesos_master_event_queue_http_requests_total", float64(stats.EventQueueHttpRequests), "Total of event queue HTTP requests")
	e.counter(metricsChan, labels, labelValues, "mesos_master_event_queue_messages_total", float64(stats.EventQueueMessages), "Total of event queue messages")

	e.gauge(metricsChan, labels, labelValues, "mesos_master_frameworks_active", float64(stats.FrameworksActive), "Amount of active frameworks")
	e.counter(metricsChan, labels, labelValues, "mesos_master_frameworks_connected_total", float64(stats.FrameworksConnected), "Total of frameworks that have been connected")
	e.counter(metricsChan, labels, labelValues, "mesos_master_frameworks_disconnected_total", float64(stats.FrameworksDisconnected), "Total of frameworks that have been disconnected")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_frameworks_inactive", float64(stats.FrameworksInactive), "Amount of inactive frameworks")

	e.counter(metricsChan, labels, labelValues, "mesos_master_invalid_framework_to_executor_messages_total", float64(stats.InvalidFrameworkToExecutorMessages), "Total of invalid framework-to-executor messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_invalid_status_update_acknowledgements_total", float64(stats.InvalidStatusUpdateAcknowledgements), "Total of invalid status update acknowledgements")
	e.counter(metricsChan, labels, labelValues, "mesos_master_invalid_status_updates_total", float64(stats.InvalidStatusUpdates), "Total of invalid status updates")

	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_authenticate_total", float64(stats.MessagesAuthenticate), "Total of authenticate messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_deactivate_framework_total", float64(stats.MessagesDeactivateFramework), "Total of deactivate framework messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_decline_offers_total", float64(stats.MessagesDeclineOffers), "Total of decline offers messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_exited_executor_total", float64(stats.MessagesExitedExecutor), "Total of exited executor messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_framework_to_executor_total", float64(stats.MessagesFrameworkToExecutor), "Total of framework-to-executor messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_kill_task_total", float64(stats.MessagesKillTask), "Total of kill tasks messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_launch_tasks_total", float64(stats.MessagesLaunchTasks), "Total of launch tasks messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_reconcile_tasks_total", float64(stats.MessagesReconcileTasks), "Total of reconcile tasks messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_register_framework_total", float64(stats.MessagesRegisterFramework), "Total of register framework messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_register_slave_total", float64(stats.MessagesRegisterSlave), "Total of register slave messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_reregister_framework_total", float64(stats.MessagesReregisterFramework), "Total of re-register framework messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_reregister_slave_total", float64(stats.MessagesReregisterSlave), "Total of re-register slave messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_resource_request_total", float64(stats.MessagesResourceRequest), "Total of resource request messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_revive_offers_total", float64(stats.MessagesReviveOffers), "Total of revive offers messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_status_update_total", float64(stats.MessagesStatusUpdate), "Total of status update messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_status_update_acknowledgement_total", float64(stats.MessagesStatusUpdateAcknowledgement), "Total of status update acknowledgement messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_unregister_framework_total", float64(stats.MessagesUnregisterFramework), "Total of unregister framework messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_messages_unregister_slave_total", float64(stats.MessagesUnregisterSlave), "Total of unregister slave messages")

	e.gauge(metricsChan, labels, labelValues, "mesos_master_outstanding_offers", float64(stats.OutstandingOffers), "Amount of offers outstanding")
	e.counter(metricsChan, labels, labelValues, "mesos_master_recovery_slave_removals_total", float64(stats.RecoverySlaveRemovals), "Total of recovery slave removals")

	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_registrations_total", float64(stats.SlaveRegistrations), "Total of slave registrations")
	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_removals_total", float64(stats.SlaveRemovals), "Total of slave removals")
	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_reregistrations_total", float64(stats.SlaveReregistrations), "Total of slave re-registrations")
	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_shutdowns_canceled_total", float64(stats.SlaveShutdownsCanceled), "Total of slave shutdowns canceled")
	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_shutdowns_scheduled_total", float64(stats.SlaveShutdownsScheduled), "Total of slave shutdowns scheduled")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_slave_active", float64(stats.SlavesActive), "Amount of active slaves")
	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_connected_total", float64(stats.SlavesConnected), "Total of connected slaved")
	e.counter(metricsChan, labels, labelValues, "mesos_master_slave_disconnected_total", float64(stats.SlavesDisconnected), "Total of disconnected slaves")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_slave_inactive", float64(stats.SlavesInactive), "Amount of inactive slaves")

	e.counter(metricsChan, labels, labelValues, "mesos_master_task_failed_source_slave_reason_command_executor_failed_total", float64(stats.TaskFailedSourceSlaveReasonCommandExecutorFailed), "Total tasks failed with as reason the command executor failed")

	e.counter(metricsChan, labels, labelValues, "mesos_master_tasks_error_total", float64(stats.TasksError), "Total tasks in error")
	e.counter(metricsChan, labels, labelValues, "mesos_master_tasks_failed_total", float64(stats.TasksFailed), "Total of failed tasks")
	e.counter(metricsChan, labels, labelValues, "mesos_master_tasks_finished_total", float64(stats.TasksFinished), "Total of finished tasks")
	e.counter(metricsChan, labels, labelValues, "mesos_master_tasks_killed_total", float64(stats.TasksKilled), "Total of killed tasks")
	e.counter(metricsChan, labels, labelValues, "mesos_master_tasks_lost_total", float64(stats.TasksLost), "Total of lost tasks")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_tasks_running", float64(stats.TasksRunning), "Amount of running tasks")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_tasks_staging", float64(stats.TasksStaging), "Amount of staging tasks")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_tasks_starting", float64(stats.TasksStarting), "Amount of starting tasks")

	e.counter(metricsChan, labels, labelValues, "mesos_master_uptime_seconds", stats.UptimeSecs, "Master uptime in seconds")

	e.counter(metricsChan, labels, labelValues, "mesos_master_valid_framework_to_executor_messages_total", float64(stats.ValidFrameworkToExecutorMessages), "Total of valid framework-to-executor messages")
	e.counter(metricsChan, labels, labelValues, "mesos_master_valid_status_update_acknowledgements_total", float64(stats.ValidStatusUpdateAcknowledgements), "Total of valid status update acknowledgements")
	e.counter(metricsChan, labels, labelValues, "mesos_master_valid_status_updates_total", float64(stats.ValidStatusUpdates), "Total of valid status updates")

	e.counter(metricsChan, labels, labelValues, "mesos_master_registrar_queued_operations_total", float64(stats.RegistrarQueuedOperations), "Total of queued operations in registrar")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_registrar_registry_size_bytes", float64(stats.RegistrarSizeBytes), "Registrar registry size in bytes")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_registrar_state_fetch_duration_milliseconds", stats.RegistrarStateFetchMilliSecs, "Duration of state fetch by registrar (in milliseconds)")
	e.counter(metricsChan, labels, labelValues, "mesos_master_registrar_state_store_total", float64(stats.RegistrarStateStoreMilliSecsCount), "Total of state stores by registrar")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_registrar_state_store_duration_milliseconds_max", stats.RegistrarStateStoreMilliSecsMax, "Maximum duration of state store by registrar (in milliseconds)")
	e.gauge(metricsChan, labels, labelValues, "mesos_master_registrar_state_store_duration_milliseconds_min", stats.RegistrarStateStoreMilliSecsMin, "Minimum duration of state store by registrar (in milliseconds)")
	summaryQuantileLabels := append(labels, "quantile")
	e.gauge(metricsChan, summaryQuantileLabels, append(labelValues, "0.5"), "mesos_master_registrar_state_store_duration_milliseconds", stats.RegistrarStateStoreMilliSecsP50, "Duration of state store by registrar (in milliseconds)")
	e.gauge(metricsChan, summaryQuantileLabels, append(labelValues, "0.9"), "mesos_master_registrar_state_store_duration_milliseconds", stats.RegistrarStateStoreMilliSecsP90, "Duration of state store by registrar (in milliseconds)")
	e.gauge(metricsChan, summaryQuantileLabels, append(labelValues, "0.95"), "mesos_master_registrar_state_store_duration_milliseconds", stats.RegistrarStateStoreMilliSecsP95, "Duration of state store by registrar (in milliseconds)")
	e.gauge(metricsChan, summaryQuantileLabels, append(labelValues, "0.99"), "mesos_master_registrar_state_store_duration_milliseconds", stats.RegistrarStateStoreMilliSecsP99, "Duration of state store by registrar (in milliseconds)")
	e.gauge(metricsChan, summaryQuantileLabels, append(labelValues, "0.999"), "mesos_master_registrar_state_store_duration_milliseconds", stats.RegistrarStateStoreMilliSecsP999, "Duration of state store by registrar (in milliseconds)")
	e.gauge(metricsChan, summaryQuantileLabels, append(labelValues, "0.9999"), "mesos_master_registrar_state_store_duration_milliseconds", stats.RegistrarStateStoreMilliSecsP9999, "Duration of state store by registrar (in milliseconds)")
}

func (e *periodicExporter) gauge(metricsChan chan <- prometheus.Metric,
labels []string, labelValues []string,
key string, value float64, help string) {
	metricsChan <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(key, help, labels, nil),
		prometheus.GaugeValue, value, labelValues...)
}

func (e *periodicExporter) counter(metricsChan chan <- prometheus.Metric,
labels []string, labelValues []string,
key string, value float64, help string) {
	metricsChan <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(key, help, labels, nil),
		prometheus.CounterValue, value, labelValues...)
}

func (e *periodicExporter) rLockMetrics(f func()) {
	e.RLock()
	defer e.RUnlock()
	f()
}

func (e *periodicExporter) setMetrics(ch chan prometheus.Metric) {
	metrics := make([]prometheus.Metric, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	e.Lock()
	e.metrics = metrics
	e.Unlock()
}

func (e *periodicExporter) scrapeNodes() {
	e.nodes.Lock()
	masterUrls := []string{}
	slaveUrls := []string{}
	for i := 0; i < len(e.nodes.nodes); i++ {
		switch e.nodes.nodes[i].Type {
		case MasterNode:
			masterUrls = append(masterUrls, e.nodes.nodes[i].url)
		case SlaveNode:
			slaveUrls = append(slaveUrls, e.nodes.nodes[i].url)
		default:
			log.Warn("Node [", e.nodes.nodes[i], "] has an unknown type of [", e.nodes.nodes[i].Type, "]")
		}
	}
	e.nodes.Unlock()

	masterUrlsSize := len(masterUrls)
	slaveUrlsSize := len(slaveUrls)
	log.Debugf("active masters: %d", masterUrlsSize)
	log.Debugf("active slaves: %d", slaveUrlsSize)

	masterUrlChan := make(chan string)
	slaveUrlChan := make(chan string)
	metricsChan := make(chan prometheus.Metric)
	go e.setMetrics(metricsChan)

	poolSize := concurrentFetch
	urlCount := masterUrlsSize + slaveUrlsSize
	if urlCount < concurrentFetch {
		poolSize = urlCount
	}

	log.Debugf("creating fetch pool of size %d", poolSize)

	var wg sync.WaitGroup
	wg.Add(poolSize)
	for i := 0; i < masterUrlsSize; i++ {
		go e.fetchMasterMetrics(masterUrlChan, metricsChan, &wg)
	}
	for i := 0; i < slaveUrlsSize; i++ {
		go e.fetchSlaveMetrics(slaveUrlChan, metricsChan, &wg)
	}

	for _, url := range masterUrls {
		masterUrlChan <- url
	}
	close(masterUrlChan)
	for _, url := range slaveUrls {
		slaveUrlChan <- url
	}
	close(slaveUrlChan)

	wg.Wait()
	close(metricsChan)
}

func (e *periodicExporter) updateNodes() {
	log.Debug("discovering nodes...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s/master/redirect", e.opts.masterURL)
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

	var metricsNodes []node

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		log.Warnf("%d response missing Location header", rresp.StatusCode)
		return
	}

	log.Debugf("current elected master at: %s", masterLoc)
	metricsNodes = append(metricsNodes, node{url: masterLoc, Type: MasterNode})

	// Find all active slaves
	stateURL := fmt.Sprintf("%s/master/state.json", masterLoc)
	resp, err := http.Get(stateURL)
	if err != nil {
		log.Warn(err)
		return
	}
	defer resp.Body.Close()

	type slave struct {
		Active   bool   `json:"active"`
		Hostname string `json:"hostname"`
		Pid      string `json:"pid"`
	}

	var req struct {
		Slaves []*slave `json:"slaves"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&req); err != nil {
		log.Warnf("failed to deserialize request: %s", err)
		return
	}

	for _, slave := range req.Slaves {
		if slave.Active {
			// Extract slave port from pid
			_, port, err := net.SplitHostPort(slave.Pid)
			if err != nil {
				port = "5051"
			}
			url := fmt.Sprintf("http://%s:%s", slave.Hostname, port)

			metricsNodes = append(metricsNodes, node{url: url, Type: SlaveNode})
		}
	}

	log.Debugf("%d nodes discovered", len(metricsNodes))

	e.nodes.Lock()
	e.nodes.nodes = metricsNodes
	e.nodes.Unlock()
}

func runEvery(f func(), interval time.Duration) {
	for _ = range time.NewTicker(interval).C {
		f()
	}
}

func main() {
	flag.Parse()

	opts := &exporterOpts{
		autoDiscover: *autoDiscover,
		interval:     *scrapeInterval,
		localURL:     strings.TrimRight(*localURL, "/"),
		masterURL:    strings.TrimRight(*masterURL, "/"),
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
