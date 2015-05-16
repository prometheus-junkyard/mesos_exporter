package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/antonlindstrom/mesos_stats"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

const concurrentFetch = 100

// Commandline flags.
var (
	addr           = flag.String("web.listen-address", ":9105", "Address to listen on for web interface and telemetry")
	autoDiscover   = flag.Bool("exporter.discovery", false, "Discover all Mesos slaves")
	localAddr      = flag.String("exporter.local-address", "127.0.0.1", "Address to connect to the local Mesos slave")
	masterURL      = flag.String("exporter.discovery.master", "http://mesos-master.example.com", "Mesos master URL")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics")
	scrapeInterval = flag.Duration("exporter.interval", (60 * time.Second), "Scrape interval duration")
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

type ExporterOpts struct {
	autoDiscover bool
	interval     time.Duration
	localAddr    string
	masterURL    string
}

type PeriodicExporter struct {
	sync.RWMutex
	errors  *prometheus.CounterVec
	metrics []prometheus.Gauge
	opts    *ExporterOpts
	slaves  struct {
		sync.Mutex
		hostnames []string
	}
}

func NewMesosExporter(opts *ExporterOpts) *PeriodicExporter {
	e := &PeriodicExporter{
		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "exporter",
				Name:      "scrape_errors",
				Help:      "Current total scrape errors ",
			},
			[]string{"mesos_slave"},
		),
		opts:    opts,
		metrics: make([]prometheus.Gauge, 0),
	}
	e.slaves.hostnames = []string{e.opts.localAddr}

	if e.opts.autoDiscover {
		glog.Info("auto discovery enabled from command line flag.")

		// Update nr. of mesos slave every 10 minute
		e.slaves.hostnames = []string{}
		go e.runEvery(e.updateSlaves, (10 * time.Minute))
	}

	// Fetch slave metrics every interval
	go e.runEvery(e.scrapeSlaves, e.opts.interval)

	return e
}

func (e *PeriodicExporter) Describe(ch chan<- *prometheus.Desc) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			m.Describe(ch)
		}
	})
	e.errors.MetricVec.Describe(ch)
}

func (e *PeriodicExporter) Collect(ch chan<- prometheus.Metric) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			m.Collect(ch)
		}
	})
	e.errors.MetricVec.Collect(ch)
}

func (e *PeriodicExporter) fetch(inCh chan string, outCh chan prometheus.Gauge, wg *sync.WaitGroup) {
	defer wg.Done()

	for host := range inCh {
		monitorURL := fmt.Sprintf("http://%s:5051/monitor/statistics.json", host)
		resp, err := httpClient.Get(monitorURL)
		if err != nil {
			glog.Warningf("GET %s failed. Error: %s", monitorURL, err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}
		defer resp.Body.Close()

		var stats []mesos_stats.Monitor
		if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			glog.Warningf("failed to deserialize request: %s", err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		for _, stat := range stats {
			cpuLimitVal := prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: "mesos_task",
					Name:      "cpu_limit",
					Help:      "Fractional CPU limit.",
					ConstLabels: prometheus.Labels{
						"task":         stat.Source,
						"mesos_slave":  host,
						"framework_id": stat.FrameworkId,
					},
				},
			)
			cpuLimitVal.Set(stat.Statistics.CpusSystemTimeSecs)

			cpuSysVal := prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: "mesos_task",
					Name:      "cpu_system_seconds_total",
					Help:      "Cumulative system CPU time in seconds.",
					ConstLabels: prometheus.Labels{
						"task":         stat.Source,
						"mesos_slave":  host,
						"framework_id": stat.FrameworkId,
					},
				},
			)
			cpuSysVal.Set(stat.Statistics.CpusSystemTimeSecs)

			cpuUsrVal := prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: "mesos_task",
					Name:      "cpu_user_seconds_total",
					Help:      "Cumulative user CPU time in seconds.",
					ConstLabels: prometheus.Labels{
						"task":         stat.Source,
						"mesos_slave":  host,
						"framework_id": stat.FrameworkId,
					},
				},
			)
			cpuUsrVal.Set(stat.Statistics.CpusUserTimeSecs)

			memLimitVal := prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: "mesos_task",
					Name:      "memory_limit_bytes",
					Help:      "Task memory limit in bytes.",
					ConstLabels: prometheus.Labels{
						"task":         stat.Source,
						"mesos_slave":  host,
						"framework_id": stat.FrameworkId,
					},
				},
			)
			memLimitVal.Set(float64(stat.Statistics.MemLimitBytes))

			memRssVal := prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: "mesos_task",
					Name:      "memory_rss_bytes",
					Help:      "Task memory RSS usage in bytes.",
					ConstLabels: prometheus.Labels{
						"task":         stat.Source,
						"mesos_slave":  host,
						"framework_id": stat.FrameworkId,
					},
				},
			)
			memRssVal.Set(float64(stat.Statistics.MemRssBytes))

			outCh <- cpuLimitVal
			outCh <- cpuSysVal
			outCh <- cpuUsrVal
			outCh <- memLimitVal
			outCh <- memRssVal
		}
	}
}

func (e *PeriodicExporter) lockSlaves(f func() []string) []string {
	e.slaves.Lock()
	defer e.slaves.Unlock()
	return f()
}

func (e *PeriodicExporter) rLockMetrics(f func()) {
	e.RLock()
	defer e.RUnlock()
	f()
}

func (e *PeriodicExporter) runEvery(f func(), interval time.Duration) {
	f()
	for {
		select {
		case <-time.After(interval):
			f()
		}
	}
}

func (e *PeriodicExporter) setMetrics(ch chan prometheus.Gauge) {
	metrics := make([]prometheus.Gauge, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	e.Lock()
	e.metrics = metrics
	e.Unlock()
}

func (e *PeriodicExporter) scrapeSlaves() {
	hostnames := e.lockSlaves(func() []string {
		return e.slaves.hostnames
	})

	totalHostnames := len(hostnames)
	glog.V(6).Infof("active slaves: %d", totalHostnames)

	dispatchCh := make(chan string)
	ch := make(chan prometheus.Gauge)
	go e.setMetrics(ch)

	poolSize := concurrentFetch
	if totalHostnames < concurrentFetch {
		poolSize = totalHostnames
	}

	glog.V(6).Infof("creating fetch pool of size %d", poolSize)

	var wg sync.WaitGroup
	wg.Add(poolSize)
	for i := 0; i < poolSize; i++ {
		go e.fetch(dispatchCh, ch, &wg)
	}

	for _, host := range hostnames {
		dispatchCh <- host
	}
	close(dispatchCh)

	wg.Wait()
	close(ch)
}

func (e *PeriodicExporter) updateSlaves() {
	glog.V(6).Info("discovering slaves...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s:5050/master/redirect", e.opts.masterURL)
	rReq, _ := http.NewRequest("GET", redirectURL, nil)

	tr := http.Transport{}
	rresp, err := tr.RoundTrip(rReq)
	if err != nil {
		glog.Warningf("GET %s failed. Error: %s", redirectURL, err)
		return
	}
	defer rresp.Body.Close()

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		glog.Warningf("%d response missing Location header", rresp.StatusCode)
		return
	}

	glog.V(6).Infof("current elected master at: %s", masterLoc)

	// Find all active slaves
	stateURL := fmt.Sprintf("%s/master/state.json", masterLoc)
	resp, err := http.Get(stateURL)
	if err != nil {
		glog.Warningf("GET %s failed. Error: %s", stateURL, err)
		return
	}
	defer resp.Body.Close()

	type slave struct {
		Active   bool   `json:"active"`
		Hostname string `json:"hostname"`
	}

	var req struct {
		Slaves []*slave `json:"slaves"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&req); err != nil {
		glog.Warningf("failed to deserialize request: %s", err)
		return
	}

	var slaveHostnames []string
	for _, slave := range req.Slaves {
		if slave.Active {
			slaveHostnames = append(slaveHostnames, slave.Hostname)
		}
	}

	glog.V(6).Infof("%d slaves discovered", len(slaveHostnames))

	e.slaves.hostnames = e.lockSlaves(func() []string {
		return slaveHostnames
	})
}

func main() {
	flag.Parse()

	opts := &ExporterOpts{
		autoDiscover: *autoDiscover,
		interval:     *scrapeInterval,
		localAddr:    *localAddr,
		masterURL:    *masterURL,
	}
	exporter := NewMesosExporter(opts)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, *metricsPath, http.StatusMovedPermanently)
	})

	glog.Info("starting mesos_exporter on ", *addr)

	log.Fatal(http.ListenAndServe(*addr, nil))
}
