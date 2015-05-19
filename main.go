package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
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
	localAddr      = flag.String("exporter.local-address", "http://127.0.0.1:5051", "URL to the local Mesos slave")
	masterURL      = flag.String("exporter.discovery.master", "http://mesos-master.example.com:5050", "Mesos master URL")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics")
	scrapeInterval = flag.Duration("exporter.interval", (60 * time.Second), "Scrape interval duration")
)

var (
	variableLabels = []string{"task", "slave", "framework_id"}

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
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

type exporterOpts struct {
	autoDiscover bool
	interval     time.Duration
	localAddr    string
	masterURL    string
}

type periodicExporter struct {
	sync.RWMutex
	errors  *prometheus.CounterVec
	metrics []prometheus.Metric
	opts    *exporterOpts
	slaves  struct {
		sync.Mutex
		urls []string
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
	e.slaves.urls = []string{e.opts.localAddr}

	if e.opts.autoDiscover {
		glog.Info("auto discovery enabled from command line flag.")

		// Update nr. of mesos slaves every 10 minutes
		e.updateSlaves()
		go runEvery(e.updateSlaves, 10*time.Minute)
	}

	// Fetch slave metrics every interval
	go runEvery(e.scrapeSlaves, e.opts.interval)

	return e
}

func (e *periodicExporter) Describe(ch chan<- *prometheus.Desc) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m.Desc()
		}
	})
	e.errors.MetricVec.Describe(ch)
}

func (e *periodicExporter) Collect(ch chan<- prometheus.Metric) {
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
			glog.Warning("could not parse slave URL: ", err)
			continue
		}

		host, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			glog.Warning("could not parse network address: ", err)
			continue
		}

		monitorURL := fmt.Sprintf("%s/monitor/statistics.json", u)
		resp, err := httpClient.Get(monitorURL)
		if err != nil {
			glog.Warning(err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}
		defer resp.Body.Close()

		var stats []mesos_stats.Monitor
		if err = json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			glog.Warning("failed to deserialize response: ", err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		for _, stat := range stats {
			metricsChan <- prometheus.MustNewConstMetric(
				cpuLimitDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.CpusLimit),
				stat.Source, host, stat.FrameworkId,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				cpuSysDesc,
				prometheus.CounterValue,
				float64(stat.Statistics.CpusSystemTimeSecs),
				stat.Source, host, stat.FrameworkId,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				cpuUsrDesc,
				prometheus.CounterValue,
				float64(stat.Statistics.CpusUserTimeSecs),
				stat.Source, host, stat.FrameworkId,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				memLimitDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.MemLimitBytes),
				stat.Source, host, stat.FrameworkId,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				memRssDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.MemRssBytes),
				stat.Source, host, stat.FrameworkId,
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
	metrics := make([]prometheus.Metric, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
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
	glog.V(6).Infof("active slaves: %d", urlCount)

	urlChan := make(chan string)
	metricsChan := make(chan prometheus.Metric)
	go e.setMetrics(metricsChan)

	poolSize := concurrentFetch
	if urlCount < concurrentFetch {
		poolSize = urlCount
	}

	glog.V(6).Infof("creating fetch pool of size %d", poolSize)

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
	glog.V(6).Info("discovering slaves...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s/master/redirect", e.opts.masterURL)
	rReq, err := http.NewRequest("GET", redirectURL, nil)
	if err != nil {
		panic(err)
	}

	tr := http.Transport{}
	rresp, err := tr.RoundTrip(rReq)
	if err != nil {
		glog.Warning(err)
		return
	}
	defer rresp.Body.Close()

	_, err = ioutil.ReadAll(rresp.Body)
	if err != nil {
		glog.Warning(err)
		return
	}

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
		glog.Warning(err)
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
		glog.Warningf("failed to deserialize request: %s", err)
		return
	}

	var slaveURLs []string
	for _, slave := range req.Slaves {
		if slave.Active {
			// Extract slave port from pid
			_, port, err := net.SplitHostPort(slave.Pid)
			if err != nil {
				port = "5051"
			}
			url := fmt.Sprintf("http://%s:%s", slave.Hostname, port)

			slaveURLs = append(slaveURLs, url)
		}
	}

	glog.V(6).Infof("%d slaves discovered", len(slaveURLs))

	e.slaves.Lock()
	e.slaves.urls = slaveURLs
	e.slaves.Unlock()
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
		localAddr:    strings.TrimRight(*localAddr, "/"),
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

	glog.Info("starting mesos_exporter on ", *addr)

	log.Fatal(http.ListenAndServe(*addr, nil))
}
