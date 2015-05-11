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

var (
	addr           string
	masterURL      string
	metricsPath    string
	port           int
	scrapeInterval time.Duration
)

var (
	cpuLimitVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "cpu_limit",
			Help:      "Fractional CPU limit.",
		},
		[]string{"task", "mesos_slave", "framework_id"},
	)
	cpuSysVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "cpu_system_seconds_total",
			Help:      "Cumulative system CPU time in seconds.",
		},
		[]string{"task", "mesos_slave", "framework_id"},
	)
	cpuUsrVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "cpu_user_seconds_total",
			Help:      "Cumulative user CPU time in seconds.",
		},
		[]string{"task", "mesos_slave", "framework_id"},
	)
	memLimitVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "memory_limit_bytes",
			Help:      "Task memory limit in bytes.",
		},
		[]string{"task", "mesos_slave", "framework_id"},
	)
	memRssVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "memory_rss_bytes",
			Help:      "Task memory RSS usage in bytes.",
		},
		[]string{"task", "mesos_slave", "framework_id"},
	)
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

func init() {
	flag.StringVar(&metricsPath, "web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	flag.IntVar(&port, "port", 9105, "Expose metrics on port")
	flag.StringVar(&masterURL, "target", "http://mesos-master.example.com", "Mesos master URL")
	flag.DurationVar(&scrapeInterval, "scrapeIntreval", (10 * time.Second), "Scrape interval")
	flag.Parse()

	addr = fmt.Sprint(":", port)

	prometheus.MustRegister(cpuLimitVal)
	prometheus.MustRegister(cpuSysVal)
	prometheus.MustRegister(cpuUsrVal)
	prometheus.MustRegister(memLimitVal)
	prometheus.MustRegister(memRssVal)
}

var ActiveSlaves struct {
	sync.Mutex
	Hostnames []string
}

func LockActiveSlaves(f func()) {
	ActiveSlaves.Lock()
	defer ActiveSlaves.Unlock()
	f()
}

func Periodic(f func(), interval time.Duration) {
	f()
	for {
		select {
		case <-time.After(interval):
			f()
		}
	}
}

func request(host string) error {
	resp, err := httpClient.Get(host + "/monitor/statistics.json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var stats []mesos_stats.Monitor
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return err
	}

	for _, t := range stats {
		cpuLimitVal.WithLabelValues(t.Source, host, t.FrameworkId).
			Set(t.Statistics.CpusLimit)

		cpuSysVal.WithLabelValues(t.Source, host, t.FrameworkId).
			Set(t.Statistics.CpusSystemTimeSecs)

		cpuUsrVal.WithLabelValues(t.Source, host, t.FrameworkId).
			Set(t.Statistics.CpusUserTimeSecs)

		memLimitVal.WithLabelValues(t.Source, host, t.FrameworkId).
			Set(float64(t.Statistics.MemLimitBytes))

		memRssVal.WithLabelValues(t.Source, host, t.FrameworkId).
			Set(float64(t.Statistics.MemRssBytes))
	}

	return nil
}

func requestRunner() {
	var hostnames []string
	LockActiveSlaves(func() {
		hostnames = ActiveSlaves.Hostnames
	})

	glog.V(6).Infof("active slaves: %d", len(hostnames))

	var wg sync.WaitGroup
	wg.Add(len(hostnames))
	for _, host := range hostnames {
		go func(host string) {
			if err := request(host); err != nil {
				glog.Warningf("%s failed. Error: %s", host, err)
			}

		}(host)
	}
	wg.Wait()
}

func slaveDiscover() {
	glog.V(6).Info("discovering slaves...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s:5050/master/redirect", masterURL)
	rresp, err := httpClient.Get(redirectURL)
	if err != nil {
		glog.Warningf("GET %s failed. Error: %s", masterURL, err)
		return
	}
	defer rresp.Body.Close()

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		glog.Warningf("%d response missing Location header", rresp.StatusCode)
		return
	}

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

	LockActiveSlaves(func() {
		ActiveSlaves.Hostnames = slaveHostnames
	})
}

func main() {

	// Refresh the nr. of mesos slaves every 10 minute
	go Periodic(slaveDiscover, (10 * time.Minute))
	// Fetch slave metrics every scrapeInterval
	go Periodic(requestRunner, scrapeInterval)

	http.Handle(metricsPath, prometheus.Handler())
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, metricsPath, http.StatusMovedPermanently)
	})

	glog.Info("starting mesos_exporter on port ", port)

	log.Fatal(http.ListenAndServe(addr, nil))
}
