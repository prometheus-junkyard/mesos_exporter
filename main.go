package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/antonlindstrom/mesos_stats"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	configFile  = flag.String("config.file", "", "Path to config file.")
	metricsPath = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	cpuLimitVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "cpus_limit",
			Help:      "CPU share limit.",
		},
		[]string{"service", "mesos_slave", "framework_id"},
	)
	cpuSysVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "cpus_system_time_secs",
			Help:      "CPU system time for task.",
		},
		[]string{"service", "mesos_slave", "framework_id"},
	)
	cpuUsrVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "cpus_user_time_secs",
			Help:      "CPU system time for task.",
		},
		[]string{"service", "mesos_slave", "framework_id"},
	)
	memLimitVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "mem_limit_bytes",
			Help:      "Task memory limit in bytes.",
		},
		[]string{"service", "mesos_slave", "framework_id"},
	)
	memRssVal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mesos_task",
			Name:      "mem_rss_bytes",
			Help:      "Task memory RSS usage in bytes.",
		},
		[]string{"service", "mesos_slave", "framework_id"},
	)
)

type Config struct {
	Port      string      `json:"port"`
	Interval  string      `json:"scrape_interval"`
	Endpoints *[]Endpoint `json:"endpoints"`
}

type Endpoint struct {
	Name       string        `json:"name"`
	Target     string        `json:"target"`
	Interval   string        `json:"scrape_interval,omitempty"`
	TimeoutStr string        `json:"timeout,omitempty"`
	Timeout    time.Duration `json:"-"`
}

func newConfig() (*Config, error) {
	var config Config

	file, err := ioutil.ReadFile(*configFile)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &config)
	return &config, err
}

func request(e Endpoint) error {
	client := http.Client{Timeout: 3 * time.Second}

	resp, err := client.Get(e.Target + "/monitor/statistics.json")
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var stats []mesos_stats.Monitor

	err = json.Unmarshal(body, &stats)
	if err != nil {
		return err
	}

	for _, t := range stats {
		cpuLimitVal.WithLabelValues(t.Source, e.Name, t.FrameworkId).Set(t.Statistics.CpusLimit)
		cpuSysVal.WithLabelValues(t.Source, e.Name, t.FrameworkId).Set(t.Statistics.CpusSystemTimeSecs)
		cpuUsrVal.WithLabelValues(t.Source, e.Name, t.FrameworkId).Set(t.Statistics.CpusUserTimeSecs)
		memLimitVal.WithLabelValues(t.Source, e.Name, t.FrameworkId).Set(float64(t.Statistics.MemLimitBytes))
		memRssVal.WithLabelValues(t.Source, e.Name, t.FrameworkId).Set(float64(t.Statistics.MemRssBytes))
	}

	return err
}

func requestRunner(e Endpoint) {
	for {
		err := request(e)
		if err != nil {
			glog.Warningln(err)
		}

		t, err := time.ParseDuration(e.Interval)
		if err != nil {
			glog.Warningln(err)
			t = 30 * time.Second
		}

		time.Sleep(t)
	}
}

func processRequests(c *Config) {
	for _, endpoint := range *c.Endpoints {
		glog.Infof("Found %s\n", endpoint.Name)

		if len(endpoint.Interval) == 0 {
			endpoint.Interval = c.Interval
		}

		if len(endpoint.TimeoutStr) != 0 {
			t, err := time.ParseDuration(endpoint.TimeoutStr)
			if err != nil {
				glog.Fatalf("Invalid specification of time.Duration: %s\n", err)
			}
			endpoint.Timeout = t
		} else {
			endpoint.Timeout = 3 * time.Second
		}

		go requestRunner(endpoint)
	}
}

func init() {
	prometheus.MustRegister(cpuLimitVal)
	prometheus.MustRegister(cpuSysVal)
	prometheus.MustRegister(cpuUsrVal)
	prometheus.MustRegister(memLimitVal)
	prometheus.MustRegister(memRssVal)
}

func main() {
	flag.Parse()

	if len(*configFile) < 2 {
		fmt.Printf("Error: Flag -config.file is required.\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	config, err := newConfig()
	if err != nil {
		fmt.Printf("Configuration error: %s\n", err)
		os.Exit(2)
	}

	go func() {
		http.Handle(*metricsPath, prometheus.Handler())
		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "OK")
		})
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, *metricsPath, http.StatusMovedPermanently)
		})

		processRequests(config)

		glog.Infof("Starting up on %s\n", config.Port)
		err := http.ListenAndServe(":"+config.Port, nil)
		if err != nil {
			glog.Fatal(err)
		}
	}()

	select {}
}
