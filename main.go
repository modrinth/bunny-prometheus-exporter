package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type statusIndicator struct {
	Indicator float64
}

type overview struct {
	Status           string
	ActiveInstances  statusIndicator
	DesiredInstances int
	ActiveRegions    statusIndicator
	AverageCPU       statusIndicator
	AverageRAM       statusIndicator
	CurrentLatency   statusIndicator
	Limits           struct {
		CPUCores *float64
		RAMBytes *float64
	}
	Regions []region
}

type region struct {
	Region         string
	Status         string
	Instances      int
	AverageCPU     float64
	AverageRAM     float64
	Requests       float64
	AnycastTraffic float64
	Pods           []pod
}

type pod struct {
	Name          string
	Status        string
	LastHeartBeat *string
	CPUUsage      float64
	RAMUsage      float64
	CPUUsageCores *float64
	RAMUsageBytes *float64
	Containers    []container
}

type container struct {
	Name             string
	Status           string
	CPUUsage         float64
	RAMUsage         float64
	NumberOfRestarts *int
	ExitCode         *int
}

type pullZone struct {
	ID                    int64
	Name                  string
	Enabled               bool
	Suspended             bool
	MonthlyBandwidthUsed  int64
	MonthlyBandwidthLimit int64
}

type pullZoneStatistics struct {
	TotalBandwidthUsed        int64
	TotalOriginTraffic        int64
	TotalRequestsServed       int64
	AverageOriginResponseTime int
	CacheHitRate              float64
	Error3xxChart             map[string]float64
	Error4xxChart             map[string]float64
	Error5xxChart             map[string]float64
}

type storageZone struct {
	ID          int64
	Name        string
	Region      string
	StorageUsed int64
	FilesStored int64
}

type appListItem struct {
	ID     string
	Name   string
	Status string
}

type appList struct {
	Items  []appListItem
	Cursor *string
}

type collector struct{}

var (
	apiKey     = requireEnv("BUNNY_API_KEY")
	listenAddr = envOrDefault("LISTEN_ADDR", ":9877")
	httpClient = &http.Client{Timeout: 10 * time.Second}

	appLabelNames         = []string{"app_id"}
	regionLabelNames      = []string{"app_id", "region"}
	podLabelNames         = []string{"app_id", "region", "pod"}
	containerLabelNames   = []string{"app_id", "region", "pod", "container"}
	pullZoneLabelNames    = []string{"pullzone_id", "pullzone"}
	storageZoneLabelNames = []string{"storagezone_id", "storagezone", "region"}

	scrapeSuccessDesc = prometheus.NewDesc("bunny_scrape_success", "Whether fetching from the bunny API succeeded.", []string{"source", "id"}, nil)

	appInfoDesc             = prometheus.NewDesc("bunny_mc_app_info", "Application metadata, join on app_id for the name.", []string{"app_id", "app_name"}, nil)
	appStatusDesc           = prometheus.NewDesc("bunny_mc_app_status", "Application status, 1 for the current status.", []string{"app_id", "status"}, nil)
	appActiveInstancesDesc  = prometheus.NewDesc("bunny_mc_app_active_instances", "Active instances.", appLabelNames, nil)
	appDesiredInstancesDesc = prometheus.NewDesc("bunny_mc_app_desired_instances", "Desired instances.", appLabelNames, nil)
	appActiveRegionsDesc    = prometheus.NewDesc("bunny_mc_app_active_regions", "Active regions.", appLabelNames, nil)
	appAverageCPUDesc       = prometheus.NewDesc("bunny_mc_app_average_cpu", "Average CPU usage as reported by bunny.", appLabelNames, nil)
	appAverageRAMDesc       = prometheus.NewDesc("bunny_mc_app_average_ram", "Average RAM usage as reported by bunny.", appLabelNames, nil)
	appCurrentLatencyDesc   = prometheus.NewDesc("bunny_mc_app_current_latency", "Current latency as reported by bunny.", appLabelNames, nil)
	appLimitCPUCoresDesc    = prometheus.NewDesc("bunny_mc_app_limit_cpu_cores", "CPU core limit per instance.", appLabelNames, nil)
	appLimitRAMBytesDesc    = prometheus.NewDesc("bunny_mc_app_limit_ram_bytes", "RAM limit per instance in bytes.", appLabelNames, nil)

	regionStatusDesc         = prometheus.NewDesc("bunny_mc_region_status", "Region deployment status, 1 for the current status.", []string{"app_id", "region", "status"}, nil)
	regionInstancesDesc      = prometheus.NewDesc("bunny_mc_region_instances", "Instances in the region.", regionLabelNames, nil)
	regionAverageCPUDesc     = prometheus.NewDesc("bunny_mc_region_average_cpu", "Average CPU usage in the region as reported by bunny.", regionLabelNames, nil)
	regionAverageRAMDesc     = prometheus.NewDesc("bunny_mc_region_average_ram", "Average RAM usage in the region as reported by bunny.", regionLabelNames, nil)
	regionRequestsDesc       = prometheus.NewDesc("bunny_mc_region_requests", "Requests in the region as reported by bunny.", regionLabelNames, nil)
	regionAnycastTrafficDesc = prometheus.NewDesc("bunny_mc_region_anycast_traffic", "Anycast traffic in the region as reported by bunny.", regionLabelNames, nil)

	podStatusDesc        = prometheus.NewDesc("bunny_mc_pod_status", "Pod status, 1 for the current status.", []string{"app_id", "region", "pod", "status"}, nil)
	podReadyDesc         = prometheus.NewDesc("bunny_mc_pod_ready", "Whether the pod status is ready.", podLabelNames, nil)
	podCPUUsageDesc      = prometheus.NewDesc("bunny_mc_pod_cpu_usage", "Pod CPU usage as reported by bunny.", podLabelNames, nil)
	podRAMUsageDesc      = prometheus.NewDesc("bunny_mc_pod_ram_usage", "Pod RAM usage as reported by bunny.", podLabelNames, nil)
	podCPUUsageCoresDesc = prometheus.NewDesc("bunny_mc_pod_cpu_usage_cores", "Pod CPU usage in cores.", podLabelNames, nil)
	podRAMUsageBytesDesc = prometheus.NewDesc("bunny_mc_pod_ram_usage_bytes", "Pod RAM usage in bytes.", podLabelNames, nil)
	podLastHeartbeatDesc = prometheus.NewDesc("bunny_mc_pod_last_heartbeat_timestamp_seconds", "Unix time of the last pod heartbeat.", podLabelNames, nil)

	containerStatusDesc       = prometheus.NewDesc("bunny_mc_container_status", "Container status, 1 for the current status.", []string{"app_id", "region", "pod", "container", "status"}, nil)
	containerCPUUsageDesc     = prometheus.NewDesc("bunny_mc_container_cpu_usage", "Container CPU usage as reported by bunny.", containerLabelNames, nil)
	containerRAMUsageDesc     = prometheus.NewDesc("bunny_mc_container_ram_usage", "Container RAM usage as reported by bunny.", containerLabelNames, nil)
	containerRestartsDesc     = prometheus.NewDesc("bunny_mc_container_restarts_total", "Container restart count.", containerLabelNames, nil)
	containerLastExitCodeDesc = prometheus.NewDesc("bunny_mc_container_last_exit_code", "Last container exit code.", containerLabelNames, nil)

	pullZoneEnabledDesc               = prometheus.NewDesc("bunny_pullzone_enabled", "Whether the pull zone is enabled.", pullZoneLabelNames, nil)
	pullZoneSuspendedDesc             = prometheus.NewDesc("bunny_pullzone_suspended", "Whether the pull zone is suspended.", pullZoneLabelNames, nil)
	pullZoneMonthlyBandwidthUsedDesc  = prometheus.NewDesc("bunny_pullzone_monthly_bandwidth_used_bytes", "Bandwidth used this month in bytes.", pullZoneLabelNames, nil)
	pullZoneMonthlyBandwidthLimitDesc = prometheus.NewDesc("bunny_pullzone_monthly_bandwidth_limit_bytes", "Monthly bandwidth limit in bytes, 0 when unlimited.", pullZoneLabelNames, nil)
	pullZoneBandwidthDesc             = prometheus.NewDesc("bunny_pullzone_bandwidth_bytes_total", "Bandwidth served since 00:00 UTC in bytes, resets daily.", pullZoneLabelNames, nil)
	pullZoneOriginTrafficDesc         = prometheus.NewDesc("bunny_pullzone_origin_traffic_bytes_total", "Origin traffic since 00:00 UTC in bytes, resets daily.", pullZoneLabelNames, nil)
	pullZoneRequestsDesc              = prometheus.NewDesc("bunny_pullzone_requests_total", "Requests served since 00:00 UTC, resets daily.", pullZoneLabelNames, nil)
	pullZoneErrorResponsesDesc        = prometheus.NewDesc("bunny_pullzone_error_responses_total", "Error responses since 00:00 UTC, resets daily.", []string{"pullzone_id", "pullzone", "status_class"}, nil)
	pullZoneCacheHitRateDesc          = prometheus.NewDesc("bunny_pullzone_cache_hit_rate", "Cache hit rate since 00:00 UTC as reported by bunny.", pullZoneLabelNames, nil)
	pullZoneOriginResponseTimeDesc    = prometheus.NewDesc("bunny_pullzone_origin_response_time_milliseconds", "Average origin response time since 00:00 UTC.", pullZoneLabelNames, nil)

	storageZoneUsedBytesDesc = prometheus.NewDesc("bunny_storagezone_used_bytes", "Storage used by the zone in bytes.", storageZoneLabelNames, nil)
	storageZoneFilesDesc     = prometheus.NewDesc("bunny_storagezone_files", "Files stored in the zone.", storageZoneLabelNames, nil)
)

func requireEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func getJSON(pathAndQuery string, target any) error {
	request, err := http.NewRequest(http.MethodGet, "https://api.bunny.net"+pathAndQuery, nil)
	if err != nil {
		return err
	}
	request.Header.Set("AccessKey", apiKey)
	request.Header.Set("Accept", "application/json")

	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("status %d: %s", response.StatusCode, body)
	}

	return json.NewDecoder(response.Body).Decode(target)
}

func gauge(metrics chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labelValues ...string) {
	metrics <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labelValues...)
}

func counter(metrics chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labelValues ...string) {
	metrics <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labelValues...)
}

func gaugeIfPresent[T int | float64](metrics chan<- prometheus.Metric, desc *prometheus.Desc, value *T, labelValues ...string) {
	if value == nil {
		return
	}
	gauge(metrics, desc, float64(*value), labelValues...)
}

func recordScrape(metrics chan<- prometheus.Metric, source, id string, err error) {
	if err != nil {
		log.Printf("scraping %s %s: %v", source, id, err)
	}
	gauge(metrics, scrapeSuccessDesc, boolToFloat(err == nil), source, id)
}

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func sumChart(chart map[string]float64) float64 {
	total := 0.0
	for _, value := range chart {
		total += value
	}
	return total
}

func recordOverview(metrics chan<- prometheus.Metric, appID string, app *overview) {
	gauge(metrics, appStatusDesc, 1, appID, app.Status)
	gauge(metrics, appActiveInstancesDesc, app.ActiveInstances.Indicator, appID)
	gauge(metrics, appDesiredInstancesDesc, float64(app.DesiredInstances), appID)
	gauge(metrics, appActiveRegionsDesc, app.ActiveRegions.Indicator, appID)
	gauge(metrics, appAverageCPUDesc, app.AverageCPU.Indicator, appID)
	gauge(metrics, appAverageRAMDesc, app.AverageRAM.Indicator, appID)
	gauge(metrics, appCurrentLatencyDesc, app.CurrentLatency.Indicator, appID)
	gaugeIfPresent(metrics, appLimitCPUCoresDesc, app.Limits.CPUCores, appID)
	gaugeIfPresent(metrics, appLimitRAMBytesDesc, app.Limits.RAMBytes, appID)

	for _, appRegion := range app.Regions {
		gauge(metrics, regionStatusDesc, 1, appID, appRegion.Region, appRegion.Status)
		gauge(metrics, regionInstancesDesc, float64(appRegion.Instances), appID, appRegion.Region)
		gauge(metrics, regionAverageCPUDesc, appRegion.AverageCPU, appID, appRegion.Region)
		gauge(metrics, regionAverageRAMDesc, appRegion.AverageRAM, appID, appRegion.Region)
		gauge(metrics, regionRequestsDesc, appRegion.Requests, appID, appRegion.Region)
		gauge(metrics, regionAnycastTrafficDesc, appRegion.AnycastTraffic, appID, appRegion.Region)

		for _, regionPod := range appRegion.Pods {
			recordPod(metrics, appID, appRegion.Region, regionPod)
		}
	}
}

func recordPod(metrics chan<- prometheus.Metric, appID, regionName string, regionPod pod) {
	gauge(metrics, podStatusDesc, 1, appID, regionName, regionPod.Name, regionPod.Status)
	gauge(metrics, podReadyDesc, boolToFloat(regionPod.Status == "ready"), appID, regionName, regionPod.Name)
	gauge(metrics, podCPUUsageDesc, regionPod.CPUUsage, appID, regionName, regionPod.Name)
	gauge(metrics, podRAMUsageDesc, regionPod.RAMUsage, appID, regionName, regionPod.Name)
	gaugeIfPresent(metrics, podCPUUsageCoresDesc, regionPod.CPUUsageCores, appID, regionName, regionPod.Name)
	gaugeIfPresent(metrics, podRAMUsageBytesDesc, regionPod.RAMUsageBytes, appID, regionName, regionPod.Name)

	if regionPod.LastHeartBeat != nil {
		heartbeat, err := time.Parse(time.RFC3339, *regionPod.LastHeartBeat)
		if err == nil {
			gauge(metrics, podLastHeartbeatDesc, float64(heartbeat.Unix()), appID, regionName, regionPod.Name)
		}
	}

	for _, podContainer := range regionPod.Containers {
		containerLabels := []string{appID, regionName, regionPod.Name, podContainer.Name}
		gauge(metrics, containerStatusDesc, 1, append(containerLabels, podContainer.Status)...)
		gauge(metrics, containerCPUUsageDesc, podContainer.CPUUsage, containerLabels...)
		gauge(metrics, containerRAMUsageDesc, podContainer.RAMUsage, containerLabels...)
		gaugeIfPresent(metrics, containerLastExitCodeDesc, podContainer.ExitCode, containerLabels...)
		if podContainer.NumberOfRestarts != nil {
			counter(metrics, containerRestartsDesc, float64(*podContainer.NumberOfRestarts), containerLabels...)
		}
	}
}

func listApps() ([]appListItem, error) {
	var apps []appListItem
	query := url.Values{"limit": {"1000"}}
	for {
		var page appList
		if err := getJSON("/mc/apps?"+query.Encode(), &page); err != nil {
			return nil, err
		}
		apps = append(apps, page.Items...)
		if page.Cursor == nil || *page.Cursor == "" {
			return apps, nil
		}
		query.Set("nextCursor", *page.Cursor)
	}
}

func scrapeApps(metrics chan<- prometheus.Metric) {
	apps, err := listApps()
	recordScrape(metrics, "mc_apps", "", err)
	if err != nil {
		return
	}

	var waitGroup sync.WaitGroup
	for _, app := range apps {
		gauge(metrics, appInfoDesc, 1, app.ID, app.Name)
		waitGroup.Go(func() { scrapeApp(metrics, app.ID) })
	}
	waitGroup.Wait()
}

func scrapeApp(metrics chan<- prometheus.Metric, appID string) {
	var app overview
	err := getJSON("/mc/apps/"+url.PathEscape(appID)+"/overview", &app)
	recordScrape(metrics, "mc_app", appID, err)
	if err != nil {
		return
	}
	recordOverview(metrics, appID, &app)
}

func scrapeStorageZones(metrics chan<- prometheus.Metric) {
	var zones []storageZone
	err := getJSON("/storagezone", &zones)
	recordScrape(metrics, "storagezones", "", err)
	if err != nil {
		return
	}

	for _, zone := range zones {
		labels := []string{strconv.FormatInt(zone.ID, 10), zone.Name, zone.Region}
		gauge(metrics, storageZoneUsedBytesDesc, float64(zone.StorageUsed), labels...)
		gauge(metrics, storageZoneFilesDesc, float64(zone.FilesStored), labels...)
	}
}

func scrapePullZones(metrics chan<- prometheus.Metric) {
	var zones []pullZone
	err := getJSON("/pullzone", &zones)
	recordScrape(metrics, "pullzones", "", err)
	if err != nil {
		return
	}

	var waitGroup sync.WaitGroup
	for _, zone := range zones {
		labels := []string{strconv.FormatInt(zone.ID, 10), zone.Name}
		gauge(metrics, pullZoneEnabledDesc, boolToFloat(zone.Enabled), labels...)
		gauge(metrics, pullZoneSuspendedDesc, boolToFloat(zone.Suspended), labels...)
		gauge(metrics, pullZoneMonthlyBandwidthUsedDesc, float64(zone.MonthlyBandwidthUsed), labels...)
		gauge(metrics, pullZoneMonthlyBandwidthLimitDesc, float64(zone.MonthlyBandwidthLimit), labels...)
		waitGroup.Go(func() { scrapePullZoneStatistics(metrics, labels) })
	}
	waitGroup.Wait()
}

// Totals since 00:00 UTC are exposed as counters, so they reset daily and increase()/rate() handle it.
func scrapePullZoneStatistics(metrics chan<- prometheus.Metric, labels []string) {
	zoneID := labels[0]
	now := time.Now().UTC()
	query := url.Values{
		"pullZone":                {zoneID},
		"dateFrom":                {now.Truncate(24 * time.Hour).Format(time.RFC3339)},
		"dateTo":                  {now.Format(time.RFC3339)},
		"loadErrors":              {"true"},
		"loadBandwidthUsed":       {"true"},
		"loadRequestsServed":      {"true"},
		"loadOriginTraffic":       {"true"},
		"loadOriginResponseTimes": {"true"},
	}

	var statistics pullZoneStatistics
	err := getJSON("/statistics?"+query.Encode(), &statistics)
	recordScrape(metrics, "pullzone_statistics", zoneID, err)
	if err != nil {
		return
	}

	counter(metrics, pullZoneBandwidthDesc, float64(statistics.TotalBandwidthUsed), labels...)
	counter(metrics, pullZoneOriginTrafficDesc, float64(statistics.TotalOriginTraffic), labels...)
	counter(metrics, pullZoneRequestsDesc, float64(statistics.TotalRequestsServed), labels...)
	counter(metrics, pullZoneErrorResponsesDesc, sumChart(statistics.Error3xxChart), append(labels, "3xx")...)
	counter(metrics, pullZoneErrorResponsesDesc, sumChart(statistics.Error4xxChart), append(labels, "4xx")...)
	counter(metrics, pullZoneErrorResponsesDesc, sumChart(statistics.Error5xxChart), append(labels, "5xx")...)
	gauge(metrics, pullZoneCacheHitRateDesc, statistics.CacheHitRate, labels...)
	gauge(metrics, pullZoneOriginResponseTimeDesc, float64(statistics.AverageOriginResponseTime), labels...)
}

// Describe sends nothing, which makes this an unchecked collector since the series are only known after calling the API.
func (collector) Describe(chan<- *prometheus.Desc) {}

func (collector) Collect(metrics chan<- prometheus.Metric) {
	var waitGroup sync.WaitGroup
	waitGroup.Go(func() { scrapeApps(metrics) })
	waitGroup.Go(func() { scrapePullZones(metrics) })
	waitGroup.Go(func() { scrapeStorageZones(metrics) })
	waitGroup.Wait()
}

func main() {
	prometheus.MustRegister(collector{})
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
