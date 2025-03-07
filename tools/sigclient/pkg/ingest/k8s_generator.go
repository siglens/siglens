package ingest

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type K8sGenerator struct {
	faker         *gofakeit.Faker
	metricCounter int
	nodes         []string
	namespaces    []string
	pods          []podInfo
	seed          int64
	metricType    MetricGeneratorType
}

type podInfo struct {
	name       string
	namespace  string
	node       string
	phase      string
	conditions map[string]bool
}

func InitK8sGenerator(seed int64, metricType MetricGeneratorType) *K8sGenerator {
	g := &K8sGenerator{
		seed:       seed,
		metricType: metricType,

		namespaces: []string{
			"default", "kube-system", "monitoring",
			"prod-backend", "prod-database",
		},
	}

	// Initialize random and faker first
	rand.Seed(seed)
	g.faker = gofakeit.NewUnlocked(seed)

	// Initialize nodes based on flags
	if metricType == GenerateNodeExporterOnly || metricType == GenerateBothMetrics {

		// Generate 20 dynamic nodes using proper faker methods
		g.nodes = make([]string, 20)
		for i := 0; i < 20; i++ {
			// Example using first name prefix
			prefix := strings.ToLower(g.faker.FirstName())
			g.nodes[i] = fmt.Sprintf("node-%s-%02d", prefix, i+1)
		}
	} else {
		// Default 3 nodes for KSM cases
		g.nodes = []string{"node-01", "node-02", "node-03"}
	}

	g.initializeClusterState()
	return g
}
func (g *K8sGenerator) initializeClusterState() {
	// Generate realistic cluster state
	g.pods = make([]podInfo, 0)
	for i := 0; i < 50; i++ {
		g.pods = append(g.pods, podInfo{
			name:      fmt.Sprintf("%s-%d", g.faker.AppName(), i),
			namespace: g.namespaces[rand.Intn(len(g.namespaces))],
			node:      g.nodes[rand.Intn(len(g.nodes))],
			phase:     randomPodPhase(),
			conditions: map[string]bool{
				"Ready":          rand.Float32() < 0.8,
				"MemoryPressure": rand.Float32() < 0.1,
				"DiskPressure":   rand.Float32() < 0.05,
			},
		})
	}
}

func (g *K8sGenerator) Init(args ...string) error {
	// No initialization needed for K8sGenerator
	return nil
}

func (g *K8sGenerator) GetLogLine() ([]byte, error) {
	raw, err := g.GetRawLog()
	if err != nil {
		return nil, err
	}
	return json.Marshal(raw)
}

func (g *K8sGenerator) GetRawLog() (map[string]interface{}, error) {
	g.metricCounter++

	// Generate base timestamp
	baseTs := time.Now().Unix()

	var metrics []map[string]interface{}

	switch g.metricType {
	case GenerateBothMetrics:
		// Generate both KSM and node-exporter metrics
		metrics = append(metrics, g.generateKSMMetric(baseTs))
		metrics = append(metrics, g.generateNodeExporterMetric(baseTs))
	case GenerateKSMOnly:
		// Generate KSM only
		metrics = append(metrics, g.generateKSMMetric(baseTs))

	case GenerateNodeExporterOnly:
		// Generate node-exporter only
		metrics = append(metrics, g.generateNodeExporterMetric(baseTs))

	default:
		// Default alternating behavior
		if g.metricCounter%2 == 0 {
			metrics = append(metrics, g.generateKSMMetric(baseTs))
		} else {
			metrics = append(metrics, g.generateNodeExporterMetric(baseTs))
		}

	}

	// Return first metric to preserve existing interface
	if len(metrics) > 0 {
		return metrics[0], nil
	}
	return nil, fmt.Errorf("no metrics generated")
}

func (g *K8sGenerator) generateKSMMetric(timestamp int64) map[string]interface{} {
	metricType := rand.Intn(5)
	pod := g.pods[rand.Intn(len(g.pods))]

	tags := make(map[string]interface{}) // Ensure tags is map[string]interface{}
	switch metricType {
	case 0: // kube_pod_info
		tags["pod"] = pod.name
		tags["namespace"] = pod.namespace
		tags["node"] = pod.node
		tags["phase"] = pod.phase
		tags["created_by_kind"] = "Deployment"
		tags["created_by_name"] = fmt.Sprintf("%s-deployment", g.faker.AppName())
	case 1: // kube_node_status_condition
		node := g.nodes[rand.Intn(len(g.nodes))]
		tags["node"] = node
		tags["condition"] = "Ready"
		tags["status"] = "true"
	case 2: // kube_deployment_status_replicas
		tags["namespace"] = g.namespaces[rand.Intn(len(g.namespaces))]
		tags["deployment"] = fmt.Sprintf("%s-deployment", g.faker.AppName())
	case 3: // kube_service_info
		tags["namespace"] = g.namespaces[rand.Intn(len(g.namespaces))]
		tags["service"] = fmt.Sprintf("%s-service", g.faker.AppName())
		tags["cluster_ip"] = g.faker.IPv4Address()
	default: // kube_namespace_labels
		tags["namespace"] = g.namespaces[rand.Intn(len(g.namespaces))]
		tags["label_env"] = []string{"prod", "staging", "dev"}[rand.Intn(3)]
	}

	return map[string]interface{}{
		"metric":    g.getKSMMetricName(metricType),
		"value":     g.getKSMMetricValue(metricType),
		"tags":      tags,
		"timestamp": timestamp,
	}
}

func (g *K8sGenerator) generateNodeExporterMetric(timestamp int64) map[string]interface{} {
	metricType := rand.Intn(5)
	node := g.nodes[rand.Intn(len(g.nodes))]

	tags := make(map[string]interface{})
	// Common tags for all node metrics
	tags["instance"] = fmt.Sprintf("%s:9100", node)
	tags["region"] = g.faker.State()
	tags["zone"] = fmt.Sprintf("%s-%d", g.faker.TimeZone(), rand.Intn(3)+1)

	switch metricType {
	case 0: // node_cpu_seconds_total
		tags["node"] = node
		tags["cpu"] = fmt.Sprintf("%d", rand.Intn(32)) // 0-31
		tags["mode"] = []string{"user", "system", "iowait"}[rand.Intn(3)]

	case 1: // node_memory_MemFree_bytes
		tags["node"] = node
		tags["memory_type"] = []string{"dram", "cache", "swap"}[rand.Intn(3)]

	case 2: // node_filesystem_avail_bytes
		tags["node"] = node
		tags["device"] = []string{"/dev/sda1", "/dev/nvme0n1p1"}[rand.Intn(2)]
		tags["fstype"] = "ext4"
		tags["mountpoint"] = []string{"/", "/var/lib", "/home"}[rand.Intn(3)]

	case 3: // node_network_up
		tags["node"] = node
		tags["device"] = "eth0"
		tags["speed"] = []string{"1Gbps", "10Gbps", "25Gbps"}[rand.Intn(3)]

	default: // node_disk_io_time_seconds_total
		tags["node"] = node
		tags["device"] = []string{"sda", "nvme0n1"}[rand.Intn(2)]
		tags["disk_type"] = []string{"ssd", "hdd", "nvme"}[rand.Intn(3)]
	}

	return map[string]interface{}{
		"metric":    g.getNodeExporterMetricName(metricType),
		"value":     g.getNodeExporterMetricValue(metricType),
		"tags":      tags,
		"timestamp": timestamp,
	}
}

// helper function to simplify metrics genration
func (g *K8sGenerator) getKSMMetricName(metricType int) string {
	switch metricType {
	case 0:
		return "kube_pod_info"
	case 1:
		return "kube_node_status_condition"
	case 2:
		return "kube_deployment_status_replicas"
	case 3:
		return "kube_service_info"
	default:
		return "kube_namespace_labels"
	}
}

func (g *K8sGenerator) getKSMMetricValue(metricType int) interface{} {
	switch metricType {
	case 0, 3:
		return 1
	case 1:
		return boolFloat(rand.Float32() < 0.95)
	case 2:
		return float64(rand.Intn(10) + 1)
	default:
		return 1
	}
}

func (g *K8sGenerator) getNodeExporterMetricName(metricType int) string {
	switch metricType {
	case 0:
		return "node_cpu_seconds_total"
	case 1:
		return "node_memory_MemFree_bytes"
	case 2:
		return "node_filesystem_avail_bytes"
	case 3:
		return "node_network_up"
	default:
		return "node_disk_io_time_seconds_total"
	}
}

func (g *K8sGenerator) getNodeExporterMetricValue(metricType int) interface{} {
	switch metricType {
	case 0:
		return g.faker.Float64Range(1000, 100000)
	case 1:
		return g.faker.Float64Range(1e9, 16e9)
	case 2:
		return g.faker.Float64Range(100e9, 500e9)
	case 3:
		return boolFloat(rand.Float32() < 0.99)
	default:
		return g.faker.Float64Range(1000, 50000)
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

func randomPodPhase() string {
	phases := []string{"Running", "Pending", "Succeeded", "Failed"}
	return phases[rand.Intn(len(phases))]
}
