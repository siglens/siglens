// package ingest

// import (
// 	"fmt"
// 	"math/rand"
// 	"time"
// 	"encoding/json"
// 	"github.com/brianvoe/gofakeit/v6"
// )

// type K8sGenerator struct {
// 	faker         *gofakeit.Faker
// 	metricCounter int
// 	nodes         []string
// 	namespaces    []string
// 	pods          []podInfo
// 	seed          int64
// }

// type podInfo struct {
// 	name       string
// 	namespace  string
// 	node       string
// 	phase      string
// 	conditions map[string]bool
// }

// func InitK8sGenerator(seed int64) *K8sGenerator {
// 	g := &K8sGenerator{
// 		seed:  seed,
// 		nodes: []string{"node-01", "node-02", "node-03"},
// 		namespaces: []string{
// 			"default", "kube-system", "monitoring",
// 			"prod-backend", "prod-database",
// 		},
// 	}

// 	rand.Seed(seed)
// 	g.faker = gofakeit.NewUnlocked(seed)
// 	g.initializeClusterState()
// 	return g
// }

// func (g *K8sGenerator) initializeClusterState() {
// 	// Generate realistic cluster state
// 	g.pods = make([]podInfo, 0)
// 	for i := 0; i < 50; i++ {
// 		g.pods = append(g.pods, podInfo{
// 			name:      fmt.Sprintf("%s-%d", g.faker.AppName(), i),
// 			namespace: g.namespaces[rand.Intn(len(g.namespaces))],
// 			node:      g.nodes[rand.Intn(len(g.nodes))],
// 			phase:     randomPodPhase(),
// 			conditions: map[string]bool{
// 				"Ready":          rand.Float32() < 0.8,
// 				"MemoryPressure": rand.Float32() < 0.1,
// 				"DiskPressure":   rand.Float32() < 0.05,
// 			},
// 		})
// 	}
// }

// func (g *K8sGenerator) GetLogLine() ([]byte, error) {
// 	raw, err := g.GetRawLog()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return json.Marshal(raw)
// }

// func (g *K8sGenerator) GetRawLog() (map[string]interface{}, error) {
// 	g.metricCounter++
	
// 	// Alternate between KSM and node-exporter metrics
// 	if g.metricCounter%2 == 0 {
// 		return g.generateKSMMetric(), nil
// 	}
// 	return g.generateNodeExporterMetric(), nil
// }

// func (g *K8sGenerator) Init(...string) error { return nil }

// func (g *K8sGenerator) generateKSMMetric() map[string]interface{} {
// 	metricType := rand.Intn(5)
// 	pod := g.pods[rand.Intn(len(g.pods))]

// 	switch metricType {
// 	case 0: // kube_pod_info
// 		return map[string]interface{}{
// 			"metric": "kube_pod_info",
// 			"value":  1,
// 			"tags": map[string]string{
// 				"pod":       pod.name,
// 				"namespace": pod.namespace,
// 				"node":      pod.node,
// 				"phase":     pod.phase,
// 				"created_by_kind": "Deployment",
// 				"created_by_name": fmt.Sprintf("%s-deployment", g.faker.AppName()),
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}
	
// 	case 1: // kube_node_status_condition
// 		node := g.nodes[rand.Intn(len(g.nodes))]
// 		return map[string]interface{}{
// 			"metric": "kube_node_status_condition",
// 			"value":  boolFloat(rand.Float32() < 0.95),
// 			"tags": map[string]string{
// 				"node":      node,
// 				"condition": "Ready",
// 				"status":    "true",
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	case 2: // kube_deployment_status_replicas
// 		return map[string]interface{}{
// 			"metric": "kube_deployment_status_replicas",
// 			"value":  float64(rand.Intn(10) + 1),
// 			"tags": map[string]string{
// 				"namespace":  g.namespaces[rand.Intn(len(g.namespaces))],
// 				"deployment": fmt.Sprintf("%s-deployment", g.faker.AppName()),
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	case 3: // kube_service_info
// 		return map[string]interface{}{
// 			"metric": "kube_service_info",
// 			"value":  1,
// 			"tags": map[string]string{
// 				"namespace": g.namespaces[rand.Intn(len(g.namespaces))],
// 				"service":   fmt.Sprintf("%s-service", g.faker.AppName()),
// 				"cluster_ip": g.faker.IPv4Address(),
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	default: // kube_namespace_labels
// 		return map[string]interface{}{
// 			"metric": "kube_namespace_labels",
// 			"value":  1,
// 			"tags": map[string]string{
// 				"namespace": g.namespaces[rand.Intn(len(g.namespaces))],
// 				"label_env": []string{"prod", "staging", "dev"}[rand.Intn(3)],
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}
// 	}
// }

// func (g *K8sGenerator) generateNodeExporterMetric() map[string]interface{} {
// 	metricType := rand.Intn(5)
// 	node := g.nodes[rand.Intn(len(g.nodes))]

// 	switch metricType {
// 	case 0: // node_cpu_seconds_total
// 		return map[string]interface{}{
// 			"metric": "node_cpu_seconds_total",
// 			"value":  g.faker.Float64Range(1000, 100000),
// 			"tags": map[string]string{
// 				"node": node,
// 				"cpu":  fmt.Sprintf("%d", rand.Intn(8)),
// 				"mode": []string{"user", "system", "iowait"}[rand.Intn(3)],
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	case 1: // node_memory_MemFree_bytes
// 		return map[string]interface{}{
// 			"metric": "node_memory_MemFree_bytes",
// 			"value":  g.faker.Float64Range(1e9, 16e9),
// 			"tags": map[string]string{
// 				"node": node,
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	case 2: // node_filesystem_avail_bytes
// 		return map[string]interface{}{
// 			"metric": "node_filesystem_avail_bytes",
// 			"value":  g.faker.Float64Range(100e9, 500e9),
// 			"tags": map[string]string{
// 				"node":   node,
// 				"device": []string{"/dev/sda1", "/dev/nvme0n1p1"}[rand.Intn(2)],
// 				"fstype": "ext4",
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	case 3: // node_network_up
// 		return map[string]interface{}{
// 			"metric": "node_network_up",
// 			"value":  boolFloat(rand.Float32() < 0.99),
// 			"tags": map[string]string{
// 				"node":     node,
// 				"device":   "eth0",
// 				"instance": fmt.Sprintf("%s:9100", node),
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}

// 	default: // node_disk_io_time_seconds_total
// 		return map[string]interface{}{
// 			"metric": "node_disk_io_time_seconds_total",
// 			"value":  g.faker.Float64Range(1000, 50000),
// 			"tags": map[string]string{
// 				"node": node,
// 				"device": []string{"sda", "nvme0n1"}[rand.Intn(2)],
// 			},
// 			"timestamp": time.Now().Unix(),
// 		}
// 	}
// }

// func boolFloat(b bool) float64 {
// 	if b {
// 		return 1.0
// 	}
// 	return 0.0
// }

// func randomPodPhase() string {
// 	phases := []string{"Running", "Pending", "Succeeded", "Failed"}
// 	return phases[rand.Intn(len(phases))]
// }

package ingest

import (
	"fmt"
	"math/rand"
	"time"
	"encoding/json"
	"github.com/brianvoe/gofakeit/v6"
)

type K8sGenerator struct {
	faker         *gofakeit.Faker
	metricCounter int
	nodes         []string
	namespaces    []string
	pods          []podInfo
	seed          int64
	ksmOnly       bool
	nodeOnly      bool
	bothMetrics   bool
}

type podInfo struct {
	name       string
	namespace  string
	node       string
	phase      string
	conditions map[string]bool
}

func InitK8sGenerator(seed int64, ksmOnly, nodeOnly, bothMetrics bool) *K8sGenerator {
	g := &K8sGenerator{
		seed:      seed,
		ksmOnly:   ksmOnly,
		nodeOnly:  nodeOnly,
		bothMetrics: bothMetrics,
		nodes:     []string{"node-01", "node-02", "node-03"},
		namespaces: []string{
			"default", "kube-system", "monitoring",
			"prod-backend", "prod-database",
		},
	}

	rand.Seed(seed)
	g.faker = gofakeit.NewUnlocked(seed)
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

func (g *K8sGenerator) GetLogLine() ([]byte, error) {
	raw, err := g.GetRawLog()
	if err != nil {
		return nil, err
	}
	return json.Marshal(raw)
}

func (g *K8sGenerator) GetRawLog() (map[string]interface{}, error) {
	g.metricCounter++

	if g.bothMetrics {
		// Generate both KSM and node-exporter metrics
		ksmMetric := g.generateKSMMetric()
		nodeMetric := g.generateNodeExporterMetric()
		return map[string]interface{}{
			"ksm_metric":   ksmMetric,
			"node_metric": nodeMetric,
		}, nil
	}

	if g.ksmOnly {
		return g.generateKSMMetric(), nil
	}

	if g.nodeOnly {
		return g.generateNodeExporterMetric(), nil
	}

	// Default to alternating behavior if no flags are set
	if g.metricCounter%2 == 0 {
		return g.generateKSMMetric(), nil
	}
	return g.generateNodeExporterMetric(), nil
}

func (g *K8sGenerator) Init(...string) error { return nil }

func (g *K8sGenerator) generateKSMMetric() map[string]interface{} {
	metricType := rand.Intn(5)
	pod := g.pods[rand.Intn(len(g.pods))]

	switch metricType {
	case 0: // kube_pod_info
		return map[string]interface{}{
			"metric": "kube_pod_info",
			"value":  1,
			"tags": map[string]string{
				"pod":       pod.name,
				"namespace": pod.namespace,
				"node":      pod.node,
				"phase":     pod.phase,
				"created_by_kind": "Deployment",
				"created_by_name": fmt.Sprintf("%s-deployment", g.faker.AppName()),
			},
			"timestamp": time.Now().Unix(),
		}
	
	case 1: // kube_node_status_condition
		node := g.nodes[rand.Intn(len(g.nodes))]
		return map[string]interface{}{
			"metric": "kube_node_status_condition",
			"value":  boolFloat(rand.Float32() < 0.95),
			"tags": map[string]string{
				"node":      node,
				"condition": "Ready",
				"status":    "true",
			},
			"timestamp": time.Now().Unix(),
		}

	case 2: // kube_deployment_status_replicas
		return map[string]interface{}{
			"metric": "kube_deployment_status_replicas",
			"value":  float64(rand.Intn(10) + 1),
			"tags": map[string]string{
				"namespace":  g.namespaces[rand.Intn(len(g.namespaces))],
				"deployment": fmt.Sprintf("%s-deployment", g.faker.AppName()),
			},
			"timestamp": time.Now().Unix(),
		}

	case 3: // kube_service_info
		return map[string]interface{}{
			"metric": "kube_service_info",
			"value":  1,
			"tags": map[string]string{
				"namespace": g.namespaces[rand.Intn(len(g.namespaces))],
				"service":   fmt.Sprintf("%s-service", g.faker.AppName()),
				"cluster_ip": g.faker.IPv4Address(),
			},
			"timestamp": time.Now().Unix(),
		}

	default: // kube_namespace_labels
		return map[string]interface{}{
			"metric": "kube_namespace_labels",
			"value":  1,
			"tags": map[string]string{
				"namespace": g.namespaces[rand.Intn(len(g.namespaces))],
				"label_env": []string{"prod", "staging", "dev"}[rand.Intn(3)],
			},
			"timestamp": time.Now().Unix(),
		}
	}
}

func (g *K8sGenerator) generateNodeExporterMetric() map[string]interface{} {
	metricType := rand.Intn(5)
	node := g.nodes[rand.Intn(len(g.nodes))]

	switch metricType {
	case 0: // node_cpu_seconds_total
		return map[string]interface{}{
			"metric": "node_cpu_seconds_total",
			"value":  g.faker.Float64Range(1000, 100000),
			"tags": map[string]string{
				"node": node,
				"cpu":  fmt.Sprintf("%d", rand.Intn(8)),
				"mode": []string{"user", "system", "iowait"}[rand.Intn(3)],
			},
			"timestamp": time.Now().Unix(),
		}

	case 1: // node_memory_MemFree_bytes
		return map[string]interface{}{
			"metric": "node_memory_MemFree_bytes",
			"value":  g.faker.Float64Range(1e9, 16e9),
			"tags": map[string]string{
				"node": node,
			},
			"timestamp": time.Now().Unix(),
		}

	case 2: // node_filesystem_avail_bytes
		return map[string]interface{}{
			"metric": "node_filesystem_avail_bytes",
			"value":  g.faker.Float64Range(100e9, 500e9),
			"tags": map[string]string{
				"node":   node,
				"device": []string{"/dev/sda1", "/dev/nvme0n1p1"}[rand.Intn(2)],
				"fstype": "ext4",
			},
			"timestamp": time.Now().Unix(),
		}

	case 3: // node_network_up
		return map[string]interface{}{
			"metric": "node_network_up",
			"value":  boolFloat(rand.Float32() < 0.99),
			"tags": map[string]string{
				"node":     node,
				"device":   "eth0",
				"instance": fmt.Sprintf("%s:9100", node),
			},
			"timestamp": time.Now().Unix(),
		}

	default: // node_disk_io_time_seconds_total
		return map[string]interface{}{
			"metric": "node_disk_io_time_seconds_total",
			"value":  g.faker.Float64Range(1000, 50000),
			"tags": map[string]string{
				"node": node,
				"device": []string{"sda", "nvme0n1"}[rand.Intn(2)],
			},
			"timestamp": time.Now().Unix(),
		}
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