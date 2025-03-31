/**
 * @param {Object} config - Configuration options
 * @param {string} config.timeRange - Time range for queries (default: "1h")
 * @param {string} config.rateInterval - Rate interval for queries (default: "5m")
 * @param {string} config.clusterPattern - Regex pattern for cluster selection (default: ".+")
 * @param {string} config.namespacePattern - Regex pattern for namespace selection (default: ".+")
 * @param {string} config.nodePattern - Regex pattern for node selection (default: ".+")
 * @param {string} config.workloadPattern - Regex pattern for workload selection (default: ".+")
 */

const getClusterMonitoringQueries = (config = {}) => {
    const timeRange = config.timeRange || '1h';
    const rateInterval = config.rateInterval || '5m';
    const clusterPattern = config.clusterPattern || '.+';

    return {

        CPU_USAGE_AVG: `avg_over_time(
    sum by (cluster) (
      1 - max by (cluster, instance, cpu, core) (
        rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
      )
    )[${timeRange}:${rateInterval}])`,

        CPU_USAGE_AVG_PERCENT: `avg_over_time(
    sum by (cluster) (
      1 - max by (cluster, instance, cpu, core) (
        rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
      )
    )[${timeRange}:${rateInterval}])
    / sum by (cluster) (
      max by (cluster, node) (kube_node_status_capacity{resource="cpu", cluster=~"${clusterPattern}"})
    )`,

        CPU_USAGE_MAX: `max_over_time(
    sum by (cluster) (
      1 - max by (cluster, instance, cpu, core) (
        rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
      )
    )[${timeRange}:${rateInterval}])`,

        CPU_USAGE_MAX_PERCENT: `max_over_time(
    sum by (cluster) (
      1 - max by (cluster, instance, cpu, core) (
        rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
      )
    )[${timeRange}:${rateInterval}])
    / sum by (cluster) (
      max by (cluster, node) (kube_node_status_capacity{resource="cpu", cluster=~"${clusterPattern}"})
    )`,

        MEMORY_USAGE_AVG: `avg_over_time(
      sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{resource="memory", cluster=~"${clusterPattern}"})
        - on (cluster, node) group_left
        max by (cluster, node) (
          label_replace(
            windows_memory_available_bytes{}
            OR
            node_memory_MemAvailable_bytes{},
            "node", "$1", "instance", "([^:]+).*"
          )
        )
      )[${timeRange}:1m])`,

        MEMORY_USAGE_AVG_PERCENT: `avg_over_time(
      sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{resource="memory", cluster=~"${clusterPattern}"})
        - on (cluster, node) group_left
        max by (cluster, node) (
          label_replace(
            windows_memory_available_bytes{}
            OR
            node_memory_MemAvailable_bytes{},
            "node", "$1", "instance", "([^:]+).*"
          )
        )
      )[${timeRange}:1m])
      / sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{resource="memory", cluster=~"${clusterPattern}"})
      )`,

        MEMORY_USAGE_MAX: `max_over_time(
      sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{resource="memory", cluster=~"${clusterPattern}"})
        - on (cluster, node) group_left
        max by (cluster, node) (
          label_replace(
            windows_memory_available_bytes{}
            OR
            node_memory_MemAvailable_bytes{},
            "node", "$1", "instance", "([^:]+).*"
          )
        )
      )[${timeRange}:1m])`,

        MEMORY_USAGE_MAX_PERCENT: `max_over_time(
      sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{resource="memory", cluster=~"${clusterPattern}"})
        - on (cluster, node) group_left
        max by (cluster, node) (
          label_replace(
            windows_memory_available_bytes{}
            OR
            node_memory_MemAvailable_bytes{},
            "node", "$1", "instance", "([^:]+).*"
          )
        )
      )[${timeRange}:1m])
      / sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{resource="memory", cluster=~"${clusterPattern}"})
      )`,
    };
};

const getNamespaceMonitoringQueries = (config = {}) => {
    const timeRange = config.timeRange || '1h';
    const clusterPattern = config.clusterPattern || '.+';
    const namespacePattern = config.namespacePattern || '.+';

    return {

        CPU_USAGE_AVG: `avg_over_time(
      sum by (cluster, namespace) (
        max by (cluster, namespace, pod, container) (
          node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
        )
      )[${timeRange}:1m]
    )`,

        CPU_USAGE_AVG_PERCENT: `avg_over_time(
      sum by (cluster, namespace) (
        max by (cluster, namespace, pod, container) (
          node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
        )
      )[${timeRange}:1m]
    ) / on (cluster, namespace) group_left sum by (cluster, namespace) (
      namespace_cpu:kube_pod_container_resource_requests:sum{namespace=~"${namespacePattern}", cluster=~"${clusterPattern}"}
    )`,

        CPU_USAGE_MAX: `max_over_time(
      sum by (cluster, namespace) (
        max by (cluster, namespace, pod, container) (
          node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
        )
      )[${timeRange}:1m]
    )`,

        CPU_USAGE_MAX_PERCENT: `max_over_time(
      sum by (cluster, namespace) (
        max by (cluster, namespace, pod, container) (
          node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
        )
      )[${timeRange}:1m]
    ) / on (cluster, namespace) group_left sum by (cluster, namespace) (
      namespace_cpu:kube_pod_container_resource_requests:sum{namespace=~"${namespacePattern}", cluster=~"${clusterPattern}"}
    )`,

        MEMORY_USAGE_AVG: `avg_over_time(
          sum by (cluster, namespace) (
            max by (cluster, namespace, pod, container) (
              node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
            )
          )[${timeRange}:1m]
        )`,

        MEMORY_USAGE_AVG_PERCENT: `avg_over_time(
          sum by (cluster, namespace) (
            max by (cluster, namespace, pod, container) (
              node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
            )
          )[${timeRange}:1m]
        ) / on (cluster, namespace) group_left sum by (cluster, namespace) (
          namespace_memory:kube_pod_container_resource_requests:sum{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}"}
        )`,

        MEMORY_USAGE_MAX: `max_over_time(
          sum by (cluster, namespace) (
            max by (cluster, namespace, pod, container) (
              node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
            )
          )[${timeRange}:1m]
        )`,

        MEMORY_USAGE_MAX_PERCENT: `max_over_time(
          sum by (cluster, namespace) (
            max by (cluster, namespace, pod, container) (
              node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!=""}
            )
          )[${timeRange}:1m]
        ) / on (cluster, namespace) group_left sum by (cluster, namespace) (
          namespace_memory:kube_pod_container_resource_requests:sum{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}"}
        )`,
    };
};

const getWorkloadMonitoringQueries = (config = {}) => {
    const timeRange = config.timeRange || '1h';
    const rateInterval = config.rateInterval || '5m';
    const clusterPattern = config.clusterPattern || '.+';
    const namespacePattern = config.namespacePattern || '.+';
    const workloadPattern = config.workloadPattern || '.+';

    return {

        CPU_USAGE_AVG: `sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    avg_over_time(
      sum by (pod) (max by (pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+-.+", container!=""}))
      [${timeRange}:${rateInterval}]
    )
  )`,

        CPU_USAGE_AVG_PERCENT: `sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    avg_over_time(
      sum by (pod) (max by (pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+-.+", container!=""}))
      [${timeRange}:${rateInterval}]
    )
  ) / sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    sum by (pod) (kube_pod_container_resource_requests{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!="", pod=~".+-.+", resource="cpu"})
  )`,

        CPU_USAGE_MAX: `sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    max_over_time(
      sum by (pod) (max by (pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+-.+", container!=""}))
      [${timeRange}:${rateInterval}]
    )
  )`,

        CPU_USAGE_MAX_PERCENT: `sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    max_over_time(
      sum by (pod) (max by (pod, container) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+-.+", container!=""}))
      [${timeRange}:${rateInterval}]
    )
  ) / sum by (cluster, namespace, workload,workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    sum by (pod) (kube_pod_container_resource_requests{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!="", pod=~".+-.+", resource="cpu"})
  )`,

        MEMORY_USAGE_AVG: `sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~".+", pod=~".+"}

      OR

      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")

      OR

      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    avg_over_time(
        sum by (pod) (max by (pod, container) (node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+-.+", container!=""}))
        [${timeRange}:${rateInterval}]
    )
  )`,

        MEMORY_USAGE_AVG_PERCENT: `sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}
  
      OR
  
      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")
  
      OR
  
      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")
  
      OR
  
      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    avg_over_time(
      sum by (pod) (
        max by (pod, container) (
          node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+-.+", container!=""}
        )
      )[${timeRange}:${rateInterval}]
    )
  ) 
  / 
  sum by (cluster, namespace, workload, workload_type) (
    group by (cluster, namespace, workload, pod, workload_type) (
      namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}
  
      OR
  
      label_replace(
        label_replace(
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
        , "workload", "$1", "pod", "(.+)-(.+)")
      , "workload_type", "replicaset", "", "")
  
      OR
  
      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "pod", "", "")
  
      OR
  
      label_replace(
        label_replace(
          kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
        , "workload", "$1", "pod", "(.+)")
      , "workload_type", "staticpod", "", "")
    )
    * on (pod) group_left
    sum by (pod) (
      kube_pod_container_resource_requests{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", container!="", pod=~".+-.+", resource="memory"}
    )
  )
  `,

        MEMORY_USAGE_MAX: `sum by (cluster, namespace, workload, workload_type) (
        group by (cluster, namespace, workload, pod, workload_type) (
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload=~"${workloadPattern}", pod=~".+"}
      
          OR
      
          label_replace(
            label_replace(
              namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload_type=~".+", workload="", pod=~".+.+"}
            , "workload", "$1", "pod", "(.+)-(.+)")
          , "workload_type", "replicaset", "", "")
      
          OR
      
          label_replace(
            label_replace(
              kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind=""}
            , "workload", "$1", "pod", "(.+)")
          , "workload_type", "pod", "", "")
      
          OR
      
          label_replace(
            label_replace(
              kube_pod_owner{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", owner_kind="Node"}
            , "workload", "$1", "pod", "(.+)")
          , "workload_type", "staticpod", "", "")
        )
        * on (pod) group_left
        max_over_time(
          sum by (pod) (
            max by (pod, container) (
              node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", pod=~".+", container!=""}
            )
          )
          [${timeRange}:${rateInterval}]
        )
      )
      `,

        MEMORY_USAGE_MAX_PERCENT: `sum by (cluster, namespace, workload, workload_type) (
        group by (cluster, namespace, workload, pod, workload_type) (
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload=~"${workloadPattern}"}
        )
        * on (pod) group_left
        max_over_time(
          sum by (pod) (
            max by (pod, container) (
              node_namespace_pod_container:container_memory_working_set_bytes{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}"}
            )
          )
          [${timeRange}:1m]
        )
      ) / sum by (cluster, namespace, workload, workload_type) (
        group by (cluster, namespace, workload, pod, workload_type) (
          namespace_workload_pod:kube_pod_owner:relabel{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", workload=~"${workloadPattern}"}
        )
        * on (pod) group_left
        sum by (pod) (
          kube_pod_container_resource_requests{cluster=~"${clusterPattern}", namespace=~"${namespacePattern}", resource="memory"}
        )
      )
      `,
    };
};

const getNodeMonitoringQueries = (config = {}) => {
    const timeRange = config.timeRange || '1h';
    const rateInterval = config.rateInterval || '5m';
    const clusterPattern = config.clusterPattern || '.+';

    return {
        CPU_USAGE_AVG: `avg_over_time(
      sum by (cluster, node) (
        label_replace(
          1 - max by (cluster, instance, cpu, core) (
            rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
          ), "node", "$1", "instance", "([^:]+).*"
        )
      )[${timeRange}:${rateInterval}])`,

        CPU_USAGE_AVG_PERCENT: `avg_over_time(
      sum by (cluster, node) (
        label_replace(
          1 - max by (cluster, instance, cpu, core) (
            rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
          ), "node", "$1", "instance", "([^:]+).*"
        )
      )[${timeRange}:${rateInterval}]) /
      sum by (cluster, node) (
        max by (cluster, node) (kube_node_status_capacity{resource="cpu", cluster=~"${clusterPattern}"})
      )`,

        CPU_USAGE_MAX: `max_over_time(
      sum by (cluster, node) (
        label_replace(
          1 - max by (cluster, instance, cpu, core) (
            rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
          ), "node", "$1", "instance", "([^:]+).*"
        )
      )[${timeRange}:${rateInterval}])`,

        CPU_USAGE_MAX_PERCENT: `max_over_time(
      sum by (cluster, node) (
        label_replace(
          1 - max by (cluster, instance, cpu, core) (
            rate(node_cpu_seconds_total{mode="idle", cluster=~"${clusterPattern}"}[${rateInterval}])
          ), "node", "$1", "instance", "([^:]+).*"
        )
      )[${timeRange}:${rateInterval}]) /
      sum by (cluster, node) (
        max by (cluster, node) (kube_node_status_capacity{resource="cpu", cluster=~"${clusterPattern}"})
      )`,

        MEMORY_USAGE_AVG: `avg_over_time(
        sum by (cluster, node) (
          sum by (cluster, node) (
            max by (cluster, node) (kube_node_status_capacity{cluster=~"${clusterPattern}", resource="memory"})
          )
          - on (cluster, node) group_left
          max by (cluster, node) (
            label_replace(
              windows_memory_available_bytes{cluster=~"${clusterPattern}"}
              OR
              node_memory_MemAvailable_bytes{cluster=~"${clusterPattern}"}
              , "node", "$1", "instance", "([^:]+).*"
            )
          )
        )[${timeRange}:1m])`,

        MEMORY_USAGE_AVG_PERCENT: `avg_over_time(
        sum by (cluster, node) (
          sum by (cluster, node) (
            max by (cluster, node) (kube_node_status_capacity{cluster=~"${clusterPattern}", resource="memory"})
          )
          - on (cluster, node) group_left
          max by (cluster, node) (
            label_replace(
              windows_memory_available_bytes{cluster=~"${clusterPattern}"}
              OR
              node_memory_MemAvailable_bytes{cluster=~"${clusterPattern}"}
              , "node", "$1", "instance", "([^:]+).*"
            )
          )
        )[${timeRange}:1m]) /
        sum by (cluster, node) (
          max by (cluster, node) (kube_node_status_capacity{cluster=~"${clusterPattern}", resource="memory"})
        )`,

        MEMORY_USAGE_MAX: `max_over_time(
        sum by (cluster, node) (
          sum by (cluster, node) (
            max by (cluster, node) (kube_node_status_capacity{cluster=~"${clusterPattern}", resource="memory"})
          )
          - on (cluster, node) group_left
          max by (cluster, node) (
            label_replace(
              windows_memory_available_bytes{cluster=~"${clusterPattern}"}
              OR
              node_memory_MemAvailable_bytes{cluster=~"${clusterPattern}"}
              , "node", "$1", "instance", "([^:]+).*"
            )
          )
        )[${timeRange}:1m])`,

        MEMORY_USAGE_MAX_PERCENT: `max_over_time(
        sum by (cluster, node) (
          sum by (cluster, node) (
            max by (cluster, node) (kube_node_status_capacity{cluster=~"${clusterPattern}", resource="memory"})
          )
          - on (cluster, node) group_left
          max by (cluster, node) (
            label_replace(
              windows_memory_available_bytes{cluster=~"${clusterPattern}"}
              OR
              node_memory_MemAvailable_bytes{cluster=~"${clusterPattern}"}
              , "node", "$1", "instance", "([^:]+).*"
            )
          )
        )[${timeRange}:1m]) /
        sum by (cluster, node) (
          max by (cluster, node) (kube_node_status_capacity{cluster=~"${clusterPattern}", resource="memory"})
        )`,
    };
};
