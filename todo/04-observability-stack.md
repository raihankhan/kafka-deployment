# 04 — Observability stack: Prometheus + Grafana + JMX + lag exporter

**Status:** `planned`
**Effort:** 3–4 days
**Impact:** High — every SRE on-call needs these dashboards, and
publicly-available Kafka dashboards are 2–3 years out of date for KRaft.
**Pairs with:** [01 — TLS](./01-cert-manager-tls.md), [03 — Chaos](./03-chaos-failover-harness.md)

---

## Why

The repo currently goes from "deployed" to "black box" in one step.
For a 3-controller, 3-broker cluster in production you need to
answer, **at a glance**:

- Is the controller quorum healthy? (`active_controller_count == 3`)
- Are any partitions under-replicated? (`kafka_server_replica_manager_under_replicated_partitions > 0`)
- What's the consumer lag per group? (alerts on this)
- Is the broker about to fill its disk? (`predict_linear(...4h) > 0.9`)
- Is the JVM healthy? (heap, GC pause p99)

This contribution ships **5 production dashboards + 5 alert rules**
that are KRaft-aware, TLS-aware (per #01), and ready to drop into
any team that's already running kube-prometheus-stack.

---

## File layout

```
.
├── values-prod-observability.yaml         # JMX exporter re-enabled via sidecar
├── observability/
│   ├── README.md                          # how to deploy + the dashboard index
│   ├── kustomization.yaml                 # bundles everything
│   ├── exporters/
│   │   ├── jmx-exporter-config.yaml       # Bitnami's published config map
│   │   └── kafka-lag-exporter.yaml        # Deployment + Service for seglo/lag-exporter
│   ├── prometheus/
│   │   ├── servicemonitor-kafka.yaml      # scrapes brokers on JMX port 5556
│   │   ├── servicemonitor-lag-exporter.yaml
│   │   └── rules/
│   │       ├── kafka-broker.yaml
│   │       ├── kafka-controller.yaml
│   │       ├── kafka-consumer-lag.yaml
│   │       └── kafka-resources.yaml
│   └── grafana/
│       ├── datasource.yaml                # auto-register Prometheus
│       └── dashboards/
│           ├── broker-health.json
│           ├── throughput.json
│           ├── consumer-lag.json
│           ├── controller-quorum.json
│           └── resource-saturation.json
└── scripts/
    └── import-dashboards.sh               # one-liner to push dashboards to an existing Grafana
```

---

## Chart keys to override

We **re-enable JMX** that was disabled in `values.yaml` (the
`bitnami/jmx-exporter` image was unavailable, so we ship our own
exporter sidecar).

| Key                                | Value                          | Why                                          |
|------------------------------------|--------------------------------|----------------------------------------------|
| `metrics.jmx.enabled`              | `true`                         | re-enable the sidecar that runs jmx-exporter |
| `metrics.jmx.kafkaJmxPort`        | `5555`                         | exporter scrapes this in-container           |
| `metrics.jmx.containerPorts.metrics` | `5556`                       | exposed on the pod for Prometheus            |
| `metrics.jmx.image.repository`     | `bitnamilegacy/jmx-exporter`   | bitnami/ is dead                            |
| `metrics.jmx.image.tag`            | `1.4.0-debian-12-r0`           | matches the chart's default                  |
| `extraEnvVars[].name=KAFKA_JMX_ENABLED` | `"true"`                  | also tell the broker to expose JMX on 9999   |
| `extraEnvVars[].name=KAFKA_JMX_PORT`    | `"9999"`                  |                                              |

> The `lag exporter` is **not** part of the Bitnami chart; we ship
> it as a separate `Deployment` that uses the AdminClient to walk
> consumer groups.

---

## Prometheus scrape config (via kube-prometheus-stack `additionalScrapeConfigs`)

```yaml
- job_name: kafka-jmx
  kubernetes_sd_configs:
    - role: pod
  relabel_configs:
    - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
      action: keep
      regex: (controller-eligible|broker)
    - source_labels: [__meta_kubernetes_pod_container_port_name]
      action: keep
      regex: jmx
- job_name: kafka-lag-exporter
  kubernetes_sd_configs:
    - role: service
  relabel_configs:
    - source_labels: [__meta_kubernetes_service_name]
      action: keep
      regex: kafka-lag-exporter
```

---

## Alert rules (5 high-signal, low-noise)

### `prometheus/rules/kafka-broker.yaml`

```yaml
groups:
- name: kafka-broker
  rules:
  - alert: KafkaBrokerDown
    expr: kafka_server_broker_state != 3      # 3 == Active
    for: 2m
    labels: { severity: critical }
    annotations:
      summary: "Kafka broker {{ $labels.pod }} is not Active"
      runbook: "kubectl logs -n kafka {{ $labels.pod }} | grep -i 'transition'"
  - alert: KafkaUnderReplicatedPartitions
    expr: sum by (topic) (kafka_server_replica_manager_under_replicated_partitions) > 0
    for: 5m
    labels: { severity: warning }
    annotations:
      summary: "Topic {{ $labels.topic }} has under-replicated partitions"
```

### `prometheus/rules/kafka-controller.yaml`

```yaml
- alert: KafkaNoActiveController
  expr: sum(kafka_controller_active_count) == 0
  for: 30s
  labels: { severity: critical }
  annotations:
      summary: "No active KRaft controller — cluster cannot elect a leader"
- alert: KafkaControllerQuorumLost
  expr: sum(kafka_controller_active_count) < 2
  for: 1m
  labels: { severity: critical }
```

### `prometheus/rules/kafka-consumer-lag.yaml`

```yaml
- alert: KafkaConsumerLagHigh
  expr: kafka_consumergroup_lag > 10000
  for: 10m
  labels: { severity: warning }
  annotations:
      summary: "Consumer group {{ $labels.group }} lagging on {{ $labels.topic }} ({{ $value }} msgs)"
```

### `prometheus/rules/kafka-resources.yaml`

```yaml
- alert: KafkaBrokerDiskWillFillIn4h
  expr: predict_linear(kafka_log_log_size[1h], 4*3600) > 0.9
  for: 10m
  labels: { severity: warning }
- alert: KafkaJVMHeapHigh
  expr: jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"} > 0.85
  for: 5m
  labels: { severity: warning }
```

---

## Dashboards (5, KRaft-aware)

### 1. **Broker health**

| Panel | PromQL |
|---|---|
| Active controllers (gauge) | `sum(kafka_controller_active_count)` |
| Under-replicated partitions (gauge, per topic) | `kafka_server_replica_manager_under_replicated_partitions` |
| Offline partitions (gauge) | `kafka_controller_offline_partitions_count` |
| Leader election rate (graph) | `rate(kafka_controller_controller_change_rate[5m])` |
| ISR shrink events (table) | last 10 events where `kafka_server_replica_manager_isr_shrinks_total` increased |

### 2. **Throughput**

| Panel | PromQL |
|---|---|
| BytesIn / sec (per broker) | `rate(kafka_server_broker_topic_metrics_bytes_in_total[1m])` |
| BytesOut / sec (per broker) | `rate(kafka_server_broker_topic_metrics_bytes_out_total[1m])` |
| MessagesIn / sec (per topic) | `rate(kafka_server_broker_topic_metrics_messages_in_total[1m])` |
| Produce / fetch latency p99 | histogram_quantile(0.99, `kafka_network_request_metrics_total{...}`) |

### 3. **Consumer lag**

| Panel | PromQL |
|---|---|
| Lag per (group × topic) (table, sortable) | `kafka_consumergroup_lag` |
| Total lag (gauge) | `sum(kafka_consumergroup_lag)` |
| Lag over time (graph) | `sum by (group) (kafka_consumergroup_lag)` |

### 4. **Controller quorum** (KRaft-specific)

| Panel | PromQL |
|---|---|
| Active controllers (gauge) | `sum(kafka_controller_active_count)` |
| Election count (counter) | `kafka_controller_election_count` |
| Commit latency p99 | `histogram_quantile(0.99, kafka_kraft_apis_request_latency_ms)` |
| Follower fetch lag (graph) | `kafka_kraft_replica_manager_fetch_lag` |

### 5. **Resource saturation**

| Panel | PromQL |
|---|---|
| JVM heap used / max | `jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"}` |
| GC pause p99 | `histogram_quantile(0.99, rate(jvm_gc_pause_seconds_bucket[5m]))` |
| Disk used % per broker | `kafka_log_log_size / 8Gi` |
| CPU throttling (cAdvisor) | `rate(container_cpu_cfs_throttled_seconds_total[5m])` |

---

## README changes

Insert after **Tiered storage**:

```markdown
## Observability (Prometheus + Grafana)

This repo ships a turnkey observability bundle: 5 production
dashboards, 5 high-signal alert rules, and the JMX + consumer-lag
exporters needed to populate them.

See [todo/04-observability-stack.md](./todo/04-observability-stack.md)
for the design and `observability/` for the manifests.

```bash
# 1. Make sure kube-prometheus-stack is installed
helm install kps prometheus-community/kube-prometheus-stack -n monitoring --create-namespace

# 2. Apply the Kafka observability bundle
kubectl apply -k observability/

# 3. Open Grafana (port-forward)
kubectl port-forward -n monitoring svc/kps-grafana 3000:80
# Open http://localhost:3000 (default creds admin / prom-operator)
```

The README embeds screenshots of each dashboard as PNG and links to
the alert rule sources so on-call engineers can `kubectl describe
prometheusrule` them during incidents.
```

---

## Acceptance criteria

- [ ] `helm install … -f values-prod-observability.yaml` brings the
      cluster to 3/3 Ready with JMX exposed on `pod:5556`
- [ ] `kubectl apply -k observability/` creates 5 alert rules, 2
      ServiceMonitors, 1 Deployment (lag-exporter), 5 ConfigMaps
      (dashboards)
- [ ] Prometheus targets page shows `kafka-jmx` and `kafka-lag-exporter`
      as `UP`
- [ ] All 5 dashboards render data in Grafana
- [ ] Triggering `KafkaBrokerDown` by stopping a pod fires the alert
      within 2 minutes
- [ ] README's Observability section is merged

---

## Verification commands

```bash
# Install cluster
helm install obs-kafka oci://.../bitnamicharts/kafka \
  -n kafka -f values-prod-observability.yaml --wait --timeout 15m

# Install monitoring
helm install kps prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace

# Apply bundle
kubectl apply -k observability/

# Verify
kubectl get prometheusrule -n monitoring
kubectl get servicemonitor -n monitoring
kubectl port-forward -n monitoring svc/kps-grafana 3000:80

# Tear down
kubectl delete -k observability/
helm uninstall obs-kafka -n kafka
helm uninstall kps -n monitoring
```

---

## Open questions

1. **Bundle kube-prometheus-stack, or assume it's already
   installed?** v1 assumes installed (the typical platform-team
   reality). v2 could add a `make install-monitoring` target.
2. **Which lag exporter — `seglo/kafka-lag-exporter`,
   `lightbend/kafka-lag-exporter`, or in-house?** Both are
   maintained; `seglo` has more stars and a Grafana datasource
   plugin. **Recommendation: seglo.** Confirm in PR.
3. **Ship dashboard JSONs as ConfigMaps (Grafana sidecar-provisioned)
   or push them via API?** Sidecar provisioning is simpler and
   version-controlled. **Recommendation: sidecar.**
