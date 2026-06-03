# 03 — Chaos & failover test harness

**Status:** `planned`
**Effort:** 4–5 days
**Impact:** Very high — single biggest differentiator vs. every other
public Kafka-on-K8s tutorial.
**Pairs with:** [01 — TLS](./01-cert-manager-tls.md), [04 — Observability](./04-observability-stack.md)

---

## Why

The repo currently proves the cluster *starts* and *delivers a
message*. Production teams need to know it also *survives*:

- a controller pod OOMKilled mid-raft-election
- a broker drained for a Kubernetes upgrade
- a node running out of disk
- a network policy misconfiguration blackholing inter-broker traffic
- a slow GC pause stalling a leader
- the headless service's DNS cache going stale

The "will the cluster survive this?" question needs a **rehearsed
answer**, not a hand-wave. This contribution ships a declarative
chaos-experiment library + Go-based assertion harness that runs
each scenario, asserts the cluster stayed healthy, and produces a
report. It's the kind of artifact that gets quoted in architecture
reviews and incident postmortems.

Modelled on what Netflix, Gremlin, and the major incident postmortems
(Cloudflare, Slack) point to as the reason they recover quickly.

---

## File layout

```
.
├── chaos/
│   ├── README.md                          # how the suite fits together
│   ├── namespace.yaml                     # dedicated `chaos-mesh` namespace
│   ├── rbac.yaml                          # ServiceAccount for the orchestrator
│   ├── experiments/
│   │   ├── pod-kill-controller.yaml       # PodChaos: kill a controller
│   │   ├── pod-kill-broker.yaml           # PodChaos: kill a broker
│   │   ├── network-partition-controller.yaml
│   │   ├── disk-pressure-broker.yaml
│   │   ├── dns-chaos-headless.yaml
│   │   ├── time-skew.yaml                 # TimeChaos: skew node clock 30s
│   │   └── stress-cpu.yaml                # StressChaos: pin broker CPU
│   ├── schedule.yaml                      # weekly cron of the full suite
│   └── reports/                           # gitignored, populated by CI
├── tests/
│   ├── havernd/                           # the main assertion harness
│   │   ├── go.mod
│   │   ├── main.go                        # orchestrates: produce/consume + run chaos
│   │   ├── producer.go                    # continuous N-producer workload
│   │   ├── consumer.go                    # continuous consumer with offset commits
│   │   ├── assert.go                      # loss/latency/ISR assertions
│   │   └── report.go                      # writes chaos-report.md
│   └── README.md
├── Makefile                               # `make chaos-all`, `make report`
└── .github/workflows/chaos.yml            # CI: nightly run, uploads report
```

---

## Experiments (chaos-mesh manifests)

Each manifest targets a release name (`external-kafka`, `tls-kafka`,
`dedicated-kafka` …) and is parameterised by the release name +
namespace so the same manifest works for every variant of the cluster.

### `experiments/pod-kill-controller.yaml`

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: kill-controller-1
  namespace: chaos-mesh
spec:
  action: pod-kill
  mode: one
  selector:
    namespaces: [kafka]
    labelSelectors:
      app.kubernetes.io/instance: external-kafka
      app.kubernetes.io/component: controller-eligible
      statefulset.kubernetes.io/pod-name: external-kafka-controller-1
  duration: "60s"             # pod is gone for 60s, then rescheduled
  scheduler: { cron: "@every 5m" }
```

### `experiments/disk-pressure-broker.yaml`

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: disk-pressure-broker
spec:
  mode: one
  selector:
    namespaces: [kafka]
    labelSelectors:
      app.kubernetes.io/component: broker
  stressors:
    disk:
      path: /bitnami/kafka/data
      size: "8Gi"             # push the broker's PVC near full
  duration: "5m"
```

### `experiments/network-partition-controller.yaml`

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: partition-controller-0
spec:
  action: partition
  mode: one
  selector:
    namespaces: [kafka]
    labelSelectors:
      statefulset.kubernetes.io/pod-name: external-kafka-controller-0
  direction: both
  target:
    selector:
      namespaces: [kafka]
      labelSelectors:
        app.kubernetes.io/component: controller-eligible
  duration: "30s"
```

(plus 4 more: `pod-kill-broker`, `dns-chaos-headless`, `time-skew`,
`stress-cpu`)

---

## `tests/havernd/` — Go assertion harness

**Design goals**

1. **Black-box** — talks to Kafka like any client, no special privileges
2. **Continuous workload** — produces + consumes for the full duration
   of the chaos experiment
3. **Hard assertions** — fails the test if any of:
   - A produced message is **not** consumed within 30 s
   - End-to-end latency p99 > 500 ms
   - `min.insync.replicas` ever drops below 2
   - Topic `Isr` count drops below `replicationFactor` for > 10 s
4. **Per-experiment isolation** — each experiment is a separate Go
   subtest so one failure doesn't taint the rest
5. **Machine + human readable output** — Markdown report + JUnit XML
   for CI

**Core flow** (`main.go`)

```
for each experiment in chaos/experiments/*.yaml:
  1. apply the manifest
  2. wait for it to be in "Running" phase
  3. start producer(N=20) and consumer(group=havernd)
  4. record startTime, lastProducedOffset
  5. wait for experiment duration + grace period
  6. stop producer/consumer
  7. assert:
       - consumed >= produced
       - no message older than 30s undelivered
       - p99 latency < 500ms
       - ISR never dropped (poll /api/clusters/.../topics every 5s)
  8. delete the manifest
  9. write per-experiment result to chaos-report.md
```

**Key snippet** — `assert.go` (pseudocode)

```go
func (a *Asserter) CheckISR(topic string) {
    for {
        body := kafbatGet("/api/clusters/" + cluster + "/topics/" + topic)
        for _, p := range body.Partitions {
            if len(p.InSyncReplicas) < a.MinISR {
                a.Fail("ISR dropped", topic, p)
            }
        }
        time.Sleep(5 * time.Second)
    }
}
```

**Why use the Kafbat REST API for ISR checks?** Because it's a
production-grade, auth-aware read path that we **already** trust to
be deployed (per [04 — Observability](./04-observability-stack.md)).
Reusing it means the chaos harness is stateless w.r.t. Kafka — it
needs only HTTP and a Kafka client lib.

---

## README changes

Insert after **Observability**:

```markdown
## HA verification (chaos engineering)

This repo ships a declarative chaos-experiment library and a Go
assertion harness that together prove the cluster survives the
failure modes production actually sees.

See [todo/03-chaos-failover-harness.md](./todo/03-chaos-failover-harness.md)
for the design and `chaos/` for the experiments.

```bash
# Install chaos-mesh once
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace

# Run the full suite against the running cluster
make chaos-all

# Read the report
cat chaos/reports/chaos-report.md
```

The README also links to a green/red CI badge for the latest nightly
run, so a reader can see at a glance that the cluster survives every
experiment.
```

---

## Acceptance criteria

- [ ] `chaos-mesh` installed in `chaos-mesh` namespace, all 7
      experiments are valid manifests
- [ ] `make chaos-all` runs the full suite against
      `external-kafka` and exits 0
- [ ] `chaos/reports/chaos-report.md` is generated with one row per
      experiment and a green/red status
- [ ] After `pod-kill-controller-1`, the cluster has 3/3 Ready within
      90 s and no messages are lost
- [ ] After `disk-pressure-broker`, the broker rejects new produces
      (asserted via Kafbat's metrics) but consumers keep reading; once
      pressure is removed, the broker recovers within 60 s
- [ ] After `network-partition-controller-0`, the controller quorum
      continues to function (KRaft tolerates 1 of 3 partition) and
      no data is lost
- [ ] CI workflow runs the suite nightly and posts the report as a
      PR comment

---

## Verification commands

```bash
# One-time
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace

# Run
make chaos-all

# Read
cat chaos/reports/chaos-report.md

# Tear down
helm uninstall chaos-mesh -n chaos-mesh
kubectl delete namespace chaos-mesh
```

---

## Open questions

1. **chaos-mesh vs. LitmusChaos vs. Gremlin?** chaos-mesh is the
   most active OSS project, has the most experiment types, and
   installs cleanly on KIND. Recommendation: **chaos-mesh**. Discuss
   in PR.
2. **Run experiments in serial or parallel?** Serial is safer for v1
   (no compound failures). v2 could enable parallel for CI
   performance.
3. **Store reports in git (gitignored) or as CI artifacts?** CI
   artifacts are better for traceability; a small `gh-actions-uploader`
   snippet is enough.
