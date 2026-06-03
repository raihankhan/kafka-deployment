# 05 — Multi-region active/active stretch cluster with MirrorMaker 2

**Status:** `planned`
**Effort:** 5–7 days
**Impact:** Highest — the most-Googled, least-documented thing in
the Kafka ecosystem. Pays for the entire repo's maintenance in
leverage alone.
**Pairs with:** [04 — Observability](./04-observability-stack.md), [03 — Chaos](./03-chaos-failover-harness.md)

---

## Why

HA *within* one cluster is what this repo already proves. HA
*across regions* is the actual problem at big companies: Stripe,
Shopify, Lyft, Confluent Cloud, Uber — they all run Kafka in 2–3
regions with **MirrorMaker 2** replicating topics between them for
disaster recovery and geo-local consumers.

The current state of public knowledge is bad:

- Confluent's MM2 docs are paywalled and Apache-version-specific
- Apache Kafka's own docs assume you have a working Connect cluster
  and skip the Kubernetes layer entirely
- Every "how to run MM2" blog post is a one-off and broken within a year
- Almost nobody shows the **failure modes** (controller failover
  cross-region, network partition during rebalance, DLQ topics,
  offset translation drift) that actually matter in production

This contribution delivers a **fully-reproducible multi-region
reference that runs on a single Mac** (via multi-control-plane KIND),
plus the three runbooks (DR failover, RPO/RTO test, MM2 rebalance)
that SREs reach for during incidents.

---

## File layout

```
.
├── multiregion/
│   ├── README.md                          # the architecture overview
│   ├── kind-multicluster.yaml             # 2-control-plane KIND setup
│   ├── kind-create.sh                     # `kind create cluster --config ...`
│   ├── kubeconfig-east.sh                 # KUBECONFIG=east kubeconfig
│   ├── kubeconfig-west.sh                 # KUBECONFIG=west kubeconfig
│   ├── values-east.yaml                   # region-specific overrides
│   ├── values-west.yaml
│   ├── pod-mesh.yaml                      # tailscale / submariner / skupper
│   │                                       # for cross-cluster pod IPs
│   └── ingress/
│       ├── nodeport-east.yaml
│       └── nodeport-west.yaml
├── mirrormaker2/
│   ├── README.md                          # how the connector classes fit
│   ├── connect-east.yaml                  # K8s Deployment for Connect
│   ├── connect-west.yaml
│   ├── connectors/
│   │   ├── east-to-west.yaml              # MirrorSourceConnector
│   │   ├── west-to-east.yaml
│   │   ├── checkpoint-east.yaml           # MirrorCheckpointConnector
│   │   ├── checkpoint-west.yaml
│   │   ├── heartbeat.yaml                 # MirrorHeartbeatConnector
│   │   └── config/
│   │       ├── base.json                  # replication factor translation
│   │       ├── topic-filter.json          # include/exclude regex
│   │       └── acl-translation.json
│   ├── rest-proxy.yaml                    # K8s Service for Connect REST
│   └── tests/
│       └── mm2_smoke.go                   # bidirectional produce, assert both sides
├── runbooks/
│   ├── dr-failover.md                     # "east is down, what now?"
│   ├── rpo-rto-test.md                    # quarterly DR drill
│   └── mm2-rebalance.md                   # adding a 3rd region
└── Makefile                                # `make multi-region-up`, `make mm2-status`
```

---

## Multi-cluster KIND setup

Two KIND clusters running on the same Mac, each simulating a region:

```yaml
# kind-multicluster.yaml (key parts)
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: kind-east
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30090
        hostPort: 30090                # Kafbat on east
      - containerPort: 31092
        hostPort: 31092                # Kafka on east
---
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: kind-west
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30091
        hostPort: 30091
      - containerPort: 31093
        hostPort: 31093
```

> Pod-to-pod networking across the two clusters is provided by
> **Submariner** (or **Skupper**, or a manual `nodePort` + IP
> forwarding hack for a 1-Mac dev setup). v1 ships the manual
> approach and documents Submariner as the prod upgrade.

---

## Chart keys to override (per region)

`values-east.yaml` and `values-west.yaml` inherit from
`values-prod-dedicated.yaml` with these region-specific deltas:

| Key                                | East                              | West                              | Why                                                |
|------------------------------------|-----------------------------------|-----------------------------------|----------------------------------------------------|
| `kraft.existingSecret`             | `kafka-kraft-secret` (same!)      | `kafka-kraft-secret` (same!)      | both regions form **one** KRaft cluster            |
| `nodeSelector`                     | `topology.kubernetes.io/zone: a`  | `topology.kubernetes.io/zone: b`  | AZ pin (no-op on KIND but documented for prod)     |
| `topologySpreadConstraints[].topologyKey` | `topology.kubernetes.io/zone` | same                       | spread brokers across AZs                          |
| `externalAccess.service.type`      | `NodePort`                        | `NodePort`                        | each region exposes for the other region's MM2     |
| `externalAccess.service.ports.external` | `31092`                      | `31093`                           | unique NodePort per region                         |
| `extraEnvVars[].name=KAFKA_ADVERTISED_LISTENERS` | `EXTERNAL://<east-pod-ip>:31092` | `EXTERNAL://<west-pod-ip>:31093` | reachable across the pod-mesh |
| `controller.minId`                 | `0`                               | `3`                               | unique node IDs (don't collide across regions)     |
| `broker.minId`                     | `100`                             | `103`                             | unique node IDs                                    |
| `controller.quorumBootstrapServers` | `external-kafka-controller-0...:9093,external-kafka-controller-1...:9093,<west-controller-0>...:9093` | same | single KRaft cluster across both regions |

> ⚠️ **Cross-region KRaft is bleeding-edge.** Kafka 4.0 supports it
> (since KIP-996) but expect ~50–200 ms commit latency between
> regions. v1 documents this and ships a **per-region KRaft
> cluster + MM2** topology as the safer default, with a
> **stretch KRaft cluster** as a follow-up variant.

---

## MM2 — the recommended v1 topology

```
   East (us-east-1)                 West (us-west-2)
   ┌─────────────────┐              ┌─────────────────┐
   │  KRaft cluster  │              │  KRaft cluster  │
   │  (3 ctrl, 3 br) │              │  (3 ctrl, 3 br) │
   └────────┬────────┘              └────────┬────────┘
            │                                │
   ┌────────▼────────┐              ┌────────▼────────┐
   │  Connect (MM2)  │◄────────────►│  Connect (MM2)  │
   │  MirrorSource   │  cross-region│  MirrorSource   │
   │  Checkpoint     │   HTTPS/SSL  │  Checkpoint     │
   │  Heartbeat      │              │  Heartbeat      │
   └─────────────────┘              └─────────────────┘
```

**Each region is independently HA.** MM2 replicates topics
asynchronously; consumer groups can pin to their local region for
latency, or follow the cluster to `us-east-1` for failover.

### `connectors/east-to-west.yaml`

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: east-to-west
  namespace: kafka
  labels:
    strimzi.io/cluster: connect-east
spec:
  class: org.apache.kafka.connect.mirror.MirrorSourceConnector
  config:
    source.cluster: east
    target.cluster: west
    source.bootstrap.servers: "external-kafka.kafka-east.svc.cluster.local:9092"
    target.bootstrap.servers: "external-kafka.kafka-west.svc.cluster.local:9092"

    topics: "payments\\..*,audit\\..*"
    topics.exclude: ".*\\.internal,.*\\.tmp"
    replication.factor: 3                    # local RF on the target
    offset-syncs.topic.replication.factor: 3

    # Per-topic RF translation (saves cost cross-region)
    replication.policy.class: "org.apache.kafka.connect.mirror.DefaultReplicationPolicy"
    topic.policy.class: "org.apache.kafka.connect.mirror.IdentityReplicationPolicy"

    # DLQ for failed messages
    errors.deadletterqueue.topic.name: "mm2.east-to-west.dlq"
    errors.deadletterqueue.topic.replication.factor: 3

    # Security (paired with #01)
    security.protocol: SSL
    ssl.truststore.path: /opt/kafka-connect/truststore.jks
    ssl.truststore.password: ${file:/opt/kafka-connect/secrets.properties:truststorePassword}
```

### `connectors/heartbeat.yaml`

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: heartbeat-east
  namespace: kafka
spec:
  class: org.apache.kafka.connect.mirror.HeartbeatConnector
  config:
    source.cluster: east
    target.cluster: west
    source.bootstrap.servers: "external-kafka.kafka-east.svc.cluster.local:9092"
    target.bootstrap.servers: "external-kafka.kafka-west.svc.cluster.local:9092"
    heartbeat.interval.ms: 3000
    # If heartbeats are missing for > 10s, MM2 marks the connection down
    # and Connect emits a metric. Pair with #04 alert.
```

---

## Runbooks (3 production-grade, ship as Markdown)

### `runbooks/dr-failover.md`

```markdown
# DR failover — east region is down

## Detect
- PagerDuty fires `KafkaNoActiveController-east` (from #04)
- PagerDuty fires `MM2HeartbeatMissing-east`

## Triage (5 min)
1. `kubectl --kubeconfig=east get pods -n kafka`
2. Confirm: is this a network partition or a real outage?
   `kubectl --kubeconfig=east exec deploy/mm2-east -- \
     curl -v telnet://external-kafka.kafka-east.svc.cluster.local:9092`
3. Check the cross-region latency: `mtr <west-node-ip>`

## Failover decision tree
- **If transient** (< 5 min): wait for east to recover, MM2 will
  catch up. Consumers do not need to move.
- **If sustained** (> 15 min): execute the planned failover:
  1. Freeze producers in east (`kubectl scale deploy/east-producer --replicas=0`)
  2. Repoint west DNS (or update client `bootstrap.servers`) to west
  3. Verify west lag < 1 s
  4. Re-enable producers (against west)
  5. Open an incident, page the on-call SRE
```

### `runbooks/rpo-rto-test.md`

```markdown
# RPO / RTO test (quarterly DR drill)

## Goal
Measure and document:
- **RPO** (Recovery Point Objective): how many messages were lost
- **RTO** (Recovery Time Objective): how long to recover

## Procedure
1. Schedule a 30-min maintenance window
2. Stop producers in east (`kubectl scale deploy/east-producer --replicas=0`)
3. Record `lastProducedOffset` per topic
4. `kubectl --kubeconfig=east delete pod external-kafka-controller-0`
5. Wait 30s
6. Try to produce to east — confirm it fails
7. Re-enable producers against west
8. After MM2 catches up, assert:
   - West `highWaterMark == east lastProducedOffset` (RPO = 0)
   - End-to-end failover time < 60s (RTO)
9. Document the result in `runbooks/results/YYYY-QN.md`
```

### `runbooks/mm2-rebalance.md`

```markdown
# Adding a 3rd region to MM2

(Adding region X to an east/west pair without downtime)
...
```

---

## README changes

Insert after **HA verification (chaos engineering)**:

```markdown
## Multi-region stretch cluster (active/active with MM2)

For geo-redundancy and disaster recovery, this repo ships a
multi-cluster KIND setup that simulates two regions on a single
Mac, plus a complete MirrorMaker 2 deployment that replicates
topics bidirectionally between them.

See [todo/05-multiregion-mirrormaker2.md](./todo/05-multiregion-mirrormaker2.md)
for the design and `multiregion/` + `mirrormaker2/` for the
artifacts.

```bash
# 1. Create two KIND clusters
./multiregion/kind-create.sh

# 2. Deploy Kafka + MM2 in each
make multi-region-up

# 3. Run a cross-region smoke test
go test ./mirrormaker2/tests/...

# 4. Read the DR runbook
$EDITOR runbooks/dr-failover.md
```

The README also links to a diagram of the active/active topology
and the per-region cost table so a reader can quote it in an
architecture review.
```

---

## Acceptance criteria

- [ ] `./multiregion/kind-create.sh` brings up `kind-east` and
      `kind-west`, each with a 3/3 Kafka cluster
- [ ] `make multi-region-up` deploys MM2 in both regions and starts
      the 5 connectors (2 × MirrorSource, 2 × Checkpoint, 1 × Heartbeat)
- [ ] Producing to `payments.events` in east results in a matching
      `payments.events` in west within 2 seconds
- [ ] `mm2_smoke.go` produces 1000 messages to east, asserts all
      1000 land in west, asserts consumer-group lag is 0
- [ ] `kubectl delete pod external-kafka-controller-0 --kubeconfig=east`
      does not affect MM2 replication; west keeps receiving messages
- [ ] The 3 runbooks are merged, cross-linked, and have a "Last
      tested" date

---

## Verification commands

```bash
# One-time
./multiregion/kind-create.sh

# Deploy
make multi-region-up
make mm2-status

# Smoke
go test ./mirrormaker2/tests/...

# Drill
$EDITOR runbooks/rpo-rto-test.md
# (follow the runbook, document the result)

# Tear down
kind delete cluster --name kind-east
kind delete cluster --name kind-west
```

---

## Open questions

1. **Stretch KRaft cluster vs. per-region KRaft + MM2?** Stretch
   KRaft is a single cluster spanning regions; per-region + MM2 is
   two clusters with async replication. v1 ships per-region + MM2
   (the safer, more battle-tested choice). Discuss stretch-KRaft as
   v2.
2. **Strimzi KafkaConnector CR vs. raw REST API?** Strimzi requires
   the Strimzi operator (heavyweight). Raw REST works with any
   Connect image. v1 ships raw REST + a `Makefile` wrapper. v2
   could add Strimzi for teams that already have it.
3. **Submariner vs. Skupper vs. manual port-forward for cross-
   cluster pod networking?** Submariner is the most production
   proven but needs a CNI that supports it. v1 ships the manual
   port-forward approach (works on a single Mac); v2 documents
   Submariner for multi-host setups.
