# kafka-deployment

A production-shaped **Apache Kafka 4.0** cluster deployed in **KRaft mode**
(without ZooKeeper) on a local **KIND** (Kubernetes IN Docker) cluster on
macOS, using the **Bitnami Kafka Helm chart**.

> KRaft is now the default. Kafka 4.0 completely drops ZooKeeper support,
> so every modern deployment must use the Raft-based controller quorum.

---

## Deployment modes

The repo ships **two** Kafka values files so you can pick the right
KRaft topology for the job, plus a third values file for the Kafbat
web UI that you can point at either cluster:

| Artifact          | Values file                    | Pods / role                                | When to use                                              |
|-------------------|--------------------------------|--------------------------------------------|----------------------------------------------------------|
| Kafka — Combined  | `values.yaml`                  | 3 pods, each running **broker + controller** roles | Local KIND work, small dev clusters, edge                |
| Kafka — Dedicated | `values-prod-dedicated.yaml`   | 3 broker-only pods + 3 controller-only pods | Production clusters — size, scale, and fail over each tier independently |
| Kafbat UI         | `kafbat-values.yaml`           | 1 UI pod connected to `external-kafka:9092` | Visualize / browse / produce-consume from a browser      |

Both Kafka modes run the same KRaft cluster (same `kraft-cluster-id`
Secret), both use the same `bitnamilegacy/kafka:4.0.0` image, and
both can be installed side by side in the same namespace under
different release names.

---

## Topology (combined mode, the default)

The combined cluster is what `values.yaml` deploys: every pod is both a
*broker* and a *KRaft controller*, which keeps the resource footprint
small and is perfectly adequate for a 3-node local cluster.

| Component            | Count | Role                                |
|----------------------|-------|-------------------------------------|
| Brokers              | 3     | Producer/consumer + inter-broker    |
| KRaft controllers    | 3     | Internal Raft quorum (one per pod)  |
| ZooKeeper            | 0     | Hard-disabled                       |
| Replication factor   | 3     | Default for new topics              |
| `min.insync.replicas`| 2     | Durability floor                    |

| Listener       | Port | Protocol  | Purpose                         |
|----------------|------|-----------|---------------------------------|
| `client`       | 9092 | PLAINTEXT | Producers / consumers in-cluster|
| `controller`   | 9093 | PLAINTEXT | KRaft controller quorum         |
| `interbroker`  | 9094 | PLAINTEXT | Replication between brokers     |

> All listeners are **PLAINTEXT** for local development. See the
> "Production hardening" section below for TLS/SASL.

---

## Prerequisites

- macOS with **Docker Desktop** (or any other Docker host)
- [KIND](https://kind.sigs.k8s.io/) with a running cluster
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3.8+](https://helm.sh) (OCI registry support)
- A default `StorageClass` (KIND's `local-path` provisioner is fine)

Verify the cluster is reachable:

```bash
kubectl cluster-info --context kind
kubectl get nodes
kubectl get storageclass      # you should see `standard` (local-path)
```

---

## Files in this repo

| File                          | Purpose                                                                                |
|-------------------------------|----------------------------------------------------------------------------------------|
| `values.yaml`                 | Helm values for **combined mode** on KIND — 3 broker+controller pods, modest resources |
| `values-prod-dedicated.yaml`  | Helm values for **dedicated mode** — 3 brokers + 3 dedicated controllers, production-sized resources and PVCs |
| `kafbat-values.yaml`          | Helm values for **Kafbat UI** wired to the `external-kafka` Service (PLAINTEXT)        |
| `README.md`                   | This document.                                                                         |

---

## Deploy

### 1. Generate a stable KRaft cluster-id

A **static** cluster id is important: if you only rely on the auto-generated
id and your pods restart, the brokers will see a different id and refuse to
form a quorum.

```bash
export CLUSTER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
echo "Cluster ID: $CLUSTER_ID"

kubectl create namespace kafka
kubectl create secret generic kafka-kraft-secret \
  --from-literal=kraft-cluster-id=$CLUSTER_ID \
  -n kafka
```

### 2. Install the Bitnami chart (OCI)

The Bitnami chart is published as an OCI artifact, so no `helm repo add`
is needed.

```bash
helm install external-kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  --namespace kafka \
  -f values.yaml \
  --wait \
  --timeout 15m
```

> **Image note.** Bitnami moved its images from `docker.io/bitnami/*` to
> `docker.io/bitnamilegacy/*`. The included `values.yaml` overrides every
> image the chart references (`kafka`, `kubectl`, `os-shell`, and JMX
> exporter) so the install works on Docker Hub today. You will see a
> "Substituted images" warning from the chart — it is expected and harmless.

### 3. Verify

```bash
# All 3 broker pods should be 1/1 Running
kubectl get pods -n kafka -w

# Statefulset, services, and PVCs
kubectl get svc,statefulset,pvc -n kafka
```

Expected output (abbreviated):

```
NAME                          READY   STATUS    RESTARTS   AGE
external-kafka-controller-0   1/1     Running   0          1m
external-kafka-controller-1   1/1     Running   0          1m
external-kafka-controller-2   1/1     Running   0          1m

NAME                  TYPE        CLUSTER-IP   PORT(S)
external-kafka        ClusterIP   10.96.x.x    9092/TCP
external-kafka-controller-headless   ClusterIP   None   9094/TCP,9092/TCP,9093/TCP
```

Confirm KRaft (not Zookeeper) by looking at broker logs:

```bash
kubectl logs external-kafka-controller-0 -n kafka | grep -iE "kraft|raft"
# KafkaRaftServer nodeId=0 Kafka Server started
# broker-0-to-controller-forwarding-channel-manager: Recorded new KRaft controller
```

---

## Smoke test

Spin up a one-shot client pod and round-trip a few messages:

```bash
# Launch a Kafka client (uses the same bitnamilegacy image)
kubectl run external-kafka-client \
  --restart='Never' \
  --image docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10 \
  --namespace kafka \
  --command -- sleep infinity

kubectl wait --for=condition=Ready pod/external-kafka-client -n kafka

# Create a topic with 3 partitions, RF=3
kubectl exec -n kafka external-kafka-client -- kafka-topics.sh \
  --bootstrap-server external-kafka:9092 \
  --create --topic demo --partitions 3 --replication-factor 3

# Produce
kubectl exec -n kafka external-kafka-client -- bash -c '
  echo -e "hello-kraft\nfrom-kind\nline-three" | \
  kafka-console-producer.sh --bootstrap-server external-kafka:9092 --topic demo'

# Consume
kubectl exec -n kafka external-kafka-client -- kafka-console-consumer.sh \
  --bootstrap-server external-kafka:9092 \
  --topic demo \
  --from-beginning \
  --timeout-ms 10000
```

You should see the three lines echoed back, and the topic should report
`Replicas: 0,1,2 Isr: 0,1,2` for every partition (full in-sync replication).

```bash
# Inspect replication
kubectl exec -n kafka external-kafka-client -- kafka-topics.sh \
  --bootstrap-server external-kafka:9092 --describe --topic demo
```

---

## Deploying in dedicated mode

Dedicated mode splits the cluster into two StatefulSets that can be
sized, scaled, and failed over independently:

- **3 controllers** — KRaft quorum, no broker role, no data
- **3 brokers** — data plane, no controller role, no quorum vote

The KRaft cluster id, image overrides, and listener layout are
identical to combined mode; only the topology changes.

```bash
# Install on a different release name so it can co-exist with the
# combined-mode `external-kafka` release from `values.yaml`.
helm install dedicated-kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  --namespace kafka \
  -f values-prod-dedicated.yaml \
  --wait \
  --timeout 15m
```

Verify the two-tier topology:

```bash
kubectl get pods,statefulset,svc -n kafka -l app.kubernetes.io/instance=dedicated-kafka
```

Expected:

```
NAME                               READY   STATUS    RESTARTS   AGE
dedicated-kafka-broker-0           1/1     Running   0          1m
dedicated-kafka-broker-1           1/1     Running   0          1m
dedicated-kafka-broker-2           1/1     Running   0          1m
dedicated-kafka-controller-0       1/1     Running   0          1m
dedicated-kafka-controller-1       1/1     Running   0          1m
dedicated-kafka-controller-2       1/1     Running   0          1m

NAME                                          READY   AGE
statefulset.apps/dedicated-kafka-broker       3/3     1m
statefulset.apps/dedicated-kafka-controller   3/3     1m

NAME                                          TYPE        PORT(S)
service/dedicated-kafka                       ClusterIP   9092/TCP
service/dedicated-kafka-broker-headless       ClusterIP   None   9094/TCP,9092/TCP
service/dedicated-kafka-controller-headless   ClusterIP   None   9093/TCP
```

Confirm the role split on each tier:

```bash
# Controller pod: ControllerServer, no broker role
kubectl logs dedicated-kafka-controller-0 -n kafka | grep -iE "KafkaRaftServer|process.roles|node.id"
# KafkaRaftServer nodeId=0 Kafka Server started

# Broker pod: process.roles = [broker], node.id = 100+
kubectl logs dedicated-kafka-broker-0 -n kafka | grep -iE "process.roles|node.id|KafkaRaftServer"
# node.id = 100
# process.roles = [broker]
# KafkaRaftServer nodeId=100 Kafka Server started
```

A topic created on the dedicated cluster has all replicas on broker
node IDs only:

```bash
kubectl run dedicated-kafka-client \
  --restart='Never' \
  --image docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10 \
  --namespace kafka --command -- sleep infinity

kubectl wait --for=condition=Ready pod/dedicated-kafka-client -n kafka

kubectl exec -n kafka dedicated-kafka-client -- kafka-topics.sh \
  --bootstrap-server dedicated-kafka:9092 \
  --create --topic dedi-demo --partitions 3 --replication-factor 3

kubectl exec -n kafka dedicated-kafka-client -- kafka-topics.sh \
  --bootstrap-server dedicated-kafka:9092 --describe --topic dedi-demo
# Partition: 0   Leader: 100  Replicas: 100,101,102  Isr: 100,101,102
# Partition: 1   Leader: 101  Replicas: 101,102,100  Isr: 101,102,100
# Partition: 2   Leader: 102  Replicas: 102,100,101  Isr: 102,100,101
```

Tear down the dedicated release (leaves the combined-mode one alone):

```bash
helm uninstall dedicated-kafka -n kafka
kubectl delete pvc -n kafka -l app.kubernetes.io/instance=dedicated-kafka
```

---

## Kafbat UI

[Kafbat](https://ui.docs.kafbat.io/) is a modern web UI for Apache
Kafka. `kafbat-values.yaml` ships a pre-wired config that points it at
the `external-kafka` Service deployed by `values.yaml`.

### Install

```bash
helm repo add kafbat-ui https://kafbat.github.io/helm-charts
helm repo update

helm install kafbat-ui kafbat-ui/kafka-ui \
  --namespace kafka \
  -f kafbat-values.yaml \
  --wait \
  --timeout 5m
```

Verify the pod and service came up:

```bash
kubectl get pods,svc -n kafka -l app.kubernetes.io/name=kafka-ui
# NAME                                      READY   STATUS    RESTARTS   AGE
# pod/kafbat-ui-kafka-ui-...                1/1     Running   0          30s
# NAME                         TYPE       PORT(S)
# service/kafbat-ui-kafka-ui   NodePort   8080:30080/TCP
```

### Open the UI

Two ways to reach the UI on macOS — pick whichever fits your KIND
setup:

**Option A — `kubectl port-forward`** (always works, no KIND port
mapping needed):

```bash
kubectl port-forward -n kafka svc/kafbat-ui-kafka-ui 8080:8080
# Open http://localhost:8080
```

**Option B — KIND NodePort `30080`** (only works if KIND was created
with a port mapping that covers `30080`):

```bash
# The KIND node's reachable IP from the host
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
echo "Open http://$NODE_IP:30080"
```

> If you created KIND with `extraPortMappings` covering
> `30000-30100` (the common `30080` case), Option B works directly.
> Otherwise stick with Option A — port-forward needs no setup.

### Verify it's wired to Kafka

The Kafbat pod logs should show a healthy connection to the cluster
and periodic metrics scrapes:

```bash
kubectl logs -n kafka -l app.kubernetes.io/name=kafka-ui --tail=200 \
  | grep -iE "bootstrap.servers|metrics|started"
# bootstrap.servers = [external-kafka:9092]
# Started KafkaUiApplication ...
# Metrics updated for cluster: external-kafka
```

You can also hit Kafbat's REST API directly through the port-forward:

```bash
kubectl port-forward -n kafka svc/kafbat-ui-kafka-ui 18080:8080 &

# Cluster stats — note "activeControllers" > 0 and "zooKeeperStatus: null"
curl -s http://localhost:18080/api/clusters/external-kafka/stats | python3 -m json.tool

# Topic list
curl -s http://localhost:18080/api/clusters/external-kafka/topics | python3 -m json.tool

pkill -f "port-forward.*kafbat-ui-kafka-ui"
```

### Connecting Kafbat to the dedicated cluster too

`kafbat-values.yaml` only knows about the combined-mode `external-kafka`
cluster. If you also have `dedicated-kafka` running (from
`values-prod-dedicated.yaml`), point Kafbat at it as well — either by
adding to the same `yamlApplicationConfig.kafka.clusters` list, or by
patching the running release:

```bash
# Upgrade with an extra cluster entry
helm upgrade kafbat-ui kafbat-ui/kafka-ui \
  --namespace kafka \
  --reuse-values \
  --set 'yamlApplicationConfig.kafka.clusters[1].name=dedicated-kafka' \
  --set 'yamlApplicationConfig.kafka.clusters[1].bootstrapServers=dedicated-kafka:9092' \
  --set 'yamlApplicationConfig.kafka.clusters[1].securityProtocol=PLAINTEXT'
```

The cluster picker in the UI's top-right will then offer both
`external-kafka` and `dedicated-kafka`.

### Tear down

```bash
helm uninstall kafbat-ui -n kafka
```

Kafka is untouched.

---

## Tear down

Uninstall Helm releases first (in any order), then drop the namespace
and its PVCs:

```bash
helm uninstall kafbat-ui -n kafka          # if you installed Kafbat
helm uninstall dedicated-kafka -n kafka    # if you installed the dedicated-mode release
helm uninstall external-kafka -n kafka
kubectl delete namespace kafka
```

The `kubectl delete namespace` will also drop the PVCs (kind's
`local-path` StorageClass uses `Delete` reclaim policy).

---

## How `values.yaml` differs from the chart defaults

The defaults below apply to `values.yaml` (combined mode). For
`values-prod-dedicated.yaml`, the rows that change are called out in
the **Dedicated** column.

| Key                                          | Default                          | Combined (`values.yaml`)                 | Dedicated (`values-prod-dedicated.yaml`)                |
|----------------------------------------------|----------------------------------|------------------------------------------|---------------------------------------------------------|
| `kraft.enabled`                              | `true`                           | `true`                                   | `true`                                                  |
| `kraft.existingSecret`                       | unset                            | `kafka-kraft-secret`                     | `kafka-kraft-secret`                                    |
| `zookeeper.enabled`                          | `false`                          | `false` (explicit)                       | `false` (explicit)                                      |
| `controller.controllerOnly`                  | `false`                          | `false`                                  | **`true`** (controllers don't serve as brokers)         |
| `controller.replicaCount`                    | `3`                              | `3`                                      | `3`                                                     |
| `broker.replicaCount`                        | `0`                              | `0` (unused)                             | **`3`** (broker-only StatefulSet created)               |
| `replicaCount` (top-level)                   | `3`                              | `3`                                      | `3`                                                     |
| `controller.minId` / `broker.minId`          | `0` / `100`                      | `0` / `100`                              | `0` / `100`                                             |
| `listeners.client/controller/interbroker`    | `PLAINTEXT`                      | `PLAINTEXT` (loopback in-cluster)        | `PLAINTEXT` (loopback in-cluster)                       |
| `controller.persistence.size`                | `8Gi`                            | `5Gi`                                    | `5Gi` (controllers don't carry data)                    |
| `broker.persistence.size`                    | `8Gi`                            | n/a (combined uses `controller.*`)       | **`50Gi`** (brokers carry the data)                     |
| `controller.resources`                      | `resourcesPreset: nano`          | modest 512Mi–1Gi                         | modest 512Mi–1Gi                                        |
| `broker.resources`                           | `resourcesPreset: small`         | n/a                                      | **1Gi–3Gi** (heavier heap for data plane)                |
| `controller.heapOpts`                        | 75 % of RAM                      | `-Xmx768m -Xms512m`                      | `-Xmx768m -Xms512m`                                     |
| `broker.heapOpts`                            | 75 % of RAM                      | n/a                                      | **`-Xmx2g -Xms2g`**                                     |
| `image.repository`                           | `bitnami/kafka`                  | `bitnamilegacy/kafka`                    | `bitnamilegacy/kafka`                                   |
| `defaultInitContainers.volumePermissions`    | enabled                          | **disabled** (local-path is fine)        | **disabled**                                            |
| `defaultInitContainers.autoDiscovery`       | enabled                          | **disabled** (we set cluster.id via Secret) | **disabled**                                          |
| `metrics.jmx.enabled`                        | `false`                          | `false` (explicit)                       | `false` (explicit)                                      |
| `podAntiAffinityPreset`                      | `soft`                           | `soft`                                   | `soft`                                                  |

---

## Production hardening (when you leave KIND)

- **Switch to dedicated mode** — use `values-prod-dedicated.yaml`
  (see *Deployment modes* above). Combined mode is fine up to a few
  hundred brokers, but for real production you typically want the
  controller quorum and the broker fleet managed independently.
- **TLS** — switch the listener `protocol` to `TLS` or `SASL_SSL`, mount a
  cert Secret, and set `listeners.tls.existingSecret` /
  `listeners.ssl.existingSecret`.
- **Storage** — replace `local-path` with `gp3`, `pd-ssd`, or a network
  provisioner and bump `broker.persistence.size` to 100Gi+ (in dedicated
  mode the controller PVC can stay small).
- **Resources** — replace the modest defaults with production-sized
  `requests`/`limits` (e.g. 4Gi–8Gi heap for brokers, 2–4Gi for
  controllers).
- **External access** — use `NodePort`, `LoadBalancer`, or a `NodePort`
  + Ingress; the chart's `externalAccess` block supports this.
- **Monitoring** — re-enable `metrics.jmx` and scrape the JMX exporter
  with Prometheus.

---

## References

- [Apache Kafka 4.0 release notes](https://kafka.apache.org/documentation#kraft)
- [Bitnami Kafka Helm chart](https://github.com/bitnami/charts/tree/main/bitnami/kafka)
- [Kafka KRaft mode docs](https://kafka.apache.org/documentation/#kraft)
- [KIND quickstart](https://kind.sigs.k8s.io/docs/user/quick-start/)
