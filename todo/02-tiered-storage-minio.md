# 02 — Tiered storage with S3 / MinIO + long-term retention

**Status:** `planned`
**Effort:** 3–4 days
**Impact:** High — Kafka 4.0's headline feature; rarely demonstrated
on Kubernetes in the open-source world.
**Pairs with:** — (independent)

---

## Why

Kafka 4.0 ships **KIP-405 / KIP-950 tiered storage** as a stable,
KRaft-compatible feature. Broker-local SSDs cannot keep up with
audit, security, or clickstream retention requirements measured in
months or years. Tiered storage moves older log segments to S3 (or
any S3-compatible object store) while keeping the hot tail on local
disk, typically cutting storage cost 10–100×.

Every "how do I enable tiered storage" tutorial on the internet is:

- Paywalled behind Confluent docs, **or**
- Stops at "set `KAFKA_TIERED_STORAGE_ENABLED=true`" without showing
  end-to-end Kubernetes deployment, **or**
- Uses a real AWS account, so you can't reproduce it on a Mac.

This contribution delivers a fully-reproducible reference:
**MinIO on Docker, KRaft cluster, hot/cold topic examples, and a cost
comparison table**.

---

## File layout

```
.
├── values-prod-tiered-storage.yaml        # main values file
├── minio/
│   ├── docker-compose.yml                 # MinIO + createbuckets init
│   └── README.md                          # how to expose it to the cluster
├── topics/
│   ├── tiered-hot.yaml                    # admin client: 1h local, 30d S3
│   ├── tiered-cold.yaml                   # 24h local, 365d S3
│   └── apply-topics.go                    # Go program that applies the topic configs
├── scripts/
│   ├── setup-minio.sh                     # boot minio, create bucket, export access key Secret
│   ├── verify-tiered.sh                   # asserts segments are in S3 after retention kicks in
│   └── cost-compare.sh                    # prints the cost table
├── tests/
│   └── tiered_storage_smoke.go            # produce 1 Gi, wait 1h+ for tier, assert segments in S3
└── docs/
    └── tiered-storage-architecture.md     # the data-flow diagram
```

---

## Chart keys to override

| Key                                                | Value                                 | Why                                                |
|----------------------------------------------------|---------------------------------------|----------------------------------------------------|
| `extraEnvVars[].name=KAFKA_TIERED_STORAGE_ENABLED` | `"true"`                              | enable the feature                                 |
| `extraEnvVars[].name=KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME` | `org.apache.kafka.storage.internals.log.remote.S3RemoteLogStorageManager` | S3 backend (MinIO is wire-compatible) |
| `extraEnvVars[].name=KAFKA_REMOTE_LOG_STORAGE_S3_BUCKET_NAME`     | `kafka-tiered`                        | bucket created by `setup-minio.sh`                 |
| `extraEnvVars[].name=KAFKA_REMOTE_LOG_STORAGE_S3_ENDPOINT`        | `http://minio.kafka.svc.cluster.local:9000` | in-cluster MinIO endpoint (we deploy MinIO as a side Service) |
| `extraEnvVars[].name=KAFKA_REMOTE_LOG_STORAGE_S3_REGION`          | `us-east-1`                           | MinIO accepts anything                             |
| `extraEnvVars[].name=KAFKA_REMOTE_LOG_STORAGE_S3_ACCESS_KEY_ID`   | (from `kafka-s3-credentials` Secret)  |                                                 |
| `extraEnvVars[].name=KAFKA_REMOTE_LOG_STORAGE_S3_SECRET_ACCESS_KEY` | (from `kafka-s3-credentials` Secret) |                                                 |
| `extraEnvVars[].name=KAFKA_TIERED_STORAGE_SKIP_OFFSET_GC`         | `"false"`                             | keep offsets until segments are tiered             |
| `extraEnvVars[].name=KAFKA_TIERED_STORAGE_MAX_SEGMENT_SIZE_BYTES`| `16777216`                            | 16 MiB remote segments (smaller = faster recovery) |
| `existingSecret` (new top-level key)               | `kafka-s3-credentials`                | the chart mounts it as envFrom                     |
| `persistence.size` (combined mode) or `broker.persistence.size`   | `10Gi`                                | keep brokers lean — only the hot tail lives here  |
| `controller.persistence.size`                     | `5Gi` (unchanged)                     | controllers don't carry data                      |
| `logRetentionHours`                               | `8760` (1 year, default for new topics) | long-term retention floor                        |

---

## Companion files

### `minio/docker-compose.yml`

```yaml
version: "3.9"
services:
  minio:
    image: quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Web console
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
  createbuckets:
    image: minio/mc:RELEASE.2024-10-08T09-37-26Z
    depends_on: { minio: { condition: service_healthy } }
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb -p local/kafka-tiered &&
      mc anonymous set download local/kafka-tiered
      "
```

### `topics/tiered-hot.yaml`

```yaml
topics:
  - name: payments.events
    partitions: 12
    replicationFactor: 3
    config:
      "remote.storage.enable": "true"     # per-topic opt-in
      "local.retention.ms": "3600000"     # keep 1h on broker
      "retention.ms": "2592000000"        # keep 30d total
      "cleanup.policy": "delete"
      "segment.ms": "3600000"             # 1h segments
```

### `topics/tiered-cold.yaml`

```yaml
topics:
  - name: audit.security
    partitions: 6
    replicationFactor: 3
    config:
      "remote.storage.enable": "true"
      "local.retention.ms": "86400000"    # 24h on broker
      "retention.ms": "31536000000"       # 365 days
      "cleanup.policy": "delete"
      "segment.ms": "21600000"            # 6h segments
```

### `scripts/cost-compare.sh` (output goes in the README)

```bash
#!/usr/bin/env bash
# A rough, realistic cost comparison for a 12-broker cluster
# running 100 Gi of hot data + 1 Ti in tiered storage.

cat <<EOF
| Tier        | Volume | $/GiB/mo | $/mo   | Notes                              |
|-------------|--------|----------|--------|------------------------------------|
| Broker SSD  | 1.2 Ti | 0.10     | $122   | gp3 / pd-ssd, 3k IOPS baseline     |
| S3 Standard | 1 Ti   | 0.023    | $24    | first 50 Ti tier                  |
| S3 IA       | (alt)  | 0.0125   | $13    | if accessed < 1× / month           |
| MinIO (self)| 1 Ti   | 0.04     | $40    | 3× replication on cheap HDDs       |

  → 10× cheaper to keep 1 year of audit logs in tiered S3
    vs keeping them on broker SSDs.
EOF
```

---

## README changes

Insert after **Securing the cluster (TLS / mTLS)**:

```markdown
## Tiered storage (S3 / MinIO)

Kafka 4.0 ships stable tiered storage. With this repo you can run
the full feature on a Mac using MinIO as the S3 backend.

See [todo/02-tiered-storage-minio.md](./todo/02-tiered-storage-minio.md)
for the design and `values-prod-tiered-storage.yaml` for the values
file.

```bash
# 1. Boot MinIO + create the bucket
./scripts/setup-minio.sh

# 2. Install the tiered cluster
helm install tiered-kafka oci://.../bitnamicharts/kafka \
  --namespace kafka -f values-prod-tiered-storage.yaml --wait --timeout 15m

# 3. Apply the example topic configs
go run topics/apply-topics.go

# 4. Verify segments land in S3 after the local retention window
./scripts/verify-tiered.sh
```

The README also embeds the cost-comparison table from
`scripts/cost-compare.sh` so a reader can quote it in an architecture
review.
```

---

## Acceptance criteria

- [ ] `setup-minio.sh` boots MinIO in a Docker container, creates the
      `kafka-tiered` bucket, and creates a `kafka-s3-credentials`
      Secret in the `kafka` namespace
- [ ] `helm install tiered-kafka … -f values-prod-tiered-storage.yaml`
      brings the cluster to 3/3 Ready
- [ ] After producing 1 Gi to `payments.events` and waiting
      `local.retention.ms` (1h in test, accelerated to 60s for
      tests), the S3 bucket shows the expected number of `.log`
      segment files
- [ ] `tiered_storage_smoke.go` produces 100 MB, waits for the
      tier-off window, and asserts `minio ls -r kafka-tiered/` shows
      at least one segment
- [ ] Reading from offset 0 (start of topic) returns the same
      messages — the broker reconstructs the log from tiered
      segments
- [ ] Cost-comparison table renders correctly in the README

---

## Verification commands

```bash
# Render & lint
helm template tiered-kafka oci://.../bitnamicharts/kafka \
  -n kafka -f values-prod-tiered-storage.yaml > /tmp/tiered.yaml

# Deploy
./scripts/setup-minio.sh
helm install tiered-kafka oci://.../bitnamicharts/kafka \
  -n kafka -f values-prod-tiered-storage.yaml --wait --timeout 15m

# Apply topics + produce
go run topics/apply-topics.go
go run tests/tiered_storage_smoke.go

# Verify in S3
docker exec minio mc ls -r local/kafka-tiered/

# Tear down
helm uninstall tiered-kafka -n kafka
docker compose -f minio/docker-compose.yml down -v
```

---

## Open questions

1. **Topic-level vs. broker-level opt-in?** KIP-950 lets you do
   either. We pick **per-topic opt-in** (`remote.storage.enable=true`)
   because it lets users keep legacy topics local-only and only
   opt in the ones that need long retention. Confirm in PR review.
2. **MinIO on the KIND host vs. in-cluster?** The proposal is
   **out-of-cluster** (host Docker) because it survives `kind delete
   cluster` and can be reused across multiple test clusters. The
   in-cluster alternative is also valid — discuss in the PR.
3. **Do we ship a real `setup-minio.sh` for macOS Docker Desktop
   only, or do we also support OrbStack / colima / podman?** v1
   should target Docker Desktop and document the others.
