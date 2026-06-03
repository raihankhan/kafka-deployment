# CONTEXT.md

> **Last refreshed:** 2026-06-03
> **Refresh discipline:** Manually updated when something material
> changes (new release, new values file, new gotcha). Don't auto-generate.

This file gives a fresh agent (Claude, a teammate, future-you) the
**30-second orientation** to this repo. Read this first; the README
is the deep dive.

---

## Project summary

- **Goal:** a production-shaped, **Kafka 4.0 in KRaft mode** reference
  deployment on a local **KIND** (Kubernetes IN Docker) cluster on
  macOS, packaged with a web UI and a planner for advanced
  contributions.
- **Stack:** Bitnami Kafka Helm chart (OCI), Kafbat UI chart,
  Kafka 4.0.0, KRaft-only (no Zookeeper).
- **Audience:** platform / SRE engineers who want a copy-pasteable
  starting point for promoting a "works on my laptop" Kafka to
  staging / production.
- **License:** see `LICENSE` (Apache 2.0).

---

## Stack pin

| Component       | Version     | Source                                |
|-----------------|-------------|---------------------------------------|
| Helm            | 3.18+       | OCI registry support required         |
| Kubernetes      | 1.32.x      | via KIND on macOS Docker host         |
| Kafka           | 4.0.0       | `bitnamilegacy/kafka:4.0.0-debian-12-r10` |
| KRaft           | enabled     | hard-coded in every values file       |
| Zookeeper       | disabled    | `zookeeper.enabled: false` everywhere  |
| StorageClass    | `standard`  | rancher.io/local-path (KIND default)  |
| Kafbat UI       | 1.6.4 / v1.5.0 | `kafbat-ui/kafka-ui` Helm chart    |
| Cert-manager    | n/a (not yet deployed) | planned in `todo/01-cert-manager-tls.md` |

---

## File map

| Path                                                | What it is                                                  |
|-----------------------------------------------------|-------------------------------------------------------------|
| `README.md`                                         | Full deploy + verify + Kafbat docs (start here for humans)  |
| `values.yaml`                                       | **Combined mode** values: 3 pods, broker+controller in each |
| `values-prod-dedicated.yaml`                        | **Dedicated mode** values: 3 brokers + 3 dedicated controllers |
| `kafbat-values.yaml`                                | Kafbat UI wired to the `external-kafka` Service             |
| `CONTEXT.md`                                        | **This file** — agent/agent-agnostic orientation            |
| `todo/README.md`                                    | Backlog of 5 production-grade contributions                 |
| `todo/01-cert-manager-tls.md`                       | TLS/mTLS via cert-manager (planned)                         |
| `todo/02-tiered-storage-minio.md`                   | Kafka 4.0 tiered storage + MinIO (planned)                  |
| `todo/03-chaos-failover-harness.md`                 | chaos-mesh experiments + Go assertion harness (planned)     |
| `todo/04-observability-stack.md`                    | Prometheus + Grafana + 5 dashboards + 5 alerts (planned)    |
| `todo/05-multiregion-mirrormaker2.md`               | Multi-region MM2 + DR runbooks (planned)                    |
| `skills/kafka-topic/SKILL.md`                       | Read-only Kafbat REST API skill (`/kafka-topic` slash command) |
| `LICENSE`                                           | Apache 2.0                                                   |

---

## Live state (last refreshed 2026-06-03)

> Verify with `kubectl get pods,svc,statefulset -n kafka`.

| Release         | Chart           | Status   | Pods | Notes                              |
|-----------------|-----------------|----------|------|------------------------------------|
| `external-kafka`| kafka 32.4.3    | deployed | 3/3  | combined mode, PLAINTEXT           |
| `kafbat-ui`     | kafka-ui 1.6.4  | deployed | 1/1  | NodePort 30080, points at external-kafka |

**Namespace:** `kafka`
**KRaft cluster id:** stored in Secret `kafka-kraft-secret` (regenerate
to recreate; not safe to commit).
**StorageClass:** `standard` (local-path), per-pod 5 Gi PVCs.

To re-provision from scratch:

```bash
export CLUSTER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
kubectl create namespace kafka
kubectl create secret generic kafka-kraft-secret \
  --from-literal=kraft-cluster-id=$CLUSTER_ID -n kafka
helm install external-kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  -n kafka -f values.yaml --wait --timeout 15m
helm install kafbat-ui kafbat-ui/kafka-ui \
  -n kafka -f kafbat-values.yaml --wait --timeout 5m
```

---

## Critical gotchas (read these before touching anything)

1. **Bitnami moved their Docker org.** The chart's hard-coded
   `docker.io/bitnami/*` references **404** today. Every values file
   in this repo overrides the image repository to `bitnamilegacy/*`
   (kafka, os-shell, kubectl). If you copy a new key from upstream
   `values.yaml` and forget to override its image, `helm install`
   will time out with `ImagePullBackOff`. Check the rendered
   manifests with `helm template … | grep image:` if in doubt.
2. **In dedicated mode, `controllerOnly: true` alone is not enough.**
   You also need `broker.replicaCount > 0` to actually create a
   second StatefulSet. Without it, only the controller SS is created
   (controllers run as combined, despite the flag).
3. **Helm key collision in YAML files.** If you have two `controller:`
   blocks (or two `broker:` blocks) in one file, **the second one
   replaces the first** — it does not deep-merge. Use a single block
   per top-level key.
4. **StatefulSet specs are mostly immutable.** Changing
   `volumeClaimTemplates.size` (e.g. via persistence) requires
   `helm uninstall` + recreate, **not** `helm upgrade`. The chart
   will refuse the upgrade with `updates to statefulset spec for
   fields other than 'replicas', 'ordinals', 'template', ...`.
5. **Kafbat's bundled `jmx-exporter` is also a `bitnami/*` image.**
   If you re-enable JMX in `values-prod-observability.yaml`
   (planned in `todo/04-observability-stack.md`), override it to
   `bitnamilegacy/jmx-exporter` or supply a sidecar.
6. **The `/kafka-topic` skill is read-only by design.** Any
   state-changing request returns a *plan* with the exact payload,
   not an execution. Don't add write endpoints without rewriting
   the safety rules in `skills/kafka-topic/SKILL.md`.
7. **PLAINTEXT everywhere.** No TLS between clients/brokers, brokers,
   or controllers. For TLS, see `todo/01-cert-manager-tls.md`.
8. **Dedicated-mode release (`dedicated-kafka`) was tested in this
   session and torn down. Only `external-kafka` is live.**
   To re-deploy, see *Deploying in dedicated mode* in the README.

---

## Conventions

- **Release names are fixed and meaningful:** `external-kafka`
  (combined), `dedicated-kafka` (dedicated), `tiered-kafka`,
  `tls-kafka`, `obs-kafka`, `multi-region-{east,west}`. New artifacts
  get their own; don't reuse an old one.
- **One values file per topology** — never merge combined and
  dedicated configs into one.
- **Pin every production image to a digest, not a tag** (planned
  convention; currently the repo pins to tags only).
- **README is human-facing;** `todo/*.md` is design-doc-facing;
  `CONTEXT.md` (this file) is **agent-facing**. Keep them disjoint.
- **Never commit credentials, KRaft cluster ids, or `KAFBAT_BEARER_TOKEN`.**

---

## When you're stuck

1. `helm template … -f values.yaml | grep image:` → verify all
   `bitnami/*` are gone.
2. `kubectl describe pod <pod> -n kafka` → look at the *Events* section
   for `Failed` lines.
3. `kubectl logs <pod> -n kafka | grep -iE "kraft|raft|process.roles"`
   → confirm KRaft is the mode, not ZK.
4. Kafbat API quick check: `kubectl port-forward -n kafka
   svc/kafbat-ui-kafka-ui 18080:8080 &` then
   `curl -s http://localhost:18080/api/clusters/external-kafka/stats`.
5. Invoke `/kafka-topic <question>` for any topic / partition / lag
   investigation.
