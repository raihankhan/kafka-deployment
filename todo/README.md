# Contribution backlog

This directory tracks **planned contributions** that move this repo from
"deploys Kafka on KIND" to a **production-grade, big-company HA
reference**.

Each entry is a self-contained design doc — scope, file layout, chart
keys to override, acceptance criteria, and verification commands. Pick
an entry, open a PR, and link the PR back into the doc's *Status*
section.

---

## Index

| #  | Contribution                                                  | Status   | Effort    | Impact | Pairs with |
|----|---------------------------------------------------------------|----------|-----------|--------|------------|
| 01 | [cert-manager TLS / mTLS bundles](./01-cert-manager-tls.md)   | planned  | 2–3 days  | High   | 04         |
| 02 | [Tiered storage + MinIO](./02-tiered-storage-minio.md)        | planned  | 3–4 days  | High   | —          |
| 03 | [Chaos & failover test harness](./03-chaos-failover-harness.md) | planned | 4–5 days  | Very high | 04, 01  |
| 04 | [Observability stack (Prometheus/Grafana/JMX)](./04-observability-stack.md) | planned | 3–4 days | High | 01, 03 |
| 05 | [Multi-region MM2 + DR runbooks](./05-multiregion-mirrormaker2.md) | planned | 5–7 days | Highest | 04, 03 |

> **Status legend:** `planned` → `in-progress` (PR open) → `merged` →
> `verified` (reproduced on KIND).

---

## Recommended order

```
01 (TLS)  →  04 (Observability)  →  02 (Tiered storage)
                                    ↓
                              03 (Chaos)  →  05 (Multi-region)
```

**Rationale**

- **01 (TLS)** is the smallest, lowest-risk PR and immediately takes
  the repo out of "toy" territory. Do it first.
- **04 (Observability)** pairs naturally — once you have TLS, you need
  JMX metrics over the TLS port, and that's the same PR.
- **02 (Tiered storage)** is independent and can be done any time.
- **03 (Chaos)** needs 01 + 04 to be useful (you want to assert
  metrics stay green during the experiment).
- **05 (Multi-region)** is the flagship but the biggest — do it last
  when the repo has momentum and possibly other contributors.

---

## Common acceptance criteria (apply to every entry)

A contribution is **done** when **all** of the following are true on a
clean KIND cluster:

1. The deploy instructions in the new values file / manifest set work
   in one command (`make` target, single `helm install`, or single
   `kubectl apply -k`).
2. `helm template` + `kubectl apply --dry-run=server` produce no errors.
3. A smoke test (Go or bash) verifies the new behavior in < 5 minutes.
4. README has a new section with: what it does, why it matters, the
   exact commands to deploy and tear down, and a screenshot / ASCII
   proof of working.
5. CI (or at least a documented `make verify`) runs the smoke test and
   exits 0.

---

## Repo conventions every contribution must follow

- **No new top-level files** without a `todo/` ticket (don't pollute
  the root).
- **Pin every image** to a digest, not a tag, in production-shaped
  files.
- **Use the `bitnamilegacy/*` image overrides** for any Bitnami chart
  image (see `values.yaml` for the established pattern).
- **Preserve the `external-kafka` + `dedicated-kafka` release names**
  as the canonical examples; new artifacts get their own release
  names (`tls-kafka`, `multi-region-east`, etc.).
- **Always provide a tear-down command** that returns the cluster to
  the pre-deploy state.

---

## Tracking progress

When you start a contribution:

1. Open a PR that links to the design doc.
2. Edit the doc's *Status* line and link the PR.
3. When the PR merges, mark it `merged` and add the merge commit SHA.
4. When you've reproduced the contribution on a fresh KIND cluster
   end-to-end, mark it `verified`.
