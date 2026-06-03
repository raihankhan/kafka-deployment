---
name: kafka-topic
description: Read-only investigation, troubleshooting, and planning for Kafka topics via the Kafbat REST API. Refuses all write operations.
allowed-tools:
  - Bash(curl:*)
  - Bash(kubectl:*)
  - Bash(jq:*)
  - Read
when_to_use: Use when the user asks read-only questions about Kafka topics, partitions, consumer groups, broker health, or wants a plan for a future topic-management action. Auto-invoke on phrases like "check kafka topics", "list topics", "show partitions for X", "what's the replication factor on X", "is X under-replicated", "consumer group lag", "where is group X stuck", "broker health", "kafka cluster status", "active controllers", "how do I add a partition to X", "should I increase RF on X", "is this topic ready for production", "what's the retention of X", and any message starting with `/kafka-topic`.
argument-hint: "<freeform question about topics, partitions, consumer groups, brokers, or topic-management planning>"
arguments:
  - $question
context: inline
---

# kafka-topic — read-only Kafbat REST API skill

Talk to a running **Kafbat UI** (https://ui.docs.kafbat.io/) using only
GET requests against its REST API. Use this skill to investigate,
troubleshoot, and plan — never to mutate state.

> **Read-only by design.** Any POST/PUT/PATCH/DELETE is refused.
> State-changing operations must be done through the Kafbat UI, a
> separate admin skill, or the `kafka-topics.sh` CLI.

## Inputs

- `$question` (required): freeform question about topics, partitions,
  consumer groups, broker health, retention, replication factor, or
  "how do I …" planning. Examples are listed in `when_to_use` above.

## Configuration (env vars)

| Env var               | Default                              | Purpose                                              |
|-----------------------|--------------------------------------|------------------------------------------------------|
| `KAFBAT_URL`          | `http://localhost:8080`              | Base URL of the Kafbat REST API                      |
| `KAFBAT_CLUSTER`      | first cluster from `/api/clusters`   | Cluster name to scope queries                        |
| `KAFBAT_BEARER_TOKEN` | unset                                | If set, sent as `Authorization: Bearer …`            |
| `KAFBAT_USER`         | unset                                | If set with `KAFBAT_PASSWORD`, sent as HTTP Basic    |
| `KAFBAT_PASSWORD`     | unset                                | HTTP Basic password                                  |
| `KAFBAT_INSECURE`     | unset                                | If `1`, pass `-k` to curl (skip TLS verify — dev only) |

If `KAFBAT_URL` is unset and `kubectl config current-context` starts
with `kind-`, the skill prints:

```
Kafbat not reachable. Start the port-forward:
  kubectl port-forward -n kafka svc/kafbat-ui-kafka-ui 8080:8080
```

…and pauses for the user to run it.

## Hard rules

1. **Read-only.** Refuse any request that would require POST/PUT/
   PATCH/DELETE. Reply with a refusal that includes the exact
   payload that *would* have been sent, and a pointer to the
   alternative (Kafbat UI / `kafka-topics.sh`).
2. **Never log credentials.** Redact `KAFBAT_BEARER_TOKEN`,
   `KAFBAT_PASSWORD`, and the `Authorization:` header from any
   echoed curl command.
3. **No message floods.** `GET /messages` is limited to **N=10**
   unless the user types `yes peek 100` (or some explicit number ≤50).
   Refuse `N > 50` and suggest a different approach.
4. **Always pick the cluster explicitly.** If `KAFBAT_CLUSTER` is
   unset, list clusters first and ask the user to pick one before
   running any other query.
5. **Pretty output.** Render topic lists, partition tables, and
   consumer-group lag as **ASCII tables** with a trailing fenced
   ```json``` block of the raw response.
6. **No silent failures.** If a curl call returns non-200, show
   status, body, and a one-line diagnosis; never swallow the error.

## Goal

Answer the user's read-only question in one round-trip when possible,
or in a small number of round-trips for compound questions like
"is this topic ready for production?" — and, when the question implies
a state change, return a *plan* (not a mutation).

## Steps

### 1. Resolve the Kafbat endpoint and pick a cluster

```bash
# Probe Kafbat
curl -sS -m 5 -w "\nHTTP %{http_code}\n" "${KAFBAT_URL}/api/clusters" \
  -H "$(auth_header)"
```

**Success criteria:** HTTP 200 and a JSON list of clusters. If
non-200, print the error and stop. If 200 and `KAFBAT_CLUSTER` is
unset, render a one-line cluster picker and ask the user to pick.

### 2. Match the question to the right endpoint(s)

Map the question to one of the read endpoints below. If the question
is ambiguous, ask one clarifying question and stop.

| Question pattern                                           | Endpoint                                                |
|------------------------------------------------------------|---------------------------------------------------------|
| "list topics" / "what topics do we have"                   | `GET /api/clusters/{c}/topics?page=1&perPage=100`        |
| "show topic X" / "details on X"                            | `GET /api/clusters/{c}/topics/{t}`                      |
| "partitions for X"                                         | `GET /api/clusters/{c}/topics/{t}`                      |
| "retention of X" / "is X compacted"                        | `GET /api/clusters/{c}/topics/{t}/config`               |
| "consumer group G" / "where is G stuck"                    | `GET /api/clusters/{c}/consumer-groups/{g}`             |
| "lag on topic T" / "consumer groups"                       | `GET /api/clusters/{c}/consumer-groups` + per-group `/consumer-groups/{g}/lags` |
| "broker health" / "active controllers"                     | `GET /api/clusters/{c}/stats` and `/brokers`            |
| "under-replicated partitions"                              | `GET /api/clusters/{c}/topics` then filter on `inSyncReplicas < replicationFactor` |
| "messages in topic X" / "peek at X"                        | `GET /api/clusters/{c}/topics/{t}/messages?limit=N` (N≤10) |
| "how do I add a partition to X" / "increase RF" / "delete" | Step 5 below — return a plan, never a write              |

### 3. Render the response as an ASCII table + JSON

For topics:

```
+----------------------+----+----+------+--------+-----+
| NAME                 | PT | RF | ISR  | MSGS   | URP |
+----------------------+----+----+------+--------+-----+
| payments.events      | 12 | 3  | 36   | 1.2 M  |  0  |
| orders.created       |  6 | 3  | 18   | 482 K  |  0  |
| audit.security       |  3 | 3  |  9   |  12 K  |  1  |   <-- under-replicated
+----------------------+----+----+------+--------+-----+
```

For consumer groups:

```
+----------+----------------------+--------+----------+------+
| GROUP    | TOPIC                | END    | COMMITTED| LAG  |
+----------+----------------------+--------+----------+------+
| orders-  | orders.created       | 482103 | 481900   |  203 |
|  consumer|                      |        |          |      |
+----------+----------------------+--------+----------+------+
```

Then the raw JSON in a fenced block so the user can re-use it.

**Success criteria:** table is aligned, no `null` shown as
missing-number (use `—`), and the JSON block is valid.

### 4. Auto-diagnose common anomalies

If the response reveals any of the following, append a "Findings"
section after the table:

| Anomaly                                              | Detection                                        | Suggested action (in plan form, never auto-executed) |
|------------------------------------------------------|--------------------------------------------------|------------------------------------------------------|
| `inSyncReplicas < replicationFactor`                 | partition-level                                  | Investigate broker hosting an offline replica; consider reassign |
| `offlinePartitionCount > 0`                          | cluster stats                                    | Re-create topic if `__consumer_offsets` only; otherwise inspect |
| `lag > 10000` for any group                          | consumer group                                   | Check consumer logs; if multi-broker, check partition assignment skew |
| `activeControllers != brokerCount` (KRaft)           | cluster stats                                    | Inspect controller logs; quorum may be degraded       |
| `messages > 0` but `cleanup.policy = compact` and no `min.cleanable.dirty.ratio` set | topic config | Add `min.cleanable.dirty.ratio=0.1` to prevent unbounded growth |
| `segment.ms` and `segment.bytes` both unset on a high-volume topic | topic config | Set `segment.ms=3600000` so log compaction runs hourly |

### 5. Plan-only for state-change questions

For any question that implies a mutation, return a *plan*:

```
PLAN (not executed — read-only skill)

Proposed change : Add 6 partitions to topic `payments.events`
                  (current: 12 → 30)
Affected replicas: 12 × 3 = 36 partition-replicas
Estimated reassignment time: 30-90s per partition
Data loss risk  : NONE (add-partitions is non-destructive)
Downtime        : NONE

The exact call (do NOT run this skill to execute it):
  POST /api/clusters/external-kafka/topics/payments.events/partitions
  Body: { "count": 30 }

Alternative via kafka-topics.sh:
  kafka-topics.sh --bootstrap-server external-kafka:9092 \
    --alter --topic payments.events --partitions 30

Before running: confirm the producer keys have a uniform distribution
to avoid hot partitions after the rebalance.
```

**Success criteria:** plan is specific (endpoint, body, predicted
impact), shows the exact payload, and explicitly tells the user this
skill will not execute it.

## Examples

### "show consumer group lag on payments.events"

Skill resolves cluster = `external-kafka`, calls
`/consumer-groups`, then `/consumer-groups/{g}` for each group
subscribed to `payments.events`, and renders the lag table from
step 3.

### "is payments.events under-replicated?"

Skill calls `/topics/payments.events`, iterates partitions, and
flags any where `inSyncReplicas < replicationFactor`. The Findings
section shows the affected partition IDs and which broker is the
missing replica.

### "how do I increase RF on audit.security to 5?"

Skill returns a PLAN (step 5) with the exact
`kafka-reassign-partitions.sh` invocation and the JSON the user
would need to submit to the cluster's reassign endpoint.

### "peek at the first 5 messages in audit.security"

Skill calls `/messages?limit=5` and renders a 5-row table with
offset, partition, timestamp, key, and a truncated value (max 80
chars per row).

## Verification (smoke test for the skill itself)

1. Start the Kafbat port-forward:
   `kubectl port-forward -n kafka svc/kafbat-ui-kafka-ui 8080:8080`
2. Run the smoke checks the skill should answer in under 30s:
   - `/kafka-topic list topics`
   - `/kafka-topic consumer group lag on <known-group>`
   - `/kafka-topic broker health`
   - `/kafka-topic how do I add a partition to <known-topic>` (should return PLAN, not execute)
3. Confirm no write calls were made by tailing Kafbat access logs:
   `tail -f /var/log/kafbat-access.log | grep -E 'POST|PUT|DELETE'` should be empty.
