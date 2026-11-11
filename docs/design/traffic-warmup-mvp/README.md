# Real-traffic warm-up MVP design

Status: proposal for `GLOBAL-4`; implementation is intentionally not started.

Updated: 2026-11-11. Infrastructure findings below include subsequent GLOBAL-3 work
through `b0c2c80`. Protocol and lifecycle choices remain subject to review.

## Inventory

The historical implementation remains in
`GLOBAL-3_real_traffic_unordered_warmup_mvp`. It adds roughly 10,000 lines across
core, demo, audit, and infrastructure. The useful parts and their proposed treatment are:

| Historical part | Decision for GLOBAL-4 |
| --- | --- |
| Explicit assignment at the current end position | Reimplement narrowly for library-owned recipients. Preserve the readiness failure fix from `0a44051`; do not expose this as a general application subscription mode unless it is independently useful. |
| Unique service group IDs | Reuse the CORE-81 rule: every internal recipient gets a library-owned, session-specific group ID, independent of the main consumer group. |
| Record copy and acknowledged producer bridge | Reuse the key/value/header/timestamp copy and acknowledgement primitives. Remove fractional selection, send permits, and ACK-as-source-completion. Put send and outcome wait under one deadline. |
| Versioned control codec and Kafka transport | Keep versioning, one physical consumer per context, per-instance broadcast groups, poll/dispatch separation, bounded buffers, and bounded shutdown. Replace journal synchronization, snapshots, generations, and coordinator replication with the protocol below. |
| Recipient drain to final destination offsets | Keep as a tail-completion boundary in addition to per-record outcomes. It is not evidence of handler success. |
| Recipient-to-main lifecycle | Keep the library-owned `warm-up -> drain -> main` shape and cancellation race fixes. Simplify it to one local lifecycle owner without coordinator handoff. |
| Forwarding observer | Keep a structured, high-cardinality event after producer ACK. Do not turn session IDs or offsets into metric tags. |
| Old coordinator, reservations, snapshots, elections, quarantine, replicated state machine | Do not port. |
| Old forwarding-aware audit correlation | Preserve only as a historical diagnostic tool. The primary report will classify records by configured topic sets. |
| Spring terminal-failure auto-configuration refactoring | Not required by the new mechanism; do not port as part of GLOBAL-4. |

One generic fix should precede warm-up work: a poll loop that fails before assignment readiness
must complete its readiness signal exceptionally, and an empty assignment callback must still
complete readiness. This is the behavior isolated in historical commit `0a44051`; it should be
reimplemented and reviewed as an independent CORE task.

## Processing seam and outcomes

Delegation belongs after a runtime has admitted a record and applied its queue/freshness policy,
at the point where the runtime currently invokes `RecordProcessor.process`. The runtime's worker,
key lane, or partition lane remains occupied until the delegated operation reaches a terminal
decision. No warm-up component owns ordering or adds another admission queue.

The internal pipeline needs a terminal outcome model shared by every processing mode:

| Internal outcome | Meaning | Remote result |
| --- | --- | --- |
| `HandlerSucceeded` | Handler returned after its configured retries. | `SUCCEEDED` |
| `HandlerFailed` | Handler exhausted retries and the configured failure callback completed. | `TERMINAL_FAILURE` |
| `PolicyDiscarded(reason)` | A freshness policy intentionally discarded the record. | `DISCARDED(reason)` |
| `HandedOff(reason)` | Assignment/lifecycle ended without a terminal record outcome. | No `RecordCompleted` |

The current `ProcessingFailureHandler.skip()` is not a distinct successful handler outcome. It is
reported as `HandlerFailed`, because the business handler did not succeed. There is no separate
core skip policy today; a future explicit skip outcome may be added without calling it success.

CORE-83 implements this as the internal `RecordProcessingOutcome` model. Every runtime invokes
one `AdmittedRecordInterceptor` around the complete local pipeline while retaining its normal
worker/key/partition slot. A separate `RecordProcessingOutcomeObserver` also covers policy
discards that happen before the interceptor, such as freshness replacement and queue overflow.

Initial donor mapping is:

- `SUCCEEDED`: terminalize the source as processed and emit its ordinary source metrics/audit;
- `TERMINAL_FAILURE`: run the complete local handler/retry/failure pipeline once;
- `DISCARDED`: apply the same discard only when the source runtime supports that policy outcome;
  an impossible outcome/mode combination fails the session and leaves the source non-terminal;
- infrastructure failure or deadline: fail the session and run the local pipeline for every
  still-owned, unconfirmed source record;
- lifecycle cancellation caused by shutdown, revoke, or assignment loss: unregister the wait and
  propagate handoff/cancellation without local fallback.

Infrastructure exceptions are never passed to the application's processing failure handler.

## Fixed-link data plane

The first implementation uses an internal/test-provided active link and has no temporary demo
HTTP API.

1. The runtime policy admits a source record into its normal slot.
2. A synchronized interval counter atomically rolls the interval and consumes one quota unit.
3. If the link is not ready or no quota remains, the ordinary local pipeline runs.
4. A selected record is copied to its agreed warm-up topic and partition.
5. Producer ACK supplies the destination topic, partition, and offset; only then is the forwarding
   monitoring event emitted.
6. The donor registers or consumes an early result under
   `(sessionId, destination topic, partition, offset)` and suspends without blocking a thread.
7. The winning terminal result is mapped to the source outcome; the tracker entry is removed.

The deadline starts before the producer call and covers serialization/metadata blocking, ACK, and
result wait. The producer call runs on an interruptible blocking dispatcher and its Kafka timeouts
are capped by the delegation timeout. A late ACK or result cannot terminalize a source after
fallback.

There is no `maxInFlight` gate. A new interval receives a full quota regardless of prior waits;
ordinary runtime concurrency and the deadline bound outstanding work. Unused quota is discarded.

The result tracker uses one short lock and performs no network operations while holding it. It
supports both registration orders, idempotent duplicates, and a single atomic winner between
result, timeout/session failure, and lifecycle cancellation. Early results are limited by count
and TTL; overflow fails the session instead of silently evicting a potentially valid result.

## Result publication

The recipient runs the same handler, retry policy, failure callback, processing context, runtime
policy, metrics, and audit wiring as its future main consumer. It does not delegate warm-up records.
After a terminal runtime outcome it sends:

```text
RecordCompleted(sessionId, warmupTopic, partition, offset, outcome)
```

The message is sent only after terminal policy completion. Kafka commit position and processed
frontiers are never converted into business outcomes. Recipient commits retain ordinary
at-least-once semantics.

## Minimal automatic protocol

The control topic is single-partitioned. Each context has one physical consumer with its own
library group ID, so every context sees the same ordered stream; dispatch never blocks the Kafka
poll loop. On startup the context replays only the bounded session horizon needed to prove that no
unexpired session exists. Missing history, malformed state, or ambiguity causes cold start.

The MVP permits one active session per control namespace. This deliberately conservative limit
provides exclusive warm-up destination partitions without distributed reservations.

| Message | Sender -> receiver | Effect |
| --- | --- | --- |
| `WarmupRequested` | cold recipient -> all | Declares session ID, recipient, compatible consumer/topic mappings, deadline, and configuration fingerprint. |
| `DonorOffered` | running context -> recipient | Offers exact compatible local participants and current source assignments. |
| `SessionAccepted` | recipient -> selected donor context(s) | Fixes participants and one exclusive destination partition per mapping. |
| `DonorAccepted` / `SessionRejected` | donor -> recipient | Atomically accepts the session in a local idle gate or rejects it. |
| `RecipientReady` | recipient -> donors | Confirms explicit assignment and starting positions; only this enables quota admission. |
| `RecordCompleted` | recipient -> donor | Completes one destination coordinate with a terminal outcome. |
| `ForwardingStopped` | donor -> recipient | Closes new admissions and supplies final ACKed destination offsets and stop reason. |
| `RecipientDrained` | recipient -> donors | Confirms terminal outcomes/consumption through the agreed tail. |
| `SessionEnded` | owner -> all | Records `COMPLETED`, `FAILED`, or `COLD_FALLBACK`; all participants release local state. |

The recipient owns selection. After a short offer window it chooses a deterministic compatible
set (coverage first, then stable instance ID). Each consumer mapping has exactly one donor. Warm-up
partition assignment is fixed in `SessionAccepted` and remains exclusive because only one global
session may be active.

Concurrent requests are resolved by their `WarmupRequested` control offsets: the earliest valid,
unexpired request wins; later recipients immediately cold-start. A donor accepts only the winning
session in a local `idle/active` gate. Conflicts, partial acceptance, expired deadlines, and stale
participant messages terminate warm-up rather than trigger recovery. Every non-result message is
validated against the exact session ID, participants, configuration fingerprint, and deadline.
No failed session is recovered or transferred to another coordinator.

The local transitions stay deliberately small:

| Role | Transition | Trigger |
| --- | --- | --- |
| Recipient | `NEW -> DISCOVERING` | Its ordered `WarmupRequested` is the winning request. |
| Recipient | `DISCOVERING -> PREPARING` | A complete compatible donor set accepts. |
| Recipient | `PREPARING -> WARMING` | Every assigned recipient consumer is ready. |
| Recipient | `WARMING -> DRAINING` | All donors stop forwarding and provide final offsets. |
| Recipient | `DRAINING -> MAIN` | The bounded tail and outcome sends complete. |
| Recipient | Any pre-main state `-> COLD` | Conflict, ambiguity, rejection, failure, or deadline; cleanup finishes within its bound. |
| Donor | `IDLE -> ACTIVE` | It atomically accepts the winning session. |
| Donor | `ACTIVE -> STOPPING -> IDLE` | Duration, session failure, or explicit stop closes admission and resolves current waits. |
| Either | Any state `-> STOPPED` | Application lifecycle cancellation; never starts main as a side effect. |

## Lifecycle

The cold context prepares recipient consumers at the current destination ends, waits for all of
them to be ready, then announces readiness. When the duration expires or any infrastructure path
fails, donors close admission. They wait only for already-started sends within the cleanup bound
and publish final destination offsets. The recipient consumes and reports the bounded tail, closes
warm-up consumers, and starts main consumers in the same process.

If discovery, agreement, readiness, or cleanup cannot finish within its deadline, the context
performs cold start. Application shutdown cancels the lifecycle and never starts main consumers.

## Monitoring and audit

`ConsumerMetrics` is the existing common observation path for processed, failed, discarded, and
handed-off records. CKC demo audit should use that path consistently for `C/F/D/H`, rather than
writing `C/F` only inside handlers, so applying a remote outcome can emit the ordinary source audit
without executing the donor handler.

A separate structured forwarding callback carries source and destination coordinates after ACK.
The demo may encode it as an additional diagnostic audit record. Aggregate Micrometer callbacks
cover delegated count, result type, fallback, timeout, pending waits, send latency, outcome-wait
latency, actual budget, and time to main subscription. Metric tags stay low-cardinality.

The analyzer receives configured `sourceTopics` and `warmupTopics`. Source topics keep normal
publication-to-terminal correctness. Warm-up topics form a separate outcome-only category whose
records do not require generator publications and are not added to source totals or duplicate
checks. Forwarding correlation is optional diagnostics, not a report dependency.

## Proposed child tasks

These IDs avoid the historical GLOBAL-3 child IDs. They are proposals only and are not added to
`TASKS.md` or started until this design is approved.

1. `CORE-82` (done): reimplement the independent poll-loop readiness completion fix.
2. `CORE-83` (done): introduce terminal processing outcomes and a mode-independent admitted-record seam;
   verify all ordering and freshness modes without warm-up.
3. `CORE-84`: implement fixed-link quota, producer deadline, result tracker, outcome application,
   cancellation semantics, forwarding event, metrics, and real-Kafka integration tests.
4. `CORE-85`: implement the reduced control codec/transport and the reviewed single-session
   protocol, including simultaneous-request rejection and stale-message filtering.
5. `CORE-86`: implement library-owned recipient/drain/main lifecycle and the public non-global
   context/API, including cold fallback and shutdown behavior.
6. `DEMO-110`: wire the normal library API, align CKC audit with the metrics observation path, and
   record forwarding diagnostics without a demo state machine.
7. `INFRA-341`: categorize source and warm-up audit topics and render the separate report section.
8. `INFRA-342`: prepare the A/B experiment definition and evidence fields. Running the experiment
   remains a separate explicit user action.

Each child is branched from and merged back only into `GLOBAL-4_simplified_real_traffic_warmup`
after implementation, tests, separate review, and review fixes.

## Local-lab findings and deployment assumptions

The internal lab uses two Linux hosts: `optilab` is the controller/support host, and
`optilab2` is the Kubernetes worker used for application workloads. These are local-lab
hostnames, not production deployment requirements. This section describes the topology
needed to interpret the MVP experiments; a general internal-lab guide can document the
complete setup separately.

The operator's subsequent GLOBAL-3 investigation identified cross-host network packet pressure
as a limitation of the local lab. Its network interfaces have a single RX queue. Distributing
packet processing across CPU cores helped partially; HTTP stubs were then moved beside the
application on the optilab2 worker to reduce inter-host traffic. A dedicated ephemeral Kafka
broker was also deployed as a pod on that worker for control and warm-up traffic.

Preserve this prepared topology for the new comparison: application and stubs on optilab2,
generator, Redis, and primary Kafka on the controller side, and warm-up Kafka on the worker.
Keep placement, Redis capacity, JVM options, and broker presence identical between baseline
and warm-up targets. Record actual placement and resource limits in evidence. Earlier latency
or evidence anomalies must be interpreted alongside network constraints rather than attributed
entirely to JVM warm-up. Historical experiment definitions remain useful references.

The starting hypothesis is only 10-20 warm-up records per second per participating consumer.
An optional single-broker Kafka in Kubernetes is therefore a candidate low-cost production
deployment: losing a warm-up copy does not lose the source record, and failure causes local
fallback or cold start. This is not a measured sizing guarantee. Observe broker CPU, memory,
network, and contention with the application, including fixed Kafka/JVM overhead.

AWS qualification follows the working implementation and local comparison. Two-core nodes
are already densely packed; even modest broker requests may trigger an additional node.
Measure placement feasibility, actual consumption, autoscaler behavior, and incremental node
cost before selecting heap, requests, limits, or production sizing. Launch runs only on explicit
user instruction.

## Transfer plan from GLOBAL-3

The adapted opt-in infrastructure configuration is described in
[MVP lab infrastructure](lab-infrastructure.md).

Transfer reviewed final behavior in scoped child branches from GLOBAL-4. Use selected file or
function patches with source task/commit references; use cherry-pick only when a commit is truly
self-contained. Do not import old TASKS rows, coordinator machinery, or the correlation-dependent
audit model as dependencies. Transfer regression coverage with each behavior.

| Proposed task | Source | Transfer |
| --- | --- | --- |
| `INFRA-338` | INFRA-324 (`d4b781e`) | Stable pod legends and dashboard regression test. |
| `INFRA-339` | INFRA-325 (`fb8752c`) | Redis dataset/container limits and matching environment evidence/tests. |
| `INFRA-340` | INFRA-332/333/334/335/336/337 | Final dedicated-broker manifests, explicit topics, early Service endpoint publication, deployment readiness gate, shutdown-ordered cleanup, and worker-local stubs. Adapt topic discovery to the new API. |

The INFRA-340 package must preserve both sides of the readiness fix: the provisioner needs a
reachable advertised Service address before readiness, while application deployment waits for
topics to exist. Check that cleanup verifies Deployment and Service removal as well as pod
termination. Review the ten-minute lab preparation gate separately from the library's bounded
cold-fallback policy.

Adapt DEMO-109 (`75d0d4e`) independent warm-up security settings during DEMO-110, without
importing AutomaticTrafficWarmupPlan. Apply CORE-81 (`8257064`, now merged into GLOBAL-3) unique
internal group IDs and its regression scenario when implementing the new recipient lifecycle.
Select useful GC/load profiles from INFRA-323/326-331 when preparing INFRA-342; do not copy all
historical variants automatically.

Task IDs here are proposals, not started tasks. They account for historical IDs through
CORE-81, DEMO-109, and INFRA-337 and must be checked again at task creation.

## Review gates and next sequence

1. Save this inventory/design update on GLOBAL-4.
2. Implement the independent infrastructure transfers in separate scoped branches; review and
   pause before each merge into GLOBAL-4.
3. Review terminal outcome mapping, simultaneous-start behavior, and lifecycle ownership before
   core implementation. One active session per namespace is still a proposal: concurrent cold
   pods would otherwise cold-start except for the winning recipient. Independent consumer
   lifecycle versus an aggregate context barrier also remains a decision to settle.
4. Implement fixed-link data plane, then review before automatic lifecycle and demo integration.
5. Prepare local A/B, followed by AWS qualification; running either requires explicit instruction.
