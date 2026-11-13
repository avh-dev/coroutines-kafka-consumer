# Real-traffic warm-up MVP design

Status: `GLOBAL-4` design implemented through bounded-history parallel-session protocol v5 (`CORE-92`).

Updated: 2026-11-13. Infrastructure findings below include subsequent GLOBAL-3 work
through `b0c2c80`. Protocol and lifecycle choices remain subject to review.

## Parallel-session control protocol (v5)

Every context reads the single-partition control topic from its current end and maintains the
complete namespace session table after synchronization. State application is a pure operation,
separate from Kafka I/O and participant lifecycle calls. Control offsets are the only ordering
authority: applying the same prefix yields the same session and reservation table.

A joining context publishes `WarmupRequested` and buffers the bounded stream it observes after
its starting boundary. Every synchronized hot context answers with `DonorOffered`, even when it
has no donor capacity. The response contains a full state snapshot, its exact inclusive control
offset boundary, monotonic `retiredThrough` deadline, sender's donor capability, and available
mappings. One valid snapshot is enough. The joiner installs it and applies buffered records strictly
from `boundary + 1`; a gap,
late snapshot, or snapshot older than already authoritative state is rejected. Unsynchronized
contexts neither reserve partitions nor publish authoritative empty snapshots. A recipient keeps
the table current while warming and after it becomes hot.

The library obtains the actual partition IDs for every configured warm-up topic from Kafka
metadata under `negotiationTimeout`; the application configures topic mappings, not partition
counts. It also verifies that the control topic has exactly one partition. Infrastructure must
create all topics beforehand. Protocol v5 assumes warm-up topics are not recreated and their
partition sets do not change while contexts are running.

Before its first reservation request the recipient waits a random 1000-2000 ms contention delay.
The recipient then selects one free destination per mapping and publishes an atomic
`ReservationRequested(sessionId, reservationId, assignments)`. Topic partition numbers are
independent, so one set may contain `orders-0`, `batches-2`, and `telemetry-1`. A reservation wins
only when the recipient consumes its own request. The first offset whose entire set is free wins;
a conflicting request owns nothing and may retry another complete set after a bounded random pause
until the common negotiation deadline. These delays reduce collisions but are not part of safety:
the single control-partition offset order is the only arbiter. Missing acknowledgements or state
responses never prove that a destination is free. Repeated requests for the same generation are
idempotent and cannot create additional ownership. No warm-up record is sent before reservation,
donor acceptance, recipient consumer readiness, and `RecipientReady`.

One donor context may admit multiple sessions. `maxDonorSessions` limits their count and
`maxDonorDelegationsPerInterval` caps their combined configured interval budgets; admission
rechecks both atomically rather than reserving capacity at offer time. Each admitted session keeps
its own quota ramp, pending-result tracker, inflight drain, deadline, destinations, and lifecycle.
Eligible sessions are selected round-robin and each source record is offered to at most one; when
delegation is unavailable the ordinary local pipeline runs. Closing or failing one data plane does
not detach the other planes or stop the main consumer.

Control-table acceptance and donor-runtime execution are separate gates. A donor prepares only
after the ordered table identifies the exact live recipient, reservation generation, destination
subset, and negotiating lifecycle; it activates only for the same generation in `READY` before the
authorization deadline. Duplicate acceptance/readiness is idempotent: in particular, a repeated
`SessionAccepted` cannot move an already ready or closing session back to negotiation. Invalid,
expired, closed, or foreign events remain observable in the control stream but cannot consume
admission capacity, create resources, activate forwarding, or deliver completion outcomes. Closing removes a pending
admission immediately; preparation or activation that was already in progress revalidates on exit
and performs generation-local cleanup instead of installing or reviving the session.

Recipient-driven completion from `CORE-90` is unchanged per session: the recipient requests close,
each donor closes admission and completes every remote result or required local fallback, and only
then sends `DonorClosed`. After all acknowledgements the recipient stops its warm-up consumers,
flushes outcomes, publishes the terminal session event and exact reservation release, then starts
main consumers. Partial preparation publishes a failed/cold terminal event, releases prepared
resources and the matching reservation, and cold-starts main consumption.

Reservations carry an absolute epoch-millisecond authorization deadline covering discovery,
negotiation, warming, drain, cleanup, and delegation timeout. Local passage of that deadline never
changes the replicated table: an ordered `ReservationReleased` for the exact session and
reservation generation removes ownership. A normal release is published by the recipient after
the CORE-90 donor-drain acknowledgements (or after recipient cleanup on cold fallback). A late or
repeated release cannot remove a partition held by a newer generation. The protocol caps this
authorization lifetime at 10 minutes; configurations whose combined lifecycle budget exceeds that
cap are rejected at context construction.
`CLOSED` is terminal in the replicated session table, so a delayed completion, readiness, or
session-end record cannot make an explicitly released reservation active in a later snapshot.

The 10-minute cap is a namespace protocol constant, not a value derived from a pod's local warm-up
settings. Reducing it requires a staged compatibility transition: the old bootstrap quarantine must
remain in force until every authorization issued under the previous maximum has passed its deadline
and clock-skew guard. Deploying a shorter quarantine immediately can reuse a destination that an old
generation is still authorized to write. The shortened maximum therefore needs an explicit protocol
transition, normally a version change, after that drain interval.

Donors stop admission at the authorization deadline and locally fall back work whose source
ownership is still valid; shutdown/rebalance cancellation remains a handoff and never processes
after ownership loss. Every synchronized authoritative context schedules an
`EMERGENCY_TIMEOUT` release for each reservation it knows. It may publish that event only after
`authorization deadline + 5 seconds`; the table rejects an earlier emergency event. This covers a
lost recipient, a lost donor, or both while any synchronized observer survives. Multiple observers
may publish the same exact release safely. The operational clock assumption is NTP plus no more
than 5 seconds pairwise wall-clock difference between any participants. Five seconds is therefore
the reuse guard after the maximum delegation authorization, not a lease evaluated while applying
control records. Forwarded records carry the
`ckc-warmup-session` header. Recipient admission accepts exactly its current session ID; an old or
foreign record is discarded before the business handler and does not publish `RecordCompleted`.

Closed sessions remain in the table through their authorization deadline so delayed lifecycle
messages cannot revive them. To bound history, every authoritative context periodically publishes
`RetireSessionsThrough(deadlineCutoff)` with `deadlineCutoff = local wall clock - 5 seconds`.
Under the stated pairwise-clock bound, every authorization at or below that cutoff has ended for
all participants. Multiple publishers are safe: the offset-ordered `retiredThrough` boundary moves
only forward. Applying the event first closes and releases every remaining reservation through the
cutoff, aborts matching donor runtime with ownership-safe fallback, and then removes all such
sessions. Later messages whose existing `expiresAtEpochMillis` is at or below the boundary are
`IGNORED_STALE`, so no per-session tombstone is needed.

Full snapshots carry `retiredThrough` plus only newer sessions. Installing a snapshot cannot move
the boundary backward or restore retired ownership. Retirement bounds accumulation over namespace
lifetime, but the codec remains intentionally bounded: an arbitrarily large number of concurrently
unexpired sessions is not guaranteed to fit in one snapshot. Protocol safety assumes publishers
obey the 5-second clock guard; table application itself remains independent of local read time.

If no authoritative snapshot source exists (first namespace start or simultaneous loss of every
context), each pod immediately uses ordinary cold main-consumer startup. It continues observing the
control tail but cannot reserve or publish an empty authoritative snapshot. Silence is not evidence
of an empty table. After the protocol-wide 10-minute maximum authorization lifetime plus the
5-second pairwise-clock guard
has elapsed from the observed starting tail, no pre-boundary donor may still delegate under an old
authorization. The fully observed context may then bootstrap an authoritative table while retaining
all post-boundary events and serve later joiners. This is the only no-replay case that can retire
unknown historical ownership without an explicit release; it makes total-loss recovery safe and
eventually live.
Tests may inject a shorter quarantine through an internal context-constructor parameter. The public
production factory cannot set this override and always uses the protocol-wide 605-second interval.

Capacity planning is direct: each simultaneous recipient needs one partition in every warm-up topic
it maps. For `N` simultaneous recipients, every such topic therefore needs at least `N` partitions,
plus any desired contention/headroom margin. Different warm-up topics may have different counts.

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
result wait. The producer call runs on an interruptible blocking dispatcher, so waiting for it
cannot bypass the delegation deadline. A late ACK or result cannot terminalize a source after
fallback.

There is no `maxInFlight` gate. A new interval receives a full quota regardless of prior waits;
ordinary runtime concurrency and the deadline bound outstanding work. Unused quota is discarded.

The result tracker uses one short lock and performs no network operations while holding it. It
supports both registration orders, idempotent duplicates, and a single atomic winner between
result, timeout/session failure, and lifecycle cancellation. Early results are limited by count
and TTL; overflow fails the session instead of silently evicting a potentially valid result.
Completed identifiers use a separate count-bounded tombstone set, so a result repeated after the
early-result TTL is still treated as a duplicate rather than reintroduced as an early result.

The fixed-link implementation is split into these narrowly scoped classes:

| Class | Responsibility |
| --- | --- |
| `FixedLinkWarmupDataPlane` | Wrap the admitted-record local pipeline, acquire interval quota, send remotely, await an explicit outcome, apply fallback, and stop the session after infrastructure failure. |
| `IntervalQuota` | Atomically roll a lazy time interval and grant at most its configured number of delegations, without carrying unused quota forward. |
| `KafkaWarmupRecordSender` | Copy key, value, headers, and timestamp to the fixed destination and return only a validated Kafka ACK coordinate from an interruptible blocking call. |
| `WarmupResultTracker` | Match `RecordCompleted` to suspended donors, retain bounded early results, resolve timeout/result races once, and retire completed identifiers. |
| `WarmupRecipientOutcomePublisher` | Convert terminal recipient runtime outcomes into non-blocking `RecordCompleted` publications; lifecycle handoff produces no result. |
| `WarmupDataPlaneModel` types | Define session, destination, record identity, remote outcomes, forwarding diagnostics, and the narrow sender/publisher callback contracts. |
| `ConsumerMetrics` warm-up callbacks | Expose delegated sends, explicit results, fallback reasons, and pending waits without high-cardinality metric tags. |

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

## Historical single-session protocol (v2, superseded by v3)

The control topic is single-partitioned. Each context has one physical consumer with its own
library group ID, so every context sees the same ordered stream; dispatch never blocks the Kafka
poll loop. A newly started context begins at the control topic's current tail and participates only
in sessions created after it becomes ready. Existing running contexts remain authoritative for
active-session conflicts. Business/lifecycle handlers run on the separate dispatch coroutine and
can never hold the Kafka poll loop. A malformed new record fails the transport by default; an
explicitly supplied invalid-record handler also runs on dispatch rather than polling.

The v2 MVP permitted one active session per control namespace. This deliberately conservative limit
provides exclusive warm-up destination partitions without distributed reservations.

| Message | Sender -> receiver | Effect |
| --- | --- | --- |
| `WarmupRequested` | cold recipient -> all | Declares session ID, recipient, compatible consumer/topic mappings, deadline, and configuration fingerprint. |
| `DonorOffered` | running context -> recipient | Offers exact compatible local participants and current source assignments. |
| `SessionAccepted` | recipient -> selected donor context(s) | Fixes participants and one exclusive destination partition per mapping. |
| `DonorAccepted` / `SessionRejected` | donor -> recipient | Atomically accepts the session in a local idle gate or rejects it. |
| `RecipientReady` | recipient -> donors | Confirms explicit assignment and starting positions; only this enables quota admission. |
| `RecordCompleted` | recipient -> donor | Completes one destination coordinate with a terminal outcome. |
| `WarmupCompletionRequested` | recipient -> donors | Announces recipient readiness to finish; each donor atomically closes new delegation admission. |
| `DonorClosed` | donor -> recipient | Confirms that every admitted delegation has finished through its remote outcome or completed local fallback. |
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

The control implementation is split into these components:

| Class | Responsibility |
| --- | --- |
| `WarmupControlProtocol` types | Define the ten versioned wire messages, mappings, fixed destinations, deadlines, fingerprints, and terminal session states. |
| `WarmupControlCodec` | Encode and decode the bounded binary v2 format with explicit type IDs, size limits, validation, and no dependency on application serialization. |
| `WarmupControlTransport` | Own one producer and one instance-group consumer, initialize at the current tail, publish with an ACK deadline, and keep polling separate from dispatch. |
| `WarmupControlRouter` | Route broadcast and locally targeted records from the single context transport to registered local participants. |
| `WarmupSingleSessionProtocol` | Atomically select the first valid request by control offset and reject expired, foreign, out-of-order, conflicting, or stale-session messages without recovery. |

The local transitions stay deliberately small:

| Role | Transition | Trigger |
| --- | --- | --- |
| Recipient | `NEW -> DISCOVERING` | Its ordered `WarmupRequested` is the winning request. |
| Recipient | `DISCOVERING -> PREPARING` | A complete compatible donor set accepts. |
| Recipient | `PREPARING -> WARMING` | Every assigned recipient consumer is ready. |
| Recipient | `WARMING -> CLOSING` | Its readiness condition fires and it requests completion from every donor. |
| Recipient | `CLOSING -> MAIN` | Every donor confirms all admitted work terminal; warm-up consumers stop and their outcome outboxes flush. |
| Recipient | Any pre-main state `-> COLD` | Conflict, ambiguity, rejection, failure, or deadline; cleanup finishes within its bound. |
| Donor | `IDLE -> ACTIVE` | It atomically accepts the winning session. |
| Donor | `ACTIVE -> CLOSING -> IDLE` | Recipient request closes admission; all admitted remote outcomes or local fallbacks finish before confirmation. |
| Either | Any state `-> STOPPED` | Application lifecycle cancellation; never starts main as a side effect. |

## Lifecycle

The cold context prepares recipient consumers at the current destination ends, waits for all of
them to be ready, then announces readiness. The recipient owns the completion decision. Today its
readiness condition is elapsed warm-up duration measured from recipient warm-up start; the wait is
isolated so a future JIT-monitoring signal can replace or augment it without moving ownership back
to donors. A rejection from any selected donor interrupts this wait immediately. When ready, the
recipient requests completion from every selected donor but keeps its warm-up consumers and outcome
publications active. If completion is processed while activation is running, the donor retains it
and applies it as soon as activation finishes. If activation is only queued, the prepared donor
closes directly and the queued task cannot activate it. Thus control-handler scheduling cannot lose
the transition or leave the recipient waiting for a `DonorClosed` that no coroutine can publish.

Each donor atomically closes admission, so later source records stay on the normal local pipeline.
It confirms `DonorClosed` only after every admitted delegation has finished: producer ACK alone is
insufficient, and any remote terminal failure must finish its local fallback first. After all donor
confirmations, the recipient stops warm-up consumers, flushes outcome publications, and starts its
main consumers.

Session expiry, recipient loss, or warm-up failure closes donor admission and releases still-owned
unconfirmed records to local fallback. This does not cancel donor workers or stop its main consumer.
Application shutdown and partition ownership loss remain lifecycle cancellation/handoff paths and
do not execute local fallback after ownership has been lost.

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
3. `CORE-84` (done): implement fixed-link quota, producer deadline, result tracker, outcome application,
   cancellation semantics, forwarding event, metrics, and real-Kafka integration tests.
4. `CORE-85` (done): implement the reduced control codec/transport and the reviewed single-session
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
