# GLOBAL-5 shared-partition real-traffic warm-up

Status: proposed design prepared for review before implementation child tasks.

Updated: 2026-11-14.

## Scope and source baseline

GLOBAL-5 warms a new application context with real records delegated by already running
contexts. Every warm-up topic has one shared partition. Recipients consume that partition
independently, but only admit records carrying their own session identifier. A donor may serve
several recipients concurrently.

The initiative is developed on `GLOBAL-5_shared_partition_warmup`, created from local `master`
commit `2af3f88f8ea1ed52ad12a429cbd727613d63d398`. Child branches merge only into this umbrella
until publication is authorized separately.

The selected implementation source is the immutable `archive/global-4` tag at commit
`2cdde950ab328dc35bf75cd178b29bcc025471b1`. Transfers use selected files or functions and cite
the more specific source commits in the map below. The archived branch is never merged or changed.
The coordinator-based `archive/global-3` tag at
`811a230b34e31d0b2b88a9137e5ff05b5e03227f` is a secondary historical reference only.

Master already contains application, stub, and generator placement, stable pod legends,
node/per-core observability, runtime Redis evidence, and increased Redis capacity. GLOBAL-5 does
not transfer those changes again. It does transfer the optional warm-up Kafka lifecycle from
GLOBAL-4.

## Protocol identity and compatibility

GLOBAL-4 used control protocol version 5. GLOBAL-5 introduces incompatible protocol version 6.
Every control envelope identifies version 6 and the configured control namespace. A context
rejects or ignores envelopes from another version or namespace before participant dispatch.

GLOBAL-5 experiments use a new namespace in the form
`global5-shared-partition-v6-<experiment>`. Infrastructure creates a fresh control topic and fresh
warm-up topics for that namespace. Topic naming remains an infrastructure concern, but names must
not overlap the archived GLOBAL-3/4 experiments. No v6 experiment reuses an archived control
topic.

Version 6 deliberately has no compatibility transition from v5: participants begin consuming the
control topic at its current end, do not reconstruct historical state, and do not share topics or
namespaces with v5.

## Topology and configuration invariants

- Infrastructure creates the control topic and all warm-up topics before contexts start.
- The control topic has exactly one partition and is common to one namespace.
- Every warm-up topic has exactly one partition.
- Warm-up topics may be spread across Kafka clusters and need not use the main-consumer cluster.
- Every participant configures a forwarding-producer connection and a warm-up-consumer connection
  for each mapping.
- Across destination clusters, warm-up topic names are unique within a namespace. Therefore
  `(topic, partition)` identifies one destination without putting credentials or a cluster alias on
  the wire.
- A participant resolves warm-up metadata through that mapping's participant connection. It
  resolves control metadata only through the control connection.
- Topics are not recreated and their partition counts do not change while v6 contexts are running.
- A control topic with a count other than one, a warm-up topic with a count other than one,
  ambiguous destination names, unavailable metadata, or one unprepared mapping causes cold
  fallback for the complete recipient context.

Configuration validation happens before discovery where possible. Runtime metadata resolution and
resource preparation share bounded deadlines. Properties and exception messages are excluded from
structured fallback diagnostics so credentials cannot be logged.

## Local state only

There is no namespace-wide session table. A context stores only:

1. its own current recipient session, while it is starting;
2. donor sessions that it has atomically accepted and is currently serving;
3. bounded duplicate/result bookkeeping belonging to those local sessions.

Publishing an offer does not reserve capacity and does not create donor-session state. Foreign
requests, offers, acceptances, readiness, completion records, results, and terminal records do not
create local state. A message can mutate state only after sender, recipient, session, mappings,
parameters, current state, and deadline have been checked against an already owned local session
or a locally targeted acceptance request.

The control topic is an ordered broadcast transport, not a replicated database. A restarted
context consumes only new control messages. When all participants restart, new v6 discovery can
begin immediately; no snapshots, replay, retirement boundary, emergency release, or quarantine is
needed because no shared destination is exclusively reserved.

Complete loss and recreation of the warm-up Kafka is a transport boundary, not protocol recovery.
Recipient runtimes that lose their assigned log terminate the current session, perform bounded
cleanup, and cold-start their main consumers. Donors close the affected sessions through the same
deadline, drain, and ownership-safe fallback path used for any other transport failure. Neither
side resumes an old session or reconstructs control history.

The long-lived donor control transport distinguishes ordinary reconnects from a recreated control
topic by its Kafka topic identity and by validating that its position still belongs to the current
log. Short metadata failures leave the existing clients and positions unchanged. If a longer outage
requires replacing a consumer while the topic identity is unchanged, the replacement explicitly
seeks to the last offset already admitted to the local dispatch queue; it never relies on
`auto.offset.reset` or replays preserved history. A confirmed new topic identity replaces the stale
control clients after a bounded retry delay. The replacement consumer reads the fresh log from its
beginning so requests published after topic recreation are not skipped; only the very first context
start seeks to the then-current tail. Identity requests and Admin shutdown are bounded, and shutdown
interrupts an in-progress identity wait or recovery delay. Once infrastructure recreates the
one-partition control and destination topics at the configured addresses, new recipient sessions can
be admitted without restarting an otherwise healthy donor context.

## Domain model

### Participant and mapping

A context registers one or more participants. A participant binds:

- one main consumer and its readiness/assignment view;
- source mapping identities used for delegation eligibility;
- forwarding producer settings for destination topics;
- warm-up consumer settings used if this context becomes a recipient;
- bounded donor-session and aggregate delegation budgets.

A mapping identifies the source participant/mapping and one warm-up destination topic. Discovery
messages contain stable mapping descriptions and configuration fingerprints, not Kafka client
properties. Selection assigns exactly one donor to each recipient mapping. One donor may receive a
subset or all mappings and may serve mappings for several recipient sessions.

### Session and record identities

Every recipient attempt creates a cryptographically strong unique session ID. A forwarded record
contains exactly one `ckc-warmup-session` header whose value is that session ID. Missing,
duplicated, malformed, foreign, or stale values are rejected by the recipient filter.

A remote result is addressed by:

```text
(sessionId, destinationTopic, destinationPartition, destinationOffset)
```

The destination topic name is unambiguous within the namespace and the partition is always zero,
but the partition remains explicit in messages and checks. Results never match by offset alone.

### Donor readiness

A context may offer a participant only when its main consumer is running, all relevant poll loops
have completed readiness, and it currently owns every source partition advertised for that
mapping. The offer contains the actual eligible source assignments and their ownership generation.

Acceptance repeats this check atomically with donor capacity admission. The donor must still own
the selected source assignments under the same current ownership generation. An offer is only
discovery information; it grants no capacity and authorizes no delegation. If ownership changes
after admission, active or queued records follow the existing ownership-safe handoff rules and the
affected donor session performs bounded close/fallback without affecting other sessions.

## Control messages

The final codec names may be refined during CORE implementation, but version 6 has these semantic
messages:

| Message | Sender | Purpose |
| --- | --- | --- |
| `WarmupRequested` | Recipient | Declares its unique session, recipient identity, mappings, fingerprints, and deadlines. |
| `DonorOffered` | Ready donor | Statelessly reports compatible mappings, current source ownership, and capability. |
| `SessionAccepted` | Recipient | Selects one donor for a mapping subset and asks it to admit that exact session. |
| `DonorAccepted` | Donor | Confirms atomic capacity and ownership admission plus the exact accepted parameters. |
| `SessionRejected` | Donor | Rejects a targeted acceptance without creating or retaining session capacity. |
| `RecipientReady` | Recipient | Confirms that every mapping consumer is assigned, positioned, and ready; authorizes delegation. |
| `RecordCompleted` | Recipient | Reports a terminal runtime result for one forwarded destination coordinate. |
| `WarmupCompletionRequested` | Recipient | Closes new delegation admission for all selected donors. |
| `DonorClosed` | Donor | Confirms no admitted record remains without a remote terminal result or required local fallback. |
| `SessionEnded` | Recipient | Announces completed or cold-fallback outcome for diagnostics; it is not a shared tombstone. |

Messages are bounded and versioned. Targeted messages carry exact sender and receiver identities.
Lifecycle messages repeat the immutable session parameters or their fingerprint so mismatched or
forged transitions cannot act on a local session. Duplicates are idempotent only when all immutable
fields match.

## Recipient lifecycle

The context owns one recipient lifecycle:

```text
NEW
  -> CONTROL_READY
  -> DISCOVERING
  -> SELECTING
  -> ADMITTING_DONORS
  -> PREPARING_CONSUMERS
  -> READY
  -> WARMING
  -> DRAINING
  -> STARTING_MAIN
  -> MAIN
```

The steps are:

1. Start one control consumer at the current end of the v6 control topic.
2. Generate a session ID and publish `WarmupRequested` with every required mapping.
3. Collect offers until the discovery deadline.
4. Select exactly one donor for every mapping. Prefer complete coverage and stable ordering; do not
   treat an offer as a reservation.
5. Send targeted acceptances. Each donor atomically checks limits and source ownership before
   replying.
6. Require a complete accepted donor set. One rejection, timeout, or parameter mismatch fails the
   complete context rather than dropping a mapping.
7. Create every warm-up consumer with a session-specific group ID. Independently assign partition
   zero of each destination, seek to the captured current end, and wait for assignment/position
   readiness.
8. Only after all consumers are ready, publish `RecipientReady` for the exact accepted set.
9. Run the normal handler runtime until the recipient's warm-up completion condition fires.
10. Request completion from every donor, keep consumers and result publication active, wait for all
    `DonorClosed` acknowledgements, then stop consumers, flush outcomes, and start main consumers.

No donor may forward before it validates `RecipientReady`. The recipient's captured starting
positions exclude old shared-topic records from useful work; the session filter remains the safety
boundary for old, foreign, racing, or malformed records.

Every pre-main state has a bounded transition to `COLD_FALLBACK`. Cold fallback cancels discovery
and incomplete preparation, closes accepted donor sessions, stops every prepared warm-up consumer,
flushes or abandons bounded outcome work as appropriate, and starts all main consumers only after
local cleanup. Application shutdown transitions to `STOPPED` and never starts main consumers as a
side effect.

## Donor-session lifecycle

Offers are stateless. A local donor session begins only after a targeted acceptance wins atomic
admission:

```text
ABSENT
  -> PREPARING
  -> WAITING_RECIPIENT_READY
  -> ACTIVE
  -> DRAINING
  -> CLOSED
```

An acceptance transaction checks the configured maximum session count, aggregate interval budget,
mapping compatibility, immutable parameters, deadline, main-consumer readiness, and selected source
ownership. Failure returns `SessionRejected` and leaves the session `ABSENT`.

Preparation that finishes after cancellation revalidates the local generation and immediately
cleans up its result instead of installing it. `DonorAccepted` confirms atomic admission and may
therefore be published while donor resource preparation is still running. A valid matching
`RecipientReady` received in `PREPARING` is recorded as a generation-local `readySeen` fact; it is
not rejected and does not activate delegation early. When preparation finishes, one atomic
transition either activates the same generation immediately when `readySeen` is set, or moves it to
`WAITING_RECIPIENT_READY`. If completion, cancellation, expiry, or preparation failure has already
won, cleanup wins and the saved readiness cannot activate the session. A matching readiness received
in `WAITING_RECIPIENT_READY` activates directly. Repeated acceptance/readiness/completion messages
are idempotent and can never revive `DRAINING` or `CLOSED` state.

Completion closes admission before waiting. The donor stops selecting the session for new records,
then lets every admitted record receive a remote terminal result or execute required local
fallback. Only then does it publish `DonorClosed` and release local capacity. Deadline, recipient
loss, transport failure, or lost ability to serve follows the same bounded close path. Source
shutdown, revoke, and ownership loss remain cancellation/handoff: they never execute the handler
locally after ownership has been lost.

Closed session IDs remain in bounded local duplicate fencing long enough to reject late local work,
but there is no replicated tombstone or namespace history. Late messages for a session that is not
locally active are discarded.

## Data plane and fair selection

Delegation stays inside the normal admitted-record runtime seam. Ordering and freshness modes keep
their worker/key/partition slot until the remote decision becomes terminal. Warm-up adds no second
business admission queue.

For each admitted source record:

1. Build the set of active donor sessions whose mapping covers the record, whose recipient is ready,
   whose deadline is live, and whose current quota can admit work.
2. Choose fairly by a per-participant round-robin cursor. A session that temporarily lacks quota is
   skipped without blocking the source record.
3. Offer the record to at most one selected recipient session.
4. If no session is eligible or send admission fails before ownership is transferred, execute the
   ordinary local pipeline.
5. Copy key, value, timestamp, and application headers, replacing any warm-up session header with
   exactly one current session header.
6. After producer acknowledgement, register the full destination coordinate and await its result
   under that session's independent deadline.
7. Map remote success to the source terminal outcome with `processedRemotely=true`. Map a remote
   terminal failure or infrastructure failure to the defined local fallback while ownership is
   still valid.

Each session owns its quota ramp, pending-result tracker, inflight drain, deadline, destinations,
and failure state. Global participant limits cap session count and the sum of configured interval
budgets. Failure or closure of one session detaches only that session. It does not stop other donor
sessions, the main consumer, or locally handled records.

## Recipient early filtering and offsets

Warm-up consumers install an internal `PolledRecordSink` wrapper at this boundary:

```text
Kafka poll
  -> register offset and ownership
  -> validate the single session header
  -> enqueue an owned record into the ordinary processing runtime
  -> invoke the handler
```

A valid local-session record enters the unchanged runtime. A foreign, old, missing, duplicated, or
malformed session header closes only its own offset in the existing tracker and releases its
ownership bookkeeping. Offset advancement still follows the contiguous processed frontier: a
skipped later offset cannot commit past an earlier unfinished local-session record.

Filtered records do not invoke the handler, retries, processing context, failure policy, terminal
audit, `RecordCompleted`, or ordinary processed/drop/failure metrics. Native Kafka/poll metrics do
include every record actually fetched. Filtered records do not occupy runtime queues, key lanes,
partition lanes, or worker capacity. Normal main consumers do not install this filter. A redundant
check immediately before the handler may remain as defense in depth.

Deserialization and the bounded poll-loop stash are intentionally unchanged in GLOBAL-5.

## Results, fallback, and processing guarantees

The recipient reports a result only after the normal runtime reaches a terminal business outcome.
Kafka positions or commits are never interpreted as handler success. Results route by session ID
and the acknowledged destination coordinate.

Remote success terminalizes the donor source record and invokes its metrics callback with
`processedRemotely=true`. The demo audit wrapper may record that actual source completion, while
Micrometer suppresses duplicate ordinary processed, handler-duration, and end-to-end observations
on the donor. The recipient records its actual handler execution normally. Local fallback uses
`processedRemotely=false` and remains ordinary local processing.

Deadline or infrastructure failure executes local fallback only while the source record is still
owned. Shutdown, revoke, assignment loss, or ownership-generation mismatch propagates handoff and
does not perform subsequent local execution. These rules preserve the guarantees of the main
consumer; warm-up transport never acknowledges a source merely because its producer send succeeded.

## Kafka client properties

Library-owned clients apply these defaults:

| Client | Defaults |
| --- | --- |
| Control producer | `linger.ms=0` |
| Forwarding producer | `linger.ms=5` |
| Control consumer | `fetch.min.bytes=1`, `fetch.max.wait.ms=3000` |
| Warm-up consumer | `fetch.min.bytes=1`, `fetch.max.wait.ms=3000` |

Merge order is defaults, inherited/configured user properties, explicit client-specific user
overrides, then protocol-required identity/assignment properties. Thus explicit user tuning wins
over defaults, while the protocol retains final control over unique group/client IDs, manual
assignment behavior, serializers/deserializers required by the transport, and other safety-critical
settings. Input maps and main-consumer settings are never mutated.

## Deadlines and failure isolation

Discovery, donor admission, consumer preparation, active delegation, result waiting, drain, and
cleanup have explicit finite bounds. The implementation may use one overall recipient deadline
plus smaller phase deadlines, but no phase may wait indefinitely. A timeout is a local event; no
observer releases shared ownership because shared ownership does not exist.

Recipient preparation is all-or-nothing across mappings. Failure of one mapping cold-starts the
whole context. Donor failure is session-local: it closes that donor session, performs allowed
fallback for its owned records, and leaves other sessions and the donor main consumer running.
Recipient loss is handled by each donor's local session deadline and close path. A lost donor makes
the recipient finish or cold-fallback within its own deadline; it does not elect a replacement in
the same session.

Destroying the warm-up broker and its data may therefore lose every active warm-up session. That
loss does not stop main consumers and does not revive completed sessions when topics return. New
sessions use only records from the recreated transport. Cleanup that cannot finish within its
graceful Kafka-client deadline uses the bounded forced-close path and remains explicitly incomplete
if resources still cannot be released.

## Explicit exclusions

GLOBAL-5 must not contain or depend on:

- coordinator election, coordinator epochs, or coordinator handoff;
- a replicated namespace session table;
- destination partition reservations or release messages;
- full-state snapshots, snapshot donors, journal synchronization, or control-topic history replay;
- emergency release by third-party observers;
- replicated session retirement, `retiredThrough`, or tombstone compaction;
- bootstrap quarantine after namespace loss;
- exclusive destination allocation or partition-count capacity planning;
- automatic recovery of an old session after all its local owners disappear.

The v6 code and tests should use terminology such as selection, admission, mapping, session, and
close. Names such as reservation, release, snapshot, coordinator, or quarantine are permitted only
in migration documentation and negative assertions.

## Transfer map

Every implementation task records the exact source commits it actually adapts. The initial map is:

| GLOBAL-5 area | GLOBAL-4 source | Treatment |
| --- | --- | --- |
| Terminal runtime outcome seam | `e83dae2d` (`CORE-83`) | Adapt the mode-independent outcome/interceptor seam; preserve all processing modes and current master behavior. |
| Fixed-link data plane | `f013ae6b`, review fix `7e0918cc` (`CORE-84`) | Transfer record copying, acknowledged send, result tracking, deadlines, fallback, cancellation, and tests; replace one fixed link with session selection. |
| Quota ramp | `e76be636` (`CORE-87`) | Transfer per-session initial quota and ramp without global reservation state. |
| Destination result routing | `fc8708c2` (`CORE-88`) | Retain session plus destination-coordinate routing; require unique destination topic names. |
| Control codec and transport | `20f7bead`, review fix `69fa2fc2` (`CORE-85`) | Retain bounded versioned codec, one context consumer, broadcast/target routing, and separated poll/dispatch; define new v6 messages. |
| Start control at current tail | `aa31b88c` (`CORE-89`) | Transfer current-end initialization and tests; remove snapshot/bootstrap synchronization. |
| Library-owned lifecycle | `45464da2`, review fix `72048208` (`CORE-86`) | Retain warm-up-to-main ownership, bounded cold fallback, session-specific consumers, and cleanup race handling. |
| Recipient-driven completion | `005afa9c` (`CORE-90`) | Transfer close-admission, inflight drain, donor acknowledgement, recipient consumer shutdown, and main start ordering. |
| Multiple donor sessions | `dec5ab81`, `9405c1fc`, `d88571f7`, `33ab8172` (`CORE-91`) | Extract atomic capacity checks, independent planes, generation fencing, and activation/close race fixes; do not transfer reservation table operations. |
| Metadata by participant mapping | `ba75bc78`, `5b1eae53` (`CORE-93`) | Transfer participant-connection lookup, common timeout, validation, and credential-safe fallback diagnostics; require exactly one partition. |
| Remote-processing metrics | `07e7fe26` (`CORE-94`) | Transfer `processedRemotely` callback semantics and Micrometer duplicate suppression. |
| Kafka client defaults | `cb9ed640` (`CORE-95`) | Transfer defaults and immutable merge rules, adjusted to v6 required properties. |
| Demo audit and diagnostics | `7483b708` (`DEMO-112`) and `2259bf71`/`7b201c41` (`DEMO-110`) | Transfer audit observation and forwarding diagnostics; replace old DSL and remove coordinator/reservation settings. |
| Independent warm-up transport | `75d0d4ec` (`DEMO-109`) | Adapt separate control/forwarding/recipient Kafka connections without the archived automatic plan. |
| Optional Kubernetes Kafka | `088231fa`, `e3fba567`, `a5284032`, `d37b9fc2`, `ebcd6513` (`INFRA-332` through `INFRA-337`) | Transfer broker, one-partition topic creation, early endpoint, readiness gate, cleanup verification, and evidence. Do not transfer placement or observability already on master. |
| Audit/report analysis | `8cb55c7b`, `2395ecd3` (`INFRA-341`) | Adapt source/warm-up classification and outcome reporting without correlation or reservation dependencies. |
| Experiment definitions | `f7d071fb`, `dd56e37f`, `a631f3fa` (`INFRA-342`) | Use as scenario/evidence references; create fresh v6 namespaces and shared one-partition topics. |

The following GLOBAL-4 work is explicitly reference-only and excluded: reservation ordering and
release portions of `dec5ab81`, `72518ae0`, `202c8a5e`, and `5921ca19`; replicated retirement in
`a4234b21`; `WarmupSessionTable`; snapshot codec/state; emergency release; and safe-no-source
bootstrap quarantine.

## Child tasks and review gates

All IDs are based on the master tracker at GLOBAL-5 creation. Each task branches from the current
umbrella, is reviewed separately, and merges only into the umbrella after approval.

### DOC-11 — architecture and transfer map

This document. Approval freezes the v6 invariants, exclusions, readiness criterion, source baseline,
and initial child-task boundaries. Later discoveries update the document in the task that changes
the contract.

### CORE-74 — multi-session data plane

Implement terminal outcomes, session-aware send/result tracking, independent inflight and deadlines,
quota ramp, round-robin selection, atomic donor limits, ownership-safe fallback/cancellation, and
session-local failure. Port focused unit and Kafka tests with their source citations.

Review gate: one source record reaches at most one recipient; one donor can serve two independent
sessions; one failed session neither stops another session nor changes main-consumer guarantees.

### CORE-75 — early recipient session filter

Add the internal warm-up-only `PolledRecordSink` wrapper and cover interleaved sessions, offset holes,
invalid headers, all processing modes, ownership loss, metrics/audit absence, and queue isolation.

Review gate: foreign records close their own tracked offsets without advancing past unfinished local
records and never enter any business runtime queue.

### CORE-76 — discovery and activation

Implement the v6 codec/transport, stateless offers, mapping selection, atomic donor acceptance,
metadata preparation, explicit consumer assignment/positioning, and `RecipientReady` gate. Add a
deterministic race test in which `RecipientReady` arrives after `DonorAccepted` but before donor
preparation completes; completion of preparation must activate exactly once without another
readiness message. Also cover cancellation or completion winning that race so saved readiness
cannot revive the session.

Review gate: no forwarded record exists before every recipient consumer is ready; early readiness
is retained through donor preparation without early activation; any incomplete mapping preparation
cold-starts the complete context and cleans partial resources.

### CORE-77 — completion and failures

Implement recipient-driven completion, preparation cancellation, late cleanup, terminal fencing,
idempotency, validation, finite deadlines, recipient/donor loss, and parallel-session isolation.

Review gate: closure drains or safely falls back every still-owned record, cannot revive a closed
session, and always releases local donor capacity.

### CORE-78 — Kafka settings and observability

Finalize participant metadata connections, safe diagnostics, Kafka defaults/merge rules,
`processedRemotely`, Micrometer suppression, and low-cardinality warm-up metrics.

Review gate: user overrides beat defaults, required properties remain authoritative, recipient
execution is measured once, donor remote success does not duplicate processed/duration/E2E, and
local fallback remains ordinary local processing.

### CORE-79 — Kafka/runtime integration coverage

Add these acceptance scenarios using real Kafka and the real consumer runtime:

1. one donor warms one recipient;
2. one donor warms two recipients concurrently through one destination partition;
3. several donors serve several recipients;
4. a new recipient joins while an existing warm-up is active;
5. a recipient handles only records for its own session;
6. a late record from an old session never invokes a new recipient's handler;
7. closing a session with non-empty inflight does not block another session;
8. recipient loss causes ownership-safe local donor fallback;
9. loss of donor capability causes bounded close/fallback;
10. after normal and abnormal close, the donor handles new source records;
11. after every participant restarts, the namespace warms again without quarantine;
12. invalid metadata or one failed mapping cold-starts the complete recipient context;
13. remote success does not duplicate donor processed/end-to-end observations.

Assert handlers, source results, offsets, lifecycle, metrics, capacity release, and absence of
terminal consumer failure in every applicable scenario.

Review gate: the full core test matrix passes repeatedly, including total namespace restart without
quarantine and new recipients joining while another session is active.

### DEMO-106 — demo integration

Wire the v6 DSL, independent connections, remote-aware audit wrapper, and forwarding diagnostics.
Remove archived coordinator/reservation settings while preserving handlers and processing profiles.

Review gate: the demo starts cold with unavailable warm-up infrastructure and records actual remote
execution without changing the audit format.

### INFRA-323 — separate warm-up Kafka

Package the pinned official Apache Kafka broker and a continuous topic reconciler as one optional
image under `demo/infra/shared/warmup-kafka`. One supervised container owns the broker process and
the reconciler: either process exiting terminates the container, while shutdown forwards signals to
both under a bounded grace period. `KAFKA_BOOTSTRAP_TOPICS` supplies the complete comma-separated
v6 control/destination list. Reconciliation trims and deduplicates names, creates only missing topics
with one partition and replication factor one, rejects incompatible partition counts, and never
deletes topics or changes existing metadata. Topic lookup uses a quoted literal Kafka pattern and
also verifies the topic name returned by metadata, so punctuation cannot match a different topic.

The broker exposes an internal listener advertised as `localhost` for reconciliation and broker
traffic, plus a separately advertised external listener for applications. Initial topic preparation
therefore never depends on a readiness-gated Kubernetes Service or a host-published Docker port.

Readiness requires live supervised processes plus a fresh complete metadata reconciliation; a stale
marker from an earlier broker lifetime is insufficient. Detection of broker or metadata loss removes
readiness, while successful revalidation restores it. Internal-lab deployment uses a stable Service,
independent application connection properties, ephemeral broker data, a bounded initial readiness
wait, verified labeled cleanup, pod logs, and captured image/topic/readiness evidence. If initial
readiness does not arrive, application deployment proceeds so protocol-v6 cold fallback remains
available. Full data loss creates fresh topics at the same Service address; CORE-83 closes affected
sessions and allows healthy donors to serve later recipients without an application restart.

The selective port uses archived GLOBAL-4 commits `088231fa`, `e3fba567`, `4d05e072`,
`ebcd6513`, and lifecycle reference `dae8a517`. The archived two-container provisioner is not
transferred; INFRA-323 replaces it with the autonomous single-container image and continuous
reconciliation required by the v6 recovery contract. AWS deployment and public image publication
remain out of scope.

Review gate: prepared validation commands pass and exact smoke command is documented; no smoke or
experiment is launched without explicit operator instruction.

### INFRA-324 — experiments and analysis

Prepare cold, one-to-one, one-to-two, early-recipient, lost-recipient, and join-during-session
scenarios with identical infrastructure/load/profile comparisons. Adapt audit/report/dashboard
logic without old-protocol dependencies.

Review gate: every scenario exposes activation/fallback reason, session isolation, audit correctness,
non-duplicated processed/E2E, donor continuity, cleanup, and complete evidence. Separate warm-up Kafka
support on AWS remains a later task.

### DOC-12 — initiative completion

Document the final DSL, cross-cluster examples, lifecycle, deadlines, donor readiness, shared-partition
operational limits, source provenance, verification results, experiment results, and every acceptance
criterion.

Review gate: documentation describes implemented behavior rather than the initial proposal and links
the exact archived commits used by each completed child task.

## Planned validation

Implementation tasks must run the applicable unit and integration tests and record the exact
commands and results for their changed modules. They also run ordinary build, lint, static, shell,
manifest, and Compose validation needed for the change. The initial command families are:

```bash
./gradlew :ckc-core:test
./gradlew :ckc-micrometer:test
./gradlew :ckc-demo:test
```

Start development checks with focused unit or integration test filters and then run the complete
affected-module test commands before review. INFRA tasks likewise run their applicable focused
Python/unit tests, shell syntax checks, manifest checks, and Compose validation. Load tests, chaos
scenarios, internal-lab smoke runs, and other experiment runs are different: prepare and present
their exact commands, but do not launch them without a separate explicit user request.

## Acceptance trace

GLOBAL-5 is complete only when evidence establishes all of the following:

- several recipients warm through shared single-partition topics;
- one donor serves several independent sessions fairly;
- foreign records never reach business handling, terminal audit, or ordinary processing metrics;
- interleaved sessions advance offsets without crossing unfinished earlier work;
- there is no coordinator, global session table, reservation, snapshot, retirement, or quarantine;
- delegation begins only after exact recipient readiness;
- drain, fallback, and ownership-safe handoff remain intact;
- one failed session does not stop other sessions or the main consumer;
- unavailable warm-up produces bounded cold startup with safe diagnostics;
- audit reflects actual executions and donor remote success does not duplicate processed/E2E.
