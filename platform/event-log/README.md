<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# maka-event-log

[简体中文](README.zh-CN.md)

`maka-event-log` owns durable State Root storage: the exclusive OS process lock (`RootOwner`), the SQLite database (`RootStore`), two-phase Host operation journals, and immutable checkpoint publication. Higher-level Agent facts and projections register migrations on top of `RootStore` from [`maka-agent-state`](../../plugins/agent/state/src/lib.rs).

## Key modules

- [`root.rs`](src/root.rs) & [`root_store.rs`](src/root_store.rs) — Acquire the exclusive State Root lock and coordinate graceful shutdown with admitted in-flight operations.
- [`connection.rs`](src/connection.rs) & [`writeback.rs`](src/writeback.rs) — Run a single serialized SQLite writer FIFO alongside concurrent snapshot reader threads, publishing scoped invalidation signals only after commits succeed.
- [`root_store/data.rs`](src/root_store/data.rs) — Root- and Session-scoped SQL statements that let native services commit domain state atomically alongside a Host operation receipt.
- [`applications.rs`](src/applications.rs) — Persistence for deployment sources, caller intent records, and provider operation receipts (`Outcome` + physical `Cleanup`).
- [`checkpoints/`](src/checkpoints.rs) — Content-addressed workspace & history checkpoint publication, CAS Head updates, and crash-safe restore settlement.
- [`schema.rs`](src/schema.rs) & [`migration.rs`](src/migration.rs) — Base Root schema and composed domain migration verification.

## Checkpoint repositories

`checkpoints::repository` decouples immutable blob storage from logical Session bindings and CAS Head commits:

- **Default local authority (`RootStore::open`)** — Stores checkpoint Heads in the Root SQLite database and promotes verified staging files directly via zero-copy local links after `fsync`.
- **External repository authority (`RootStore::open_with_checkpoint_storage`)** — Accepts a pluggable `Storage` pair. `FileRepository::open` provides a multi-process local-filesystem reference adapter arbitrated by its own SQLite database outside worker Roots.

Exact checkout (`Storage::checkout_exact`) verifies all object digests and workspace closure against the persisted logical Session binding before returning the current Head, without granting execution authority.

## Execution authority

`checkpoints::execution::ExecutionAuthority` is a trusted control-plane port, separate from the immutable object store. Its grant binds a logical Session, stable activation ID, generation, exact starting Head and sandbox/tool ceilings. `claim` and `renew` replay their original durable result for the same request, including rejection; an old successful receipt does not mean the grant is active now. Use `observe` for current status.

A trusted embedding claims the grant, calls `Execution::attach`, then supplies `Storage::with_execution` to `RootStore::open_with_checkpoint_storage` before opening the Host. A regular claim binds one native Session in its existing Root/deployment/namespace; a planned cross-Root continuation uses `claim_handoff` below. A Root remembers its execution configuration and cannot reopen as unrestricted local storage. Opting in is allowed for a Root with no local checkpoint identity or publication, or an existing shared repository with no unfinished publication. Changing an established grant requires a separate activation/migration flow.

The mutation dispatcher rejects foreign-Session writes and Root-wide or cross-Session mutations (including create/fork, goals, schedules and executor setup) in managed mode. Root-wide producers remain disabled; recovery filters foreign work and checks captured permissions before reserving a Run or consuming queued input. Authorization refusal leaves the original Run facts and queue unchanged. The native Owner checks the grant at message/run admission and at actual model/tool authorization, rather than on each configuration read. Tool selection and sandbox permission, including separately approved filesystem/network access, are intersections with the grant ceiling. Restoring a Run is rejected when its captured or approvable sandbox exceeds that ceiling. External executor backends are not admitted in this first increment. Host observation runs every 250 ms; each observation has a five-second deadline. Revocation, expiry or an observation failure cancels local work irreversibly and starts Host shutdown. This is cooperative cancellation of admitted work, not instantaneous revocation of remote side effects. An authority adapter must authenticate callers and provide linearizable decisions; possession of serialized grant fields is not a bearer credential.

`FileRepository` also provides a durable single-machine reference coordinator. Its generation check, expected-Head CAS and original publication receipt share one SQL transaction. Once a Session has been managed, plain repository commits cannot bypass execution checks. Replaying a committed operation after revocation returns its original receipt without writing again. Active observations are read-only; observing expiry rechecks under a write transaction before persisting the fence. The reference adapter is not a distributed lease service and makes no automatic takeover promise based on wall-clock expiry.

For planned handoff the supervisor calls `Host::stop_execution`. The Host closes transports, cancels/drains plugins and managed processes, and awaits Root/SQL cleanup before acknowledging `Stopped`. Cleanup failures remain failures on repeated shutdown. Revocation and the final control-plane acknowledgement each have a five-second deadline. Only that acknowledgement permits another generation; timeout, lease expiry, loss of contact or a failed cleanup leave takeover blocked. An external `Environment` adapter must cover the original environment and all managed writers, not merely kill a parent PID. Unmanaged editors or remote third-party effects require their own isolation/settlement; this interface does not make an unverified Workspace snapshot consistent.

Default local Hosts remain outside this optional execution mode. These Rust ports introduce no cloud vendor, Kubernetes, CLI configuration or automatic checkpoint trigger.

## Planned cross-Root continuation

1. A managed source publishes an exact checkpoint including its Workspace at a quiescent boundary. The native snapshot records continuation eligibility while source admission is held; Goal, Bot, schedules, checklist or saved cell state, Room/parent/child relationships, unfinished work and external executor history are not silently discarded. History-only, older or inspection-only snapshots remain readable, without acquiring continuation eligibility on import.
2. The supervisor stops and drains the source, then calls `ExecutionAuthority::claim_handoff` with the retained publisher binding and a fresh target Root/deployment/namespace/Session binding. The logical Session and agent stay unchanged. The coordinator requires the exact retained Head and all previous execution generations to be stopped. It registers the target and grant atomically, keeping the Head's publisher provenance unchanged until the target publishes its successor.
3. Trusted assembly opens the target Host with that grant. `prepareCheckpoint` imports verified canonical history and materializes an isolated child directory, but creates no live Session, model call or pending-message admission. It records the original directory identity and checkpoint. A restarted target reuses and verifies that preparation; edits, replaced directories, incomplete copies and occupied Session IDs fail without overwrite.
4. `activateCheckpoint` requires the committed preparation reference and explicit target model, tools and permissions within the grant. Target configuration, inert historical adoption, a startup activation marker and the original Host operation result commit together. A handoff grant cannot bootstrap an existing target Session without that marker, so startup recovery cannot run unrelated preexisting work. It neither replays an old tool nor automatically sends a message. A subsequent explicit `send` uses the imported history, and `publishCheckpoint` advances the same logical Head under the target generation.

Workspace consistency remains caller-confirmed or unverified; activation requires explicit acceptance of external-writer risk. Neither the preparation fence nor hash verification excludes other applications. Source credentials, dependency installations, global Memory, Goals and cross-Session orchestration are not transferred. This path continues the selected checkpoint, not changes made after it; the supervisor must coordinate a planned handoff and provide any required target environment. It does not implement a cloud scheduler, automatic failover, UI, arbitrary history rewind or mutation of an already populated target Session.

## Testing

```sh
cargo nextest run -p maka-event-log
```
