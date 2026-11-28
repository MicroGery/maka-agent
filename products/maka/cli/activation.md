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

# One-shot Cloud Session activation

`maka activation run` starts the shipped product Host, admits one explicit input,
waits for a terminal Run, publishes a Workspace checkpoint, and stops its execution
generation. It reuses the product composition, Agent service, model configuration,
tools, operation journal, and checkpoint ports. It has no Kubernetes dependency.

The CLI supplies the Unix `FileRepository` reference adapter. Production embeddings
may supply their own `SessionRepository`, immutable object store and
`ExecutionAuthority`; an imported archive or serialized grant alone is never an
execution capability.

## Invoke

Initialize a separate configuration Root with the existing
`maka host init --root /config --settings-stdin` command. Configure its default
model there. Provider credentials stay in that Root; they are neither copied into
the worker State Root nor adopted from checkpoint history. The embedding holds
that configuration owner's lease until the Host and catalog have closed. Give
concurrent workers independent injected configuration Roots.

```sh
maka activation run \
  --state-root /workers/a/state \
  --workspace-root /workers/a/project \
  --config-root /workers/a/config \
  --repository /durable/checkpoints \
  --input /requests/activation.json
```

Omit `--input` to read JSON from stdin. Arguments contain paths only. Input is
limited to 1 MiB and must arrive within 10 seconds; stimulus text is limited to
256 KiB. Roots must be independent, with no ancestor/descendant overlap. The State Root must be absent on first invocation; an existing Root must belong
to the same recorded activation. Ordinary desktop
Roots with existing Sessions are not an activation bootstrap input.

Example for a worker whose surrounding environment supplies isolation:

```json
{
  "schemaVersion": 1,
  "activationId": "f57447e3-42d7-4715-a2ab-dd6b87da1723",
  "agentId": "2e22433d-5cbc-47d1-ab14-f611537290bf",
  "start": { "kind": "new" },
  "stimulus": { "kind": "message", "text": "Continue the investigation." },
  "timeoutMillis": 60000,
  "tokenBudget": 10000,
  "permissions": { "access": "danger_full_access", "requestPermissions": false },
  "ceiling": { "sandbox": { "kind": "disabled" }, "tools": [] },
  "tools": [],
  "acceptExternalWritersRisk": true
}
```

`disabled` does not establish OS isolation. The trusted launcher can instead
supply a restricted `managed` sandbox ceiling. Requested base permissions and
explicit tools must fit that ceiling. Interactive permission escalation is not
available in this protocol. The token budget uses the existing Run budget; the
wall-clock execution budget is 1–600 seconds, with no automatic model retries.
Resource shutdown follows execution and must be confirmed before success.

`message`, `schedule`, and `system` identify the incoming stimulus in the request
journal. They all enter through ordinary `agent.send`; the latter two do not grant
System-prompt, scheduler, or tool authority.

## Planned continuation

Use a fresh State Root, workspace parent and activation ID for each successor.
Replace `start` with the exact identity and revision returned by the preceding
activation:

```json
{
  "kind": "resume",
  "identity": {
    "session": "<logical Session UUID>",
    "agent": "<the same Agent UUID>"
  },
  "revision": "<committed checkpoint UUID>"
}
```

For resume, `--workspace-root` is the parent of a newly materialized directory.
The source Root and source project are not needed. The authority must confirm
that the predecessor has stopped and that this is still the retained revision.
The existing prepare/activate management operations install historical context
without restarting its old Run. Only this activation's explicit stimulus starts
new work. Publication advances the same logical Session Head conditionally.

The portable closure is currently an ordinary native Session plus Workspace.
Unsupported dependent state is rejected by checkpoint eligibility checks. In particular,
a Code Mode tool call can save tool-discovery preferences: its checkpoint remains
a valid archive, but it is currently rejected for cross-Root continuation. Ordinary
text-only completed Sessions can be continued; this command does not expand that
PR3 eligibility boundary. Memory,
Goal, WorkHub, arbitrary crash takeover, and external side-effect reconciliation
are not added by this command. The explicit external-writer acknowledgment means
the caller keeps editors and other writers quiet throughout capture; hashing does
not turn a changing directory into an OS snapshot.

## JSONL output and original results

Every event has `schemaVersion: 1`. Valid input emits `started`, then bounded
`session_ready`, `answer`, `runtime_completed` and `publication_prepared` events as
those phases occur, followed by one `terminal` event when output is available.
Malformed input emits a single fatal terminal. The first bounded answer page is
provisional; `truncated` signals that the full answer remains in Session history.
No streamed answer or `runtime_completed` event claims a committed Head.

The terminal `outcome.status` is one of:

- `completed`: the exact original checkpoint commit was observed and catalog,
  Host, native processes, SQL and execution-generation cleanup were confirmed.
- `blocked`: `execution_busy`, `head_changed`, `interaction_required`,
  `recovery_required`, or `cleanup_unconfirmed` requires orchestration action.
  An observed commit may accompany unconfirmed cleanup; it does not authorize a
  successor execution.
- `retryable_failure`: a bounded execution, transport or commit-observation failure.
  The caller must query the original result and reconcile any effects before
  deciding how to proceed. It must not blindly replay input under a new ID.
- `fatal_failure`: invalid input, identity or startup configuration.

`completed` exits 0, `blocked` exits 3, failures exit 1. Ordinary diagnostics go to
stderr. User text and provider error details are not copied into protocol errors.
A SIGTERM/SIGINT during execution requests cleanup; killing the process cannot
produce a terminal event or manufacture `Stopped` evidence.

The caller's `activation.json` stores original operation references **before**
confirmation. Repeating the exact request returns the original outcome and never
sends a second message. Changing input, paths or identity conflicts. An unfinished
execution is not automatically retried; only an already-submitted publication
can be reconciled at its safe boundary.

```sh
maka activation status --state-root /workers/a/state
```

Before publishing, `publication_prepared.query` supplies a small original receipt
query containing the grant data and operation ID. Persist that query in the
orchestrator before discarding the worker. It is an observation reference, not a
bearer credential. Even after the worker Root is deleted and the Head advances:

```sh
maka activation receipt --repository /durable/checkpoints --input /receipts/a.json
```

This returns the exact original `publication` receipt (or null), never the latest
Head and never a replacement commit. It does not claim worker cleanup. A lost
response or null observation is not permission to send another stimulus.

## Verification

`products/maka/cli/tests/integration/activation.rs` uses actual product CLI
processes and a persistent coordinator/object directory outside their Roots.
The provider is a local mock; the Host, journals, storage and handoff are real.
Run it through the existing CLI integration suite. The storage execution suite
also checks original receipts after Head movement and generation mismatch.
The CLI cases cover worker death before commit, loss of the commit response,
restricted permissions, timeout cleanup, pending interaction, and duplicate input.
The upgrade capsule keeps a completed activation journal and re-queries its original
commit with a later binary without calling the obsolete model endpoint.
