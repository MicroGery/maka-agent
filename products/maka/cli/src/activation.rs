/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! A one-shot product Host driver. Checkpoint custody and execution authority
//! remain separate from provisional model output.
mod journal;
mod worker;
use journal::Journal;
use maka_agent_api::contract::WorkspacePermissions;
use maka_event_log::checkpoints::{Revision, execution::Permissions, repository::SessionIdentity};
use maka_host_runtime::HostError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{io::Read, path::PathBuf, process::ExitCode, time::Duration};
use uuid::Uuid;

#[derive(clap::Subcommand)]
pub(crate) enum Command {
    /// Read one versioned JSON request from --input or stdin. Owns and closes its Host.
    Run(Run),
    /// Read the original activation journal; never starts a Host or retries input.
    Status {
        #[arg(long)]
        state_root: PathBuf,
    },
    /// Query an original publication using the emitted query reference, without a worker Root.
    Receipt {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long)]
        input: Option<PathBuf>,
    },
}
#[derive(clap::Args)]
pub(crate) struct Run {
    #[arg(long)]
    state_root: PathBuf,
    /// New-session workspace; on resume, parent of the fresh materialized workspace.
    #[arg(long)]
    workspace_root: PathBuf,
    /// Separately initialized configuration Root (maka host init --settings-stdin).
    #[arg(long)]
    config_root: PathBuf,
    /// Persistent reference repository outside every worker's state and workspace.
    #[arg(long)]
    repository: PathBuf,
    #[arg(long)]
    input: Option<PathBuf>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    schema_version: u32,
    activation_id: Uuid,
    agent_id: Uuid,
    start: Start,
    stimulus: Stimulus,
    timeout_millis: u64,
    token_budget: u64,
    permissions: WorkspacePermissions,
    ceiling: Permissions,
    tools: Vec<String>,
    /// This reference adapter cannot freeze unrelated editors or processes.
    accept_external_writers_risk: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Start {
    New,
    Resume {
        identity: SessionIdentity,
        revision: Revision,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Stimulus {
    Message { text: String },
    Schedule { text: String },
    System { text: String },
}
impl Stimulus {
    fn text(&self) -> &str {
        match self {
            Self::Message { text } | Self::Schedule { text } | Self::System { text } => text,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum Outcome {
    Completed {
        checkpoint: Value,
    },
    Blocked {
        reason: BlockedReason,
        #[serde(skip_serializing_if = "Option::is_none")]
        checkpoint: Option<Value>,
    },
    RetryableFailure {
        reason: String,
    },
    FatalFailure {
        reason: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BlockedReason {
    ExecutionBusy,
    HeadChanged,
    InteractionRequired,
    RecoveryRequired,
    CleanupUnconfirmed,
}
impl Outcome {
    fn exit_code(&self) -> ExitCode {
        match self {
            Self::Completed { .. } => ExitCode::SUCCESS,
            Self::Blocked { .. } => ExitCode::from(3),
            _ => ExitCode::FAILURE,
        }
    }
}
impl Request {
    fn validate(&self) -> Result<(), HostError> {
        if self.schema_version != 1
            || self.activation_id.is_nil()
            || self.agent_id.is_nil()
            || !(1_000..=600_000).contains(&self.timeout_millis)
            || self.token_budget == 0
            || self.token_budget > 10_000_000
            || self.stimulus.text().trim().is_empty()
            || self.stimulus.text().len() > 262_144
            || !self.accept_external_writers_risk
            || self.permissions.request_permissions
            || self.tools.len() > 256
            || self.tools.iter().any(|name| {
                name.is_empty()
                    || name.len() > 256
                    || self
                        .ceiling
                        .tools
                        .as_ref()
                        .is_some_and(|allowed| !allowed.contains(name))
            })
            || self
                .tools
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.tools.len()
        {
            return Err("invalid_activation_request".into());
        }
        if let Start::Resume { identity, revision } = &self.start
            && (identity.agent != self.agent_id || identity.session.is_nil() || revision.0.is_nil())
        {
            return Err("activation_identity_mismatch".into());
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReceiptRead {
    schema_version: u32,
    grant: maka_event_log::checkpoints::execution::Grant,
    operation: Uuid,
}
async fn read_json<T: serde::de::DeserializeOwned>(path: Option<PathBuf>) -> Result<T, HostError> {
    let (send, receive) = tokio::sync::oneshot::channel();
    // A stalled input pipe must not keep Tokio's shutdown pool alive.
    std::thread::Builder::new()
        .name("maka-activation-input".into())
        .spawn(move || {
            let read = || {
                let mut bytes = Vec::new();
                let reader: Box<dyn Read> = match path {
                    Some(path) => {
                        let file = std::fs::File::open(path)?;
                        if !file.metadata()?.is_file() {
                            return Err("activation_input_not_regular".into());
                        }
                        Box::new(file)
                    }
                    None => Box::new(std::io::stdin()),
                };
                reader.take(1_048_577).read_to_end(&mut bytes)?;
                if bytes.len() > 1_048_576 {
                    return Err("activation_input_too_large".into());
                }
                Ok::<_, HostError>(bytes)
            };
            let _ = send.send(read());
        })?;
    let bytes = tokio::time::timeout(Duration::from_secs(10), receive).await???;
    Ok(serde_json::from_slice(&bytes)?)
}
impl Command {
    pub(crate) async fn run(self) -> Result<ExitCode, HostError> {
        match self {
            Self::Status { state_root } => {
                let record = Journal::read(state_root).await?;
                let (outcome, cleanup_confirmed) = worker::observed_outcome(&record).await?;
                emit(json!({"schemaVersion":1,"type":"status","activationId":record.request.activation_id,
                    "binding":record.binding,"phase":record.phase,"cleanupConfirmed":cleanup_confirmed,"outcome":outcome})).await?;
                Ok(ExitCode::SUCCESS)
            }
            Self::Receipt { repository, input } => {
                use maka_event_log::checkpoints::{
                    execution::ExecutionAuthority, repository::files::FileRepository,
                };
                let query: ReceiptRead = read_json(input).await?;
                if query.schema_version != 1 || query.operation.is_nil() {
                    return Err("invalid_receipt_query".into());
                }
                let repository =
                    tokio::task::spawn_blocking(move || FileRepository::open(&repository))
                        .await??;
                let publication = repository
                    .original_commit(std::sync::Arc::new(query.grant), query.operation)
                    .await?;
                emit(json!({"schemaVersion":1,"type":"commit_receipt","publication":publication}))
                    .await?;
                Ok(ExitCode::SUCCESS)
            }
            Self::Run(args) => run(args).await,
        }
    }
}
async fn run(args: Run) -> Result<ExitCode, HostError> {
    let input: Result<Request, HostError> = read_json(args.input.clone()).await;
    let input = input.and_then(|request| {
        request.validate()?;
        Ok(request)
    });
    let request = match input {
        Ok(request) => request,
        Err(_) => {
            let outcome = Outcome::FatalFailure {
                reason: "invalid_request".into(),
            };
            emit(json!({"schemaVersion":1,"type":"terminal","outcome":outcome})).await?;
            return Ok(outcome.exit_code());
        }
    };
    emit(json!({"schemaVersion":1,"type":"started","activationId":request.activation_id})).await?;
    let activation = request.activation_id;
    let outcome = match worker::execute(args, request).await {
        Ok(outcome) => outcome,
        // Provider and filesystem diagnostics can contain credentials or input. The
        // external protocol reports a fixed vocabulary, never arbitrary error text.
        Err(_) => Outcome::FatalFailure {
            reason: "activation_start_failed".into(),
        },
    };
    emit(json!({"schemaVersion":1,"type":"terminal","activationId":activation,"outcome":outcome}))
        .await?;
    Ok(outcome.exit_code())
}
async fn emit(value: Value) -> Result<(), HostError> {
    let text = serde_json::to_string(&value)?;
    if text.len() > 1_048_576 {
        return Err("activation_event_too_large".into());
    }
    let stop = tokio_util::sync::CancellationToken::new();
    let timer = stop.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        timer.cancel();
    });
    let result = crate::run::write_stdout(text, &stop).await;
    task.abort();
    result
}
