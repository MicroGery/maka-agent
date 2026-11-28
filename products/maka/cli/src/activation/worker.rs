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

use super::journal::{Operation, Paths};
use super::*;
use maka_agent_api::contract::{
    CheckpointReceipt, Created, ExecutionState, HistoryCheckpointInventory, HistoryCheckpointState,
    SendResult, Work, WorkReadiness,
};
use maka_event_log::{
    checkpoints::{
        Scope,
        execution::{Claim, ClaimResult, Execution, ExecutionAuthority, HandoffClaim},
        repository::{
            Binding, CommitReceipt, SessionRepository, Source, Storage, files::FileRepository,
        },
    },
    root::{RootNamespaces, RootOwner},
};
use maka_host_runtime::{
    DirectoryRootSpec,
    application::{
        host::{Client, Configuration, Host},
        transport::Reply,
    },
};
use maka_plugin_core::{
    operation::{OperationCleanup, OperationOutcome, OperationResult, Receipt},
    plan::Provider,
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct RequestRejected(String);
impl std::fmt::Display for RequestRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for RequestRejected {}

pub(super) async fn observed_outcome(
    record: &journal::Record,
) -> Result<(Option<Outcome>, bool), HostError> {
    let Some(operation) = record.operations.get("publishCheckpoint") else {
        return Ok((record.outcome.clone(), record.cleanup_confirmed));
    };
    let grant = Arc::new(record.grant.clone().ok_or("activation_grant_missing")?);
    let path = record.paths.repository.clone();
    let repository = tokio::task::spawn_blocking(move || FileRepository::open(&path)).await??;
    let cleanup_confirmed = record.cleanup_confirmed
        || matches!(
            repository.observe(grant.clone()).await?,
            maka_event_log::checkpoints::execution::Status::Stopped
        );
    let outcome = match repository
        .original_commit(grant, operation.reference.operation)
        .await?
    {
        Some(CommitReceipt::Published { head }) if cleanup_confirmed => Some(Outcome::Completed {
            checkpoint: serde_json::to_value(head)?,
        }),
        Some(CommitReceipt::Published { head }) => Some(Outcome::Blocked {
            reason: BlockedReason::CleanupUnconfirmed,
            checkpoint: Some(serde_json::to_value(head)?),
        }),
        Some(CommitReceipt::Conflict { .. }) => Some(Outcome::Blocked {
            reason: BlockedReason::HeadChanged,
            checkpoint: None,
        }),
        None if matches!(record.outcome, Some(Outcome::Completed { .. })) => {
            Some(Outcome::Blocked {
                reason: BlockedReason::RecoveryRequired,
                checkpoint: None,
            })
        }
        None => record.outcome.clone(),
    };
    Ok((outcome, cleanup_confirmed))
}
pub(super) async fn execute(args: Run, request: Request) -> Result<Outcome, HostError> {
    let (owner, config_owner, paths) = tokio::task::spawn_blocking(move || {
        let namespaces = RootNamespaces::for_current_account()?;
        let workspace = args.workspace_root.canonicalize()?;
        let configuration = args.config_root.canonicalize()?;
        // Initialize only an empty disposable State Root. Never adopt other local Sessions.
        let owner = if args.state_root.exists() {
            if !args.state_root.join("activation.json").is_file() {
                return Err("activation_requires_new_state_root".into());
            }
            RootOwner::open(&args.state_root, &namespaces)?
        } else {
            RootOwner::create(&args.state_root, &namespaces)?
        };
        std::fs::create_dir_all(&args.repository)?;
        let repository = args.repository.canonicalize()?;
        let roots = [
            owner.canonical_path(),
            workspace.as_path(),
            configuration.as_path(),
            repository.as_path(),
        ];
        for (i, a) in roots.iter().enumerate() {
            for b in &roots[i + 1..] {
                if a.starts_with(b) || b.starts_with(a) {
                    return Err("activation_roots_overlap".into());
                }
            }
        }
        let config_owner = RootOwner::open_existing(&configuration, &namespaces)?;
        Ok::<_, HostError>((
            Arc::new(owner),
            Arc::new(config_owner),
            Paths {
                workspace,
                configuration,
                repository,
            },
        ))
    })
    .await??;
    let (mut journal, fresh) = Journal::open(owner.clone(), request.clone(), paths).await?;
    if journal.record.outcome.is_some() {
        return observed_outcome(&journal.record)
            .await?
            .0
            .ok_or_else(|| "activation_outcome_missing".into());
    }
    let repository_path = journal.record.paths.repository.clone();
    let repository = Arc::new(
        tokio::task::spawn_blocking(move || FileRepository::open(&repository_path)).await??,
    );
    let (loaded, selection) = if journal.record.grant.is_none() {
        let store =
            maka_transport::open_store(owner.clone(), &maka_product_native::root_migrations())
                .await?;
        let prepared = crate::application::prepare_saved(
            &store,
            crate::application::Roots::Configured(Some(vec![DirectoryRootSpec {
                label: "Activation workspace".into(),
                path: journal.record.paths.workspace.clone(),
            }])),
        )
        .await;
        let cleanup = store.shutdown().await;
        cleanup?;
        prepared?
    } else {
        // Loaded only below, after restoring the original execution capability.
        // Do not reopen a managed Root with an unmanaged default repository.
        return resume(journal, config_owner, repository).await;
    };
    let Configuration::PublicHost { endpoints, .. } = &selection;
    let endpoint = endpoints
        .get("agent")
        .ok_or("activation_agent_endpoint_missing")?;
    let consumer = loaded
        .plan()
        .instances()
        .get(&endpoint.consumer)
        .ok_or("activation_consumer_missing")?;
    let [binding] = consumer
        .bindings
        .get(&endpoint.import)
        .ok_or("activation_binding_missing")?
        .as_slice()
    else {
        return Err("activation_agent_binding_ambiguous".into());
    };
    let Provider::Local { instance, .. } = &binding.provider else {
        return Err("activation_agent_is_remote".into());
    };
    let source = loaded.source().reference.clone();
    let prefix =
        maka_agent_authority::session_namespace(&source.state_domain, source.deployment, instance);
    let target = Source {
        root_id: owner.root_id().into(),
        scope: Scope {
            state_domain: source.state_domain,
            deployment: source.deployment,
            namespace: instance.clone(),
        },
        session: format!("{prefix}{}", request.activation_id.simple()),
    };
    let (binding, predecessor, revision) = match &request.start {
        Start::New => (repository.bind(target, request.agent_id).await?, None, None),
        Start::Resume { identity, revision } => {
            let Some(current) = repository.read_head(identity.clone()).await? else {
                return terminal(
                    &mut journal,
                    Outcome::Blocked {
                        reason: BlockedReason::HeadChanged,
                        checkpoint: None,
                    },
                )
                .await;
            };
            if current.head.revision != *revision {
                return terminal(
                    &mut journal,
                    Outcome::Blocked {
                        reason: BlockedReason::HeadChanged,
                        checkpoint: None,
                    },
                )
                .await;
            }
            (
                Binding {
                    identity: identity.clone(),
                    source: target,
                },
                Some(current.binding),
                Some(*revision),
            )
        }
    };
    journal.record.binding = Some(binding.clone());
    journal.save().await?;
    let claim = Claim {
        activation: request.activation_id,
        binding,
        start: revision,
        permissions: request.ceiling.clone(),
        lease_millis: request.timeout_millis + 60_000,
    };
    let result = match predecessor {
        Some(source) => {
            repository
                .claim_handoff(HandoffClaim {
                    source,
                    target: claim,
                })
                .await?
        }
        None => repository.claim(claim).await?,
    };
    let grant = match result {
        ClaimResult::Granted { grant, .. } => *grant,
        ClaimResult::Busy => {
            return terminal(
                &mut journal,
                Outcome::Blocked {
                    reason: BlockedReason::ExecutionBusy,
                    checkpoint: None,
                },
            )
            .await;
        }
        ClaimResult::Conflict { .. } => {
            return terminal(
                &mut journal,
                Outcome::Blocked {
                    reason: BlockedReason::HeadChanged,
                    checkpoint: None,
                },
            )
            .await;
        }
    };
    journal.record.grant = Some(grant);
    journal.save().await?;
    // Application preparation never starts a producer. The first actual Host starts
    // only after its exact scope, native Session and execution grant are durable.
    start(
        journal,
        config_owner,
        repository,
        Some((loaded, selection)),
        fresh,
    )
    .await
}

async fn resume(
    journal: Journal,
    config: Arc<RootOwner>,
    repository: Arc<FileRepository>,
) -> Result<Outcome, HostError> {
    start(journal, config, repository, None, false).await
}
async fn start(
    mut journal: Journal,
    config: Arc<RootOwner>,
    repository: Arc<FileRepository>,
    prepared: Option<(
        maka_event_log::applications::LoadedDeployment,
        Configuration,
    )>,
    fresh: bool,
) -> Result<Outcome, HostError> {
    let grant = Arc::new(
        journal
            .record
            .grant
            .clone()
            .ok_or("activation_grant_missing")?,
    );
    if !fresh {
        if let Some(outcome) = committed(repository.as_ref(), &journal).await?
            && matches!(
                repository.observe(grant.clone()).await?,
                maka_event_log::checkpoints::execution::Status::Stopped
            )
        {
            journal.record.cleanup_confirmed = true;
            return terminal(&mut journal, outcome).await;
        }
        // Restarting arbitrary unfinished work could replay external effects. Only a
        // publication submitted after a completed Run has a safe reconciliation path.
        if !journal.record.operations.contains_key("publishCheckpoint") {
            return terminal(
                &mut journal,
                Outcome::Blocked {
                    reason: BlockedReason::RecoveryRequired,
                    checkpoint: None,
                },
            )
            .await;
        }
    }
    let execution = match Execution::attach(repository.clone(), grant.clone()).await {
        Ok(execution) => execution,
        Err(_) => {
            return terminal(
                &mut journal,
                Outcome::Blocked {
                    reason: BlockedReason::RecoveryRequired,
                    checkpoint: None,
                },
            )
            .await;
        }
    };
    let storage = Storage::new(
        repository.objects(),
        repository.clone(),
        grant.binding.identity.agent,
    )?
    .with_execution(execution)?;
    let store = Arc::new(
        maka_transport::open_store_with_checkpoints(
            journal.owner.clone(),
            &maka_product_native::root_migrations(),
            storage,
        )
        .await?,
    );
    let (loaded, selection) = match prepared {
        Some(prepared) => prepared,
        None => {
            let launch = store
                .application_launch()
                .await?
                .ok_or("activation_application_missing")?;
            let loaded = store
                .load_application(launch.deployment)
                .await?
                .ok_or("activation_application_missing")?;
            (loaded, serde_json::from_value(launch.configuration)?)
        }
    };
    let at: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let resources = maka_agent_native::Resources {
        budget: Some(maka_agent_facts::durable::Budget {
            tokens: Some(journal.record.request.token_budget),
            retries: 0,
            deadline: Some(maka_agent_facts::durable::UnixMillis(
                (at + journal.record.request.timeout_millis) as i64,
            )),
        }),
        initial_permissions: Some(journal.record.request.permissions.clone()),
        ..Default::default()
    };
    let mut product =
        crate::application::open_activation_host(store, loaded, selection, config, resources)
            .await?;
    let cancel = CancellationToken::new();
    let result = async {
        let endpoint = crate::endpoint::LocalEndpoint::bind()?;
        let socket = endpoint.path.clone();
        let serving = tokio::spawn(serve(endpoint, product.host.clone(), cancel.clone()));
        let work = async {
            let mut client = Client::connect(&socket, Some(product.host.epoch())).await?;
            let interrupted = CancellationToken::new();
            let _signals = crate::signals::watch(interrupted.clone())?;
            let result = tokio::select! {
                biased;
                _ = interrupted.cancelled() => Ok(Ok(Outcome::RetryableFailure { reason: "interrupted".into() })),
                result = tokio::time::timeout(Duration::from_millis(journal.record.request.timeout_millis), async {
                    if fresh { drive(&client, &mut journal).await } else { recover(&client, &mut journal).await }
                }) => result,
            };
            let closed = client.close().await;
            closed?;
            Ok::<_, HostError>(match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => match error.downcast_ref::<RequestRejected>().map(|error|error.0.as_str()) {
                    Some("agent_checkpoint_permissions_exceed_grant") => Outcome::FatalFailure { reason: "permissions_exceed_grant".into() },
                    Some("agent_checkpoint_tools_unavailable") => Outcome::FatalFailure { reason: "tools_unavailable".into() },
                    Some("agent_checkpoint_backend_unavailable") => Outcome::FatalFailure { reason: "backend_unavailable".into() },
                    Some("agent_checkpoint_source_not_independently_resumable") => Outcome::Blocked { reason: BlockedReason::RecoveryRequired, checkpoint: None },
                    Some("agent_checkpoint_busy") => Outcome::RetryableFailure { reason: "checkpoint_busy".into() },
                    _ => Outcome::RetryableFailure { reason: "runtime_or_publication_failed".into() },
                },
                Err(_) => Outcome::RetryableFailure { reason: "timeout".into() },
            })
        }.await;
        cancel.cancel();
        let joined = serving.await?;
        joined?;
        work
    }.await;
    // Stopped evidence must include the catalog as well as Host/plugin/process/SQL
    // cleanup. A failed cleanup leaves this generation non-transferable.
    let cleanup = product.stop_execution().await;
    journal.record.cleanup_confirmed = cleanup.is_ok();
    let committed = committed(repository.as_ref(), &journal).await;
    let outcome = match (cleanup, committed) {
        (Ok(()), Ok(Some(outcome))) => outcome,
        (Ok(()), Ok(None)) => result.unwrap_or(Outcome::RetryableFailure {
            reason: "host_transport_failed".into(),
        }),
        (Ok(()), Err(_)) => Outcome::RetryableFailure {
            reason: "commit_observation_unavailable".into(),
        },
        (Err(_), receipt) => Outcome::Blocked {
            reason: BlockedReason::CleanupUnconfirmed,
            checkpoint: match receipt {
                Ok(Some(Outcome::Completed { checkpoint })) => Some(checkpoint),
                _ => None,
            },
        },
    };
    terminal(&mut journal, outcome).await
}
async fn serve(
    endpoint: crate::endpoint::LocalEndpoint,
    host: Arc<Host>,
    stop: CancellationToken,
) -> Result<(), HostError> {
    // Owning the complete endpoint keeps its private directory until IPC drains.
    host.serve_local(endpoint.listener, stop)
        .await
        .map_err(Into::into)
}
async fn committed(
    authority: &dyn ExecutionAuthority,
    journal: &Journal,
) -> Result<Option<Outcome>, HostError> {
    let Some(operation) = journal.record.operations.get("publishCheckpoint") else {
        return Ok(None);
    };
    let grant = Arc::new(
        journal
            .record
            .grant
            .clone()
            .ok_or("activation_grant_missing")?,
    );
    let receipt = authority
        .original_commit(grant, operation.reference.operation)
        .await?;
    Ok(receipt.map(|receipt| match receipt {
        CommitReceipt::Published { head } => Outcome::Completed {
            checkpoint: serde_json::to_value(head).expect("checkpoint head is JSON"),
        },
        CommitReceipt::Conflict { .. } => Outcome::Blocked {
            reason: BlockedReason::HeadChanged,
            checkpoint: None,
        },
    }))
}
async fn terminal(journal: &mut Journal, outcome: Outcome) -> Result<Outcome, HostError> {
    journal.record.phase = "terminal".into();
    journal.record.outcome = Some(outcome.clone());
    journal.save().await?;
    Ok(outcome)
}
async fn drive(client: &Client, journal: &mut Journal) -> Result<Outcome, HostError> {
    let request = journal.record.request.clone();
    let session = journal
        .record
        .binding
        .as_ref()
        .ok_or("activation_binding_missing")?
        .source
        .session
        .clone();
    let backend = crate::run::default_backend(client).await?;
    let path = maka_fs_tools::workspace::project::host_path(&journal.record.paths.workspace)?;
    match &request.start {
        Start::New => {
            let result = mutate(client, journal, "create", json!({"title":"Cloud Session","labels":[],"workspace":{"kind":"directory","path":path},
                "backend":backend,"tools":request.tools})).await?;
            let Created::Created { session: created } = serde_json::from_value(result)? else {
                return Err("activation_creation_rejected".into());
            };
            if created.id != session {
                return Err("activation_created_session_mismatch".into());
            }
        }
        Start::Resume { .. } => {
            let input = json!({"sessionId":session,"parent":path});
            mutate(client, journal, "prepareCheckpoint", input.clone()).await?;
            let preparation = journal.record.operations["prepareCheckpoint"]
                .reference
                .clone();
            mutate(client, journal, "activateCheckpoint", json!({"sessionId":session,"preparation":preparation,"preparedInput":input,
                "title":"Cloud Session","permissions":request.permissions,"backend":backend,"tools":request.tools,"acceptExternalWritersRisk":true})).await?;
        }
    }
    emit(json!({"schemaVersion":1,"type":"session_ready","activationId":request.activation_id,"binding":journal.record.binding,"resumed":matches!(request.start,Start::Resume {..})})).await?;
    let result = mutate(
        client,
        journal,
        "send",
        json!({"sessionId":session,"message":request.stimulus.text(),"delivery":"queue"}),
    )
    .await?;
    let SendResult::Queued { message_id, .. } = serde_json::from_value(result)? else {
        return Err("activation_message_rejected".into());
    };
    loop {
        let work: Work = crate::run::query(
            client,
            "agent",
            "work",
            json!({"sessionId":session,"target":{"kind":"message","messageId":message_id}}),
        )
        .await?;
        match work {
            Work::Delivered {
                pending_interactions,
                ..
            } if pending_interactions != 0 => {
                return Ok(Outcome::Blocked {
                    reason: BlockedReason::InteractionRequired,
                    checkpoint: None,
                });
            }
            Work::Delivered {
                execution, answer, ..
            } => match execution.state {
                ExecutionState::Completed => {
                    // Run completion may precede tool/process settlement. This is
                    // readiness only; publication still acquires the physical fence.
                    loop {
                        match crate::run::query::<WorkReadiness>(client, "agent", "workReadiness",
                            json!({"sessionId":session,"target":{"kind":"message","messageId":message_id}})).await? {
                            WorkReadiness::Ready => break,
                            WorkReadiness::Pending => tokio::time::sleep(Duration::from_millis(25)).await,
                            WorkReadiness::Unavailable => return Ok(Outcome::Blocked { reason: BlockedReason::RecoveryRequired, checkpoint: None }),
                        }
                    }
                    if let Some(answer) = answer {
                        emit(json!({"schemaVersion":1,"type":"answer","activationId":request.activation_id,"provisional":true,"text":answer.text,"truncated":answer.next.is_some()})).await?;
                    }
                    break;
                }
                ExecutionState::Failed { .. } | ExecutionState::Cancelled => {
                    return Ok(Outcome::RetryableFailure {
                        reason: "execution_failed".into(),
                    });
                }
                ExecutionState::Waiting {
                    authority_unavailable: true,
                    ..
                } => {
                    return Ok(Outcome::Blocked {
                        reason: BlockedReason::RecoveryRequired,
                        checkpoint: None,
                    });
                }
                _ => {}
            },
            Work::Cancelled | Work::Missing => {
                return Ok(Outcome::Blocked {
                    reason: BlockedReason::RecoveryRequired,
                    checkpoint: None,
                });
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    emit(json!({"schemaVersion":1,"type":"runtime_completed","activationId":request.activation_id,"provisional":true})).await?;
    let preview: HistoryCheckpointInventory = crate::run::query(
        client,
        "agent",
        "previewHistoryCheckpoint",
        json!({"sessionId":session}),
    )
    .await?;
    let expected = match request.start {
        Start::New => None,
        Start::Resume { revision, .. } => Some(revision.0),
    };
    mutate(client, journal, "publishCheckpoint", json!({"sessionId":session,"expectedRevision":expected,
        "confirmedSubtreeDigest":preview.subtree_digest,"workspaceConsistency":"caller_confirmed_quiescent"})).await?;
    recover(client, journal).await
}
async fn recover(client: &Client, journal: &mut Journal) -> Result<Outcome, HostError> {
    // Never replay a stimulus or prepare a replacement publication. Only consult
    // the exact original operation that this caller durably held before confirm.
    let Some(operation) = journal.record.operations.get("publishCheckpoint") else {
        return Ok(Outcome::Blocked {
            reason: BlockedReason::RecoveryRequired,
            checkpoint: None,
        });
    };
    let receipt: CheckpointReceipt = crate::run::query(
        client,
        "agent",
        "checkpointReceipt",
        json!({"reference":operation.reference,"input":operation.input}),
    )
    .await?;
    if let Some(publication) = receipt.publication {
        match publication.state {
            HistoryCheckpointState::Published { head } => {
                return Ok(Outcome::Completed {
                    checkpoint: serde_json::to_value(head)?,
                });
            }
            HistoryCheckpointState::Conflict { .. } => {
                return Ok(Outcome::Blocked {
                    reason: BlockedReason::HeadChanged,
                    checkpoint: None,
                });
            }
            _ => {}
        }
    }
    Ok(Outcome::Blocked {
        reason: BlockedReason::RecoveryRequired,
        checkpoint: None,
    })
}
async fn mutate(
    client: &Client,
    journal: &mut Journal,
    method: &str,
    input: Value,
) -> Result<Value, HostError> {
    if journal.record.operations.contains_key(method) {
        return Err("activation_operation_already_started".into());
    }
    let mut call = client
        .call("agent", method.into(), input.clone(), Default::default())
        .await?;
    call.start().await?;
    let Reply::Prepared { reference } = call.receive().await? else {
        call.close().await?;
        return Err("activation_operation_not_prepared".into());
    };
    journal.record.phase = method.into();
    journal.record.operations.insert(
        method.into(),
        Operation {
            input,
            reference: *reference,
            result: None,
        },
    );
    if let Err(error) = journal.save().await {
        call.close().await?;
        return Err(error);
    }
    if method == "publishCheckpoint" {
        let query = ReceiptRead {
            schema_version: 1,
            grant: journal
                .record
                .grant
                .clone()
                .ok_or("activation_grant_missing")?,
            operation: journal.record.operations[method].reference.operation,
        };
        emit(json!({"schemaVersion":1,"type":"publication_prepared","activationId":journal.record.request.activation_id,"query":query})).await?;
    }
    let mut reply = call.confirm(Value::Null).await?;
    loop {
        match reply {
            Reply::Progress { .. } => reply = call.receive().await?,
            Reply::Mutation { reply, cleanup } => {
                cleanup?;
                if let Receipt::Recorded {
                    outcome:
                        OperationOutcome::Returned {
                            result: OperationResult::Failure { diagnostic },
                        },
                    cleanup: OperationCleanup::Confirmed,
                } = &reply.receipt
                {
                    return Err(RequestRejected(diagnostic.clone()).into());
                }
                let Receipt::Recorded {
                    outcome:
                        OperationOutcome::Returned {
                            result: OperationResult::Success { value },
                        },
                    cleanup: OperationCleanup::Confirmed,
                } = reply.receipt
                else {
                    return Err("activation_operation_not_confirmed".into());
                };
                journal
                    .record
                    .operations
                    .get_mut(method)
                    .expect("saved original operation")
                    .result = Some(value.clone());
                journal.save().await?;
                return Ok(value);
            }
            _ => {
                call.close().await?;
                return Err("activation_operation_unresolved".into());
            }
        }
    }
}
