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

//! Planned continuation in a fresh Root. Imported history remains inert until
//! explicit target configuration is atomically installed, and never resumes an old Run.
use super::*;
fn diagnostic(error: impl std::fmt::Display) -> String {
    error.to_string()
}
use maka_agent_facts::{
    durable::UnixMillis,
    history_snapshot::{Continuation, Origin},
};
use maka_agent_state::session_history::snapshot::{Adoption, Snapshot};
use maka_event_log::{
    root_store::{WorkspaceFence, handoff},
    writeback::{Prepared, Statement},
};

impl Owner {
    fn handoff_operation(
        &self,
        session: &str,
        operation: &RecoveryRef,
        method: &str,
    ) -> Result<(), String> {
        operation.validate().map_err(diagnostic)?;
        if !self.owns(session)
            || operation.method != method
            || operation.export != maka_agent_api::EXPORT
            || operation.source.state_domain != self.data_source.state_domain
            || operation.source.deployment != self.data_source.deployment
            || operation.namespace != self.data_source.namespace
        {
            return Err("agent_checkpoint_handoff_authority_mismatch".into());
        }
        let grant = self
            .store
            .checkpoint_execution()
            .ok_or("agent_checkpoint_execution_required")?
            .grant();
        if grant.binding.source.session != session {
            return Err("agent_checkpoint_target_mismatch".into());
        }
        Ok(())
    }

    async fn prepare_handoff(
        &self,
        input: &CheckpointPrepare,
        operation: &RecoveryRef,
        scope: &Scope,
    ) -> Result<(handoff::PreparedHandoff, Arc<WorkspaceFence>, Snapshot), String> {
        self.handoff_operation(&input.session_id, operation, "prepareCheckpoint")?;
        let root = self.store.owned_root().map_err(diagnostic)?;
        let parent = workspace::observed(
            self.workspace_roots.clone(),
            &input.parent,
            self.home.as_deref(),
            root.canonical_path().to_owned(),
        )
        .await?
        .ok_or("agent_directory_unavailable")?;
        let path = std::path::PathBuf::from(&parent.path);
        if path.starts_with(root.control_directory()) || path.starts_with(&self.managed_worktrees) {
            return Err("agent_directory_unavailable".into());
        }
        let directory =
            workspace::open_directory(&self.workspace_roots, &parent, root.canonical_path())
                .await?;
        let observed = directory.try_clone().map_err(diagnostic)?;
        let parent_identity = tokio::task::spawn_blocking(move || {
            maka_sandbox::workspace::DirectoryIdentity::capture(&observed)
        })
        .await
        .map_err(diagnostic)?
        .map_err(diagnostic)?;
        let parent = self.store.workspace(directory).await.map_err(diagnostic)?;
        let request = handoff::Prepare {
            operation: operation.operation,
            fingerprint: hex::encode(Sha256::digest(
                serde_json::to_vec(operation).map_err(diagnostic)?,
            )),
            parent: path,
            parent_identity,
        };
        let mut start = self
            .store
            .checkout_execution_start(&request)
            .await
            .map_err(diagnostic)?;
        let manifest = start.manifest().clone();
        let state = self.checkpoint_state()?;
        let history = state.session_history(self.run_owner(), input.session_id.clone());
        let source = match history
            .imported_snapshot(
                operation.operation,
                &manifest.history,
                scope.cancellation.child_token(),
            )
            .await
            .map_err(diagnostic)?
        {
            Some(source) => source,
            None => history
                .import_snapshot(
                    operation.operation,
                    manifest.history.clone(),
                    start
                        .take_snapshot()
                        .ok_or("agent_checkpoint_prepared_history_unavailable")?,
                    scope.cancellation.child_token(),
                )
                .await
                .map_err(diagnostic)?,
        };
        let captured = source.manifest().await.map_err(diagnostic)?;
        let eligible = matches!(&captured.origin, Origin::Canonical { cut }
            if cut.session == manifest.session_id
            && cut.owner.deployment == manifest.scope.deployment
            && cut.owner.namespace == manifest.scope.namespace);
        if !eligible
            || captured.continuation != Some(Continuation::OrdinaryNativeV1)
            || manifest.workspace.is_none()
        {
            return Err("agent_checkpoint_source_not_independently_resumable".into());
        }
        let (prepared, fence) = self
            .store
            .prepare_checkpoint_handoff(request, &parent, start)
            .await
            .map_err(diagnostic)?;
        Ok((prepared, Arc::new(fence), source))
    }

    pub async fn prepare_checkpoint(
        self: &Arc<Self>,
        input: CheckpointPrepare,
        operation: &RecoveryRef,
        scope: &Scope,
    ) -> Result<CheckpointPrepared, String> {
        management(scope)?;
        let operation = operation.clone();
        self.resource_work(scope, move |owner, scope| {
            Box::pin(async move {
                let (prepared, _fence, _source) =
                    owner.prepare_handoff(&input, &operation, &scope).await?;
                let logical_session_id = owner
                    .store
                    .checkpoint_execution()
                    .ok_or("agent_checkpoint_execution_required")?
                    .grant()
                    .binding
                    .identity
                    .session;
                Ok(CheckpointPrepared {
                    operation: operation.operation,
                    session_id: input.session_id,
                    logical_session_id,
                    head: head(prepared.head),
                    path: prepared.path.to_string_lossy().into_owned(),
                    workspace: prepared.manifest.workspace.map(workspace_snapshot),
                })
            })
        })
        .await
    }

    pub async fn activate_checkpoint(
        self: &Arc<Self>,
        input: CheckpointActivate,
        operation: &RecoveryRef,
        scope: &Scope,
    ) -> Result<NativeMutation<CheckpointActivated>, String> {
        management(scope)?;
        let operation = operation.clone();
        self.resource_work(scope, move |owner, scope| {
            Box::pin(async move {
                owner.handoff_operation(&input.session_id, &operation, "activateCheckpoint")?;
                // Host completion is authoritative even after later turns changed the workspace.
                if let Some(observed) = owner
                    .store
                    .operation(&operation)
                    .map_err(diagnostic)?
                    .operation_observation()
                    .await
                    .map_err(diagnostic)?
                {
                    use maka_plugin_core::operation::{OperationOutcome, OperationResult};
                    match observed.outcome {
                        OperationOutcome::Returned {
                            result: OperationResult::Success { value },
                        } => {
                            return Ok(NativeMutation::Returned(
                                serde_json::from_value(value).map_err(diagnostic)?,
                            ));
                        }
                        OperationOutcome::Unresolved => {}
                        _ => return Err("agent_checkpoint_activation_outcome_unavailable".into()),
                    }
                }
                if input.session_id != input.prepared_input.session_id
                    || !input.accept_external_writers_risk
                {
                    return Err("agent_checkpoint_activation_configuration_invalid".into());
                }
                owner.handoff_operation(
                    &input.session_id,
                    &input.preparation,
                    "prepareCheckpoint",
                )?;
                let preparation = owner
                    .store
                    .operation(&input.preparation)
                    .map_err(diagnostic)?
                    .operation_observation()
                    .await
                    .map_err(diagnostic)?
                    .ok_or("agent_checkpoint_preparation_not_committed")?;
                use maka_plugin_core::operation::{OperationOutcome, OperationResult};
                let OperationOutcome::Returned {
                    result: OperationResult::Success { value },
                } = preparation.outcome
                else {
                    return Err("agent_checkpoint_preparation_not_committed".into());
                };
                let receipt: CheckpointPrepared =
                    serde_json::from_value(value).map_err(diagnostic)?;
                if receipt.operation != input.preparation.operation
                    || receipt.session_id != input.session_id
                {
                    return Err("agent_checkpoint_preparation_mismatch".into());
                }
                if metadata_rejection(&input.title, &[]).is_some() {
                    return Err("agent_checkpoint_title_invalid".into());
                }
                if !matches!(input.backend, BackendPreference::Model { .. })
                    || !owner.backend_available(&input.backend).await?
                {
                    return Err("agent_checkpoint_backend_unavailable".into());
                }
                let execution = owner
                    .store
                    .checkpoint_execution()
                    .ok_or("agent_checkpoint_execution_required")?
                    .clone();
                let grant = execution.grant();
                if input.tools.iter().any(|name| {
                    !owner.tools.contains(name)
                        || grant
                            .permissions
                            .tools
                            .as_ref()
                            .is_some_and(|allowed| !allowed.contains(name))
                }) {
                    return Err("agent_checkpoint_tools_unavailable".into());
                }
                let ordered = Arc::new(
                    owner
                        .runs
                        .try_ordered(&input.session_id)
                        .map_err(|_| "agent_checkpoint_busy")?,
                );
                let (prepared, fence, source) = owner
                    .prepare_handoff(&input.prepared_input, &input.preparation, &scope)
                    .await?;
                if receipt.head.revision != prepared.head.revision.0
                    || receipt.logical_session_id != grant.binding.identity.session
                    || std::path::Path::new(&receipt.path) != prepared.path
                {
                    return Err("agent_checkpoint_preparation_mismatch".into());
                }
                let writer = owner.store.data_writer().map_err(diagnostic)?;
                let workspace = Workspace {
                    path: prepared
                        .path
                        .to_str()
                        .ok_or("agent_directory_unavailable")?
                        .to_owned(),
                    project_id: None,
                    worktree: None,
                };
                let directory = workspace::open_workspace(
                    &owner.workspace_roots,
                    &workspace,
                    &owner.managed_worktrees,
                )
                .await?;
                let observed = owner.store.workspace(directory).await.map_err(diagnostic)?;
                owner
                    .store
                    .validate_workspace_fence(&fence, &observed)
                    .await
                    .map_err(diagnostic)?;
                let (candidate, workspace_statement) = owner
                    .store
                    .prepare_creation_workspace(&owner.prefix, move |_| workspace, None, &writer)
                    .await
                    .map_err(diagnostic)?;
                let configuration = StoredSessionConfiguration {
                    incognito: false,
                    parent_session_id: None,
                    backend: Some(input.backend),
                    applied_preset: None,
                    compaction_prompt: None,
                    collaboration_mode: CollaborationMode::Agent,
                    tools: Some(input.tools),
                    title: input.title,
                    title_is_manual: true,
                    pinned: false,
                    labels: vec![],
                    workspace: candidate.reference(),
                    permissions: input.permissions,
                    boundary_revision: 0,
                };
                let resolved = configuration
                    .clone()
                    .resolve(
                        serde_json::from_value(candidate.workspace.clone()).map_err(diagnostic)?,
                        candidate.stamp(),
                    )
                    .map_err(diagnostic)?;
                // Reject an excessive requested base policy before Run capture clips
                // it to the execution ceiling. Approval requests still stay within
                // that ceiling when later Runs capture their effective permissions.
                let requested = owner
                    .requested_sandbox(&resolved, scope.sandbox_floor())
                    .await?;
                if !grant
                    .permissions
                    .sandbox
                    .contains(&requested)
                    .map_err(diagnostic)?
                {
                    return Err("agent_checkpoint_permissions_exceed_grant".into());
                }
                let (spec, contents) = owner
                    .history_source_spec(&input.session_id, &resolved, &source, &operation, &scope)
                    .await?;
                let history = owner
                    .checkpoint_state()?
                    .session_history(owner.run_owner(), input.session_id.clone());
                let at = now();
                let adoption = source
                    .adopt(
                        &history,
                        operation.operation,
                        spec,
                        UnixMillis(at as i64),
                        contents,
                    )
                    .map_err(diagnostic)?;
                let planned = maka_agent_state::session_catalog::SessionCatalog::new(
                    writer.clone(),
                    owner.run_owner(),
                    owner.prefix.clone(),
                )
                .map_err(diagnostic)?
                .planned_adoption(&adoption)
                .await
                .map_err(diagnostic)?;
                let claim = maka_event_log::sessions::PluginSession {
                    session_id: input.session_id,
                    creator: maka_plugin_core::storage::Namespace::new(
                        IMPLEMENTATION,
                        maka_plugin_core::scope::Scope::Profile,
                    )
                    .map_err(diagnostic)?,
                    fingerprint: operation.fingerprint.clone(),
                    authority_session_id: None,
                };
                let statement = owner
                    .store
                    .prepare_plugin_session_with_workspace(
                        &claim,
                        &serde_json::to_value(configuration).map_err(diagnostic)?,
                        at,
                        &writer,
                        workspace_statement,
                        adoption,
                    )
                    .await
                    .map_err(diagnostic)?;
                execution.check().await.map_err(diagnostic)?;
                // A concurrent revoke can stop this activation, but never authorizes a send:
                // all later execution still passes the generation/lease admission gate.
                let activation_record = owner
                    .store
                    .prepare_checkpoint_activation(
                        &writer,
                        input.preparation.operation,
                        operation.operation,
                    )
                    .map_err(diagnostic)?;
                let change = writer
                    .prepare_result(|_| {
                        Ok(Activate {
                            statement,
                            activation_record,
                            planned,
                            source: owner.data_source.clone(),
                            fence,
                            ordered,
                            logical_session_id: grant.binding.identity.session,
                            activation: grant.activation,
                            stop: execution.cancellation(),
                        })
                    })
                    .map_err(diagnostic)?;
                Ok(NativeMutation::Computed { change })
            })
        })
        .await
    }
}
struct Activate<S> {
    statement: S,
    activation_record: handoff::Activation,
    planned: maka_agent_state::session_catalog::PlannedCatalog,
    source: DataSource,
    fence: Arc<WorkspaceFence>,
    ordered: Arc<runs::Ordered>,
    logical_session_id: Uuid,
    activation: Uuid,
    stop: tokio_util::sync::CancellationToken,
}
#[derive(Serialize)]
#[serde(transparent)]
struct ActivationResult<T> {
    reply: CheckpointActivated,
    #[serde(skip)]
    inner: T,
}
impl<S> Statement for Activate<S>
where
    S: Statement<Value = (SessionRecord, Adoption)>,
{
    type Value = ActivationResult<S::Value>;
    type Output = (Value, S::Output);
    const PREPARE_OFF_THREAD: bool = S::PREPARE_OFF_THREAD;
    fn retain(&self) -> Vec<Arc<dyn std::marker::Send + Sync>> {
        let mut retained = self.statement.retain();
        retained.extend(self.activation_record.retain());
        retained.push(self.fence.clone());
        retained.push(self.ordered.clone());
        retained
    }
    fn prepare(self) -> Result<Self, StoreError> {
        Ok(Self {
            statement: self.statement.prepare()?,
            ..self
        })
    }
    fn execute(self, c: &rusqlite::Connection) -> Result<Prepared<Self::Value>, StoreError> {
        if self.stop.is_cancelled() {
            return Err(StoreError::ConnectionClosed);
        }
        let written = self.statement.execute(c)?;
        self.activation_record.execute(c)?;
        let record = self.planned.record(written.value.0.clone())?;
        let session = crate::service::summary_for(&self.source, record)
            .map_err(StoreError::InvalidTransition)?;
        Ok(Prepared {
            value: ActivationResult {
                reply: CheckpointActivated {
                    session: Box::new(session),
                    logical_session_id: self.logical_session_id,
                    activation: self.activation,
                },
                inner: written.value,
            },
            changed: written.changed,
            notification: written.notification,
        })
    }
    fn isolated_rejection(error: &StoreError) -> bool {
        S::isolated_rejection(error)
    }
    fn committed(value: Self::Value) -> Self::Output {
        (
            serde_json::to_value(value.reply).expect("serializable activation reply"),
            S::committed(value.inner),
        )
    }
}
