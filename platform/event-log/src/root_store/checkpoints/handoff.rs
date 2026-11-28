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

//! Exact cross-Root preparation. No Session, queue admission or executable Run is created.
use super::*;
use crate::checkpoints::execution::Grant;
use cap_fs_ext::DirExt;
use maka_sandbox::workspace::DirectoryIdentity;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    pub operation: Uuid,
    pub fingerprint: String,
    pub parent: PathBuf,
    pub parent_identity: DirectoryIdentity,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedHandoff {
    pub head: Head,
    pub manifest: Manifest,
    pub path: PathBuf,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    grant: Grant,
    request: Prepare,
}

/// Verified material (or its durable completed preparation) bound to this exact
/// Root, grant and request. Callers cannot manufacture it from archive metadata.
pub struct HandoffStart {
    log: Arc<EventLog>,
    identity: Identity,
    head: Head,
    manifest: Manifest,
    snapshot: Option<std::fs::File>,
}
impl HandoffStart {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn take_snapshot(&mut self) -> Option<std::fs::File> {
        self.snapshot.take()
    }
}

impl RootStore {
    /// A grant binds the target to its own actual Root. The retained checkpoint
    /// continues to name its original publisher until a new checkpoint commits.
    pub async fn checkout_execution_start(
        &self,
        request: &Prepare,
    ) -> Result<HandoffStart, StoreError> {
        self.require_open()?;
        let storage = self
            .log
            .checkpoint
            .as_ref()
            .and_then(|a| a.storage())
            .ok_or_else(|| invalid("shared checkpoint authority required"))?;
        let execution = storage
            .execution
            .as_ref()
            .ok_or_else(|| invalid("execution grant required"))?;
        execution.check().await?;
        let grant = execution.grant();
        let source = grant
            .handoff
            .as_ref()
            .ok_or_else(|| invalid("handoff grant required"))?;
        let revision = grant
            .start
            .ok_or_else(|| invalid("execution has no checkpoint start"))?;
        let identity = Identity {
            grant: grant.clone(),
            request: request.clone(),
        };
        let key = request.operation.to_string();
        let prior = self.log.connection.read(move |db| {
            Ok(db.query_row("SELECT request_json,prepared_json,directory_identity_json FROM checkpoint_handoffs WHERE operation=?",
                [&key], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?))).optional()?)
        }).await?;
        let (head, manifest, snapshot) = if let Some((original, prepared, directory)) = prior {
            if serde_json::from_str::<Identity>(&original)? != identity || directory.is_none() {
                return Err(invalid(
                    "handoff preparation incomplete or identity conflict",
                ));
            }
            let prepared: PreparedHandoff = serde_json::from_str(&prepared)?;
            let retained = storage
                .sessions
                .read_head(grant.binding.identity.clone())
                .await?
                .ok_or_else(|| invalid("checkpoint revision unavailable"))?;
            if retained.binding != *source
                || retained.head != prepared.head
                || retained.head.revision != revision
            {
                return Err(invalid("checkpoint revision unavailable"));
            }
            (prepared.head, prepared.manifest, None)
        } else {
            let checkout = storage
                .checkout_exact(grant.binding.identity.clone(), revision)
                .await?;
            (checkout.head, checkout.manifest, Some(checkout.snapshot))
        };
        if manifest.root_id != source.source.root_id
            || manifest.scope != source.source.scope
            || manifest.session_id != source.source.session
            || source.identity != grant.binding.identity
            || source.source.root_id == grant.binding.source.root_id
        {
            return Err(invalid("checkpoint differs from handoff source"));
        }
        Ok(HandoffStart {
            log: self.log.clone(),
            identity,
            head,
            manifest,
            snapshot,
        })
    }

    /// Repeated preparation verifies and reuses the original directory; it never
    /// silently repairs edits or replaces a partial/foreign directory. The caller
    /// retains the returned fence until its activation write has settled.
    pub async fn prepare_checkpoint_handoff(
        &self,
        request: Prepare,
        parent: &RootWorkspace,
        start: HandoffStart,
    ) -> Result<(PreparedHandoff, WorkspaceFence), StoreError> {
        let owner = self.owned_root()?;
        parent.validate(&owner)?;
        if request.operation.is_nil()
            || !crate::checkpoints::valid_digest(&request.fingerprint)
            || !request.parent.is_absolute()
        {
            return Err(invalid("invalid checkpoint handoff request"));
        }
        let execution = self
            .checkpoint_execution()
            .ok_or_else(|| invalid("execution grant required"))?
            .clone();
        if !Arc::ptr_eq(&start.log, &self.log)
            || start.identity.request != request
            || start.identity.grant != *execution.grant()
        {
            return Err(invalid("handoff checkout authority mismatch"));
        }
        let identity = start.identity;
        let parent = parent.clone();
        let physical = Arc::new(std::sync::OnceLock::<WorkspaceFence>::new());
        let log = self.log.scoped(physical.clone());
        let gates = self.gates.clone();
        let result = worker(log, gates.clone(), move |log, objects| async move {
            let request = &identity.request;
            let directory = parent.directory();
            if !request.parent_identity.matches(directory)? {
                return Err(invalid("handoff parent directory changed"));
            }
            workspace::validate_alias(directory, &request.parent)?;
            let parent_path = request.parent.canonicalize()?;
            workspace::validate_alias(directory, &parent_path)?;
            if parent_path.starts_with(objects.owner.canonical_path()) {
                return Err(invalid("handoff destination overlaps private Root"));
            }
            let operation = request.operation.to_string();
            let expected = serde_json::to_string(&identity)?;
            let session = identity.grant.binding.source.session.clone();
            let key = operation.clone();
            let prior = log.connection.read(move |db| {
                let occupied: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM session_control WHERE id=?1) OR EXISTS(SELECT 1 FROM session_creations WHERE session_id=?1)", [&session], |r|r.get(0))?;
                if occupied { return Err(invalid("handoff target Session already exists")); }
                Ok(db.query_row("SELECT request_json,prepared_json,directory_identity_json FROM checkpoint_handoffs WHERE operation=?", [&key], |r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?))).optional()?)
            }).await?;
            let name = std::ffi::OsString::from(format!("maka-handoff-{}", request.operation));
            let slot = super::super::workspace_fence::ChildSlot::reserve(&gates, &objects.owner, directory.try_clone()?, name.clone())?;
            physical.set(slot.fence()).map_err(|_|invalid("handoff fence already captured"))?;
            let prepared = PreparedHandoff {
                head: start.head,
                manifest: start.manifest,
                path: parent_path.join(&name),
            };
            if let Some((original, data, inode)) = prior {
                if original != expected || serde_json::from_str::<PreparedHandoff>(&data)? != prepared {
                    return Err(invalid("handoff preparation identity conflict"));
                }
                let inode: DirectoryIdentity = serde_json::from_str(&inode.ok_or_else(||invalid("handoff preparation incomplete"))?)?;
                slot.bind_existing(&gates, directory.open_dir_nofollow(&name)?)?;
                let fence = slot.fence();
                let root = fence.directory()?;
                if !inode.matches(root)? { return Err(invalid("handoff destination replaced")); }
                verify_workspace(&objects, &prepared.manifest, root)?;
                return Ok((prepared, fence));
            }
            let data = serde_json::to_string(&prepared)?;
            let key = operation.clone();
            log.connection.run(move |db| {
                db.execute("INSERT INTO checkpoint_handoffs(operation,request_json,prepared_json) VALUES(?,?,?)", rusqlite::params![key,expected,data])?;
                Ok(())
            }).await?;
            slot.create(&gates)?;
            let fence = slot.fence();
            let destination = fence.directory()?;
            if let Some(snapshot) = &prepared.manifest.workspace {
                workspace::materialize_in(&objects, snapshot, destination)?;
            }
            directory.open(".")?.sync_all()?;
            let inode = serde_json::to_string(&DirectoryIdentity::capture(destination)?)?;
            log.connection.run(move |db| {
                let changed = db.execute("UPDATE checkpoint_handoffs SET directory_identity_json=? WHERE operation=? AND directory_identity_json IS NULL", rusqlite::params![inode,operation])?;
                if changed != 1 { return Err(invalid("handoff preparation changed")); }
                Ok(())
            }).await?;
            Ok((prepared, fence))
        }).await?;
        let storage = self
            .log
            .checkpoint
            .as_ref()
            .and_then(|a| a.storage())
            .ok_or_else(|| invalid("shared checkpoint authority required"))?;
        let retained = storage
            .sessions
            .read_head(execution.grant().binding.identity.clone())
            .await?
            .ok_or_else(|| invalid("checkpoint revision unavailable"))?;
        if retained.head != result.0.head
            || Some(&retained.binding) != execution.grant().handoff.as_ref()
        {
            return Err(invalid("checkpoint revision unavailable"));
        }
        execution.check().await?;
        Ok(result)
    }
}
fn verify_workspace(
    objects: &Objects,
    manifest: &Manifest,
    root: &cap_std::fs::Dir,
) -> Result<(), StoreError> {
    if let Some(snapshot) = &manifest.workspace {
        super::restore::verify_workspace(objects, snapshot, root)
    } else if root.entries()?.next().is_some() {
        Err(invalid("empty handoff workspace changed"))
    } else {
        Ok(())
    }
}

/// Composed with native Session creation and the Host receipt on the same writer.
/// This marker is also the startup gate for recovering a handed-off Session.
pub struct Activation {
    log: Arc<EventLog>,
    grant: Grant,
    preparation: Uuid,
    operation: Uuid,
}
impl RootStore {
    pub fn prepare_checkpoint_activation(
        &self,
        writer: &DataWriter,
        preparation: Uuid,
        operation: Uuid,
    ) -> Result<Activation, StoreError> {
        self.require_open()?;
        self.validate_data_writer(writer)?;
        if operation.is_nil() || preparation.is_nil() {
            return Err(invalid("invalid checkpoint activation identity"));
        }
        let grant = self
            .checkpoint_execution()
            .ok_or_else(|| invalid("execution grant required"))?
            .grant()
            .clone();
        if grant.handoff.is_none() {
            return Err(invalid("handoff grant required"));
        }
        Ok(Activation {
            log: self.log.clone(),
            grant,
            preparation,
            operation,
        })
    }
}
impl crate::writeback::Statement for Activation {
    type Value = ();
    type Output = ();
    fn retain(&self) -> Vec<Arc<dyn Send + Sync>> {
        vec![self.log.clone()]
    }
    fn execute(
        self,
        db: &rusqlite::Connection,
    ) -> Result<crate::writeback::Prepared<()>, StoreError> {
        self.log.connection.require_same_writer(db)?;
        let row: Option<(String,Option<String>,Option<String>)> = db.query_row(
            "SELECT request_json,directory_identity_json,activated_operation FROM checkpoint_handoffs WHERE operation=?",
            [self.preparation.to_string()], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let (request, directory, activated) =
            row.ok_or_else(|| invalid("checkpoint preparation missing"))?;
        if serde_json::from_str::<Identity>(&request)?.grant != self.grant
            || directory.is_none()
            || activated
                .as_ref()
                .is_some_and(|a| a != &self.operation.to_string())
        {
            return Err(invalid("checkpoint activation identity conflict"));
        }
        let installed: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_control WHERE id=?)",
            [&self.grant.binding.source.session],
            |r| r.get(0),
        )?;
        if !installed {
            return Err(invalid("checkpoint target Session not installed"));
        }
        let changed = db.execute("UPDATE checkpoint_handoffs SET activated_operation=? WHERE operation=? AND activated_operation IS NULL",rusqlite::params![self.operation.to_string(),self.preparation.to_string()])?;
        Ok(crate::writeback::Prepared {
            value: (),
            changed: changed != 0,
            notification: crate::writeback::Notification::NONE,
        })
    }
    fn committed(_: ()) {}
    fn isolated_rejection(_: &StoreError) -> bool {
        false
    }
}
