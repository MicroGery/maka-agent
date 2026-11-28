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

use super::*;
use maka_event_log::{
    checkpoints::{execution::Grant, repository::Binding},
    root::RootOwner,
};
use maka_plugin_core::operation::RecoveryRef;
use std::{collections::BTreeMap, sync::Arc};
const FILE: &str = "activation.json";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Paths {
    pub workspace: PathBuf,
    pub configuration: PathBuf,
    pub repository: PathBuf,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub input: Value,
    pub reference: RecoveryRef,
    pub result: Option<Value>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub request: Request,
    pub paths: Paths,
    pub binding: Option<Binding>,
    pub grant: Option<Grant>,
    pub phase: String,
    pub operations: BTreeMap<String, Operation>,
    pub outcome: Option<Outcome>,
    pub cleanup_confirmed: bool,
}
pub(super) struct Journal {
    pub owner: Arc<RootOwner>,
    pub record: Record,
}
impl Journal {
    pub async fn open(
        owner: Arc<RootOwner>,
        request: Request,
        paths: Paths,
    ) -> Result<(Self, bool), HostError> {
        let original = owner.clone();
        let saved = tokio::task::spawn_blocking(move || {
            original.validate_current()?;
            match std::fs::read(original.canonical_path().join(FILE)) {
                Ok(bytes) => Ok(Some(serde_json::from_slice::<Record>(&bytes)?)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(HostError::from(e)),
            }
        })
        .await??;
        if let Some(record) = saved {
            record.validate(owner.root_id())?;
            if record.request != request
                || record.paths.workspace != paths.workspace
                || record.paths.configuration != paths.configuration
                || record.paths.repository != paths.repository
            {
                return Err("activation_request_conflict".into());
            }
            return Ok((Self { owner, record }, false));
        }
        let journal = Self {
            owner,
            record: Record {
                request,
                paths,
                binding: None,
                grant: None,
                phase: "preparing".into(),
                operations: BTreeMap::new(),
                outcome: None,
                cleanup_confirmed: false,
            },
        };
        journal.save().await?;
        Ok((journal, true))
    }
    pub async fn save(&self) -> Result<(), HostError> {
        let owner = self.owner.clone();
        let bytes = serde_json::to_vec(&self.record)?;
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            owner.validate_current()?;
            let mut file = tempfile::NamedTempFile::new_in(owner.canonical_path())?;
            file.write_all(&bytes)?;
            file.as_file().sync_all()?;
            owner.validate_current()?;
            file.persist(owner.canonical_path().join(FILE))
                .map_err(|e| e.error)?;
            std::fs::File::open(owner.canonical_path())?.sync_all()?;
            Ok::<_, HostError>(())
        })
        .await?
    }
    pub async fn read(root: PathBuf) -> Result<Record, HostError> {
        tokio::task::spawn_blocking(move || {
            let location = maka_event_log::root::resolve(&root)?;
            let bytes = std::fs::read(location.canonical_path().join(FILE))?;
            let record: Record = serde_json::from_slice(&bytes)?;
            record.validate(location.root_id())?;
            let after = maka_event_log::root::resolve(&root)?;
            if after.root_id() != location.root_id()
                || after.canonical_path() != location.canonical_path()
            {
                return Err("activation_root_changed".into());
            }
            Ok(record)
        })
        .await?
    }
}
impl Record {
    fn validate(&self, root_id: &str) -> Result<(), HostError> {
        self.request.validate()?;
        if self.binding.as_ref().is_some_and(|binding| {
            binding.source.root_id != root_id || binding.identity.agent != self.request.agent_id
        }) {
            return Err("activation_binding_mismatch".into());
        }
        if let Some(grant) = &self.grant
            && (Some(&grant.binding) != self.binding.as_ref()
                || grant.activation != self.request.activation_id
                || grant.permissions != self.request.ceiling)
        {
            return Err("activation_grant_mismatch".into());
        }
        for (method, operation) in &self.operations {
            operation.reference.validate()?;
            let source = &self
                .binding
                .as_ref()
                .ok_or("activation_binding_missing")?
                .source;
            if !matches!(
                method.as_str(),
                "create"
                    | "prepareCheckpoint"
                    | "activateCheckpoint"
                    | "send"
                    | "publishCheckpoint"
            ) || operation.reference.method != *method
                || operation.reference.source.state_domain != source.scope.state_domain
                || operation.reference.source.deployment != source.scope.deployment
                || operation.reference.namespace != source.scope.namespace
                || operation.reference.export != maka_agent_api::EXPORT
            {
                return Err("activation_operation_mismatch".into());
            }
        }
        if matches!(self.outcome, Some(Outcome::Completed { .. }))
            && !self.operations.contains_key("publishCheckpoint")
        {
            return Err("activation_commit_reference_missing".into());
        }
        Ok(())
    }
}
