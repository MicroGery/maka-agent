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

//! Single-machine durable reference coordinator, sharing the Repository's SQL
//! transaction for generation validation, Head CAS and original receipts.
use super::*;
use crate::checkpoints::repository::SessionRepository;
use crate::checkpoints::repository::{files::FileRepository, sql};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::time::{SystemTime, UNIX_EPOCH};

fn now() -> Result<u64, StoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("execution authority clock precedes epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| invalid("execution authority clock overflow"))
}
fn deadline(at: u64, duration: u64) -> Result<u64, StoreError> {
    if duration == 0 {
        return Err(invalid("execution lease must be positive"));
    }
    at.checked_add(duration)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| invalid("execution lease overflow"))
}
fn transaction(db: &Connection) -> Result<Transaction<'_>, StoreError> {
    Ok(Transaction::new_unchecked(
        db,
        TransactionBehavior::Immediate,
    )?)
}
fn read_status(db: &Connection, grant: &Grant, at: u64) -> Result<(Status, bool), StoreError> {
    let row: Option<(String, String, i64)> = db
        .query_row(
            "SELECT grant_json,state,expires_at FROM checkpoint_executions WHERE activation_id=?",
            [grant.activation.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let (original, state, expires) = row.ok_or_else(|| invalid("execution grant is unknown"))?;
    if serde_json::from_str::<Grant>(&original)? != *grant {
        return Err(invalid("execution grant identity conflict"));
    }
    let expires = expires as u64;
    match state.as_str() {
        "active" if expires > at => Ok((
            Status::Active {
                expires_at: expires,
            },
            false,
        )),
        "active" => Ok((Status::Draining, true)),
        "draining" => Ok((Status::Draining, false)),
        "stopped" => Ok((Status::Stopped, false)),
        _ => Err(invalid("invalid execution state")),
    }
}

fn status(db: &Connection, grant: &Grant, at: u64) -> Result<Status, StoreError> {
    let (status, expired) = read_status(db, grant, at)?;
    if expired {
        db.execute(
            "UPDATE checkpoint_executions SET state='draining' WHERE activation_id=?",
            [grant.activation.to_string()],
        )?;
    }
    Ok(status)
}

fn claim(
    db: &Connection,
    request: Claim,
    handoff: Option<Binding>,
) -> Result<ClaimResult, StoreError> {
    if request.activation.is_nil() {
        return Err(invalid("execution activation is nil"));
    }
    request.binding.source.validate()?;
    let tx = transaction(db)?;
    let previous: Option<(String, String, Option<String>)> = tx.query_row(
        "SELECT request_json,receipt_json,handoff_json FROM checkpoint_execution_claims WHERE activation_id=?",
        [request.activation.to_string()],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).optional()?;
    if let Some((original, receipt, original_handoff)) = previous {
        if serde_json::from_str::<Claim>(&original)? != request
            || original_handoff
                .as_deref()
                .map(serde_json::from_str::<Binding>)
                .transpose()?
                != handoff
        {
            return Err(invalid("execution claim identity conflict"));
        }
        return Ok(serde_json::from_str(&receipt)?);
    }
    if let Some(source) = &handoff {
        if source.identity != request.binding.identity
            || source.source.root_id == request.binding.source.root_id
            || request.start.is_none()
            || sql::lookup(&tx, &source.source, source.identity.agent)?.as_ref() != Some(source)
        {
            return Err(invalid("invalid checkpoint execution handoff binding"));
        }
        if sql::lookup(&tx, &request.binding.source, request.binding.identity.agent)?.is_some() {
            return Err(invalid("handoff destination is already bound"));
        }
        let managed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM checkpoint_executions WHERE session_id=?)",
            [source.identity.session.to_string()],
            |r| r.get(0),
        )?;
        if !managed {
            return Err(invalid(
                "handoff requires a managed, stopped source environment",
            ));
        }
    } else if sql::lookup(&tx, &request.binding.source, request.binding.identity.agent)?.as_ref()
        != Some(&request.binding)
    {
        return Err(invalid("execution source has no matching Session binding"));
    }
    let expires_at = deadline(now()?, request.lease_millis)?;
    let busy: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM checkpoint_executions WHERE session_id=? AND state!='stopped')",
        [request.binding.identity.session.to_string()],
        |r| r.get(0),
    )?;
    let head = sql::head(&tx, &request.binding.identity)?;
    let actual = head.as_ref().map(|h| h.head.revision);
    if handoff.is_none() && head.as_ref().is_some_and(|h| h.binding != request.binding) {
        return Err(invalid(
            "cross-Root checkpoint start requires a handoff grant",
        ));
    }
    if handoff.as_ref().is_some_and(|source| {
        head.as_ref()
            .is_some_and(|h| Some(h.head.revision) == request.start && &h.binding != source)
    }) {
        return Err(invalid("handoff checkpoint provenance differs"));
    }
    let receipt = if busy {
        ClaimResult::Busy
    } else if actual != request.start {
        ClaimResult::Conflict { actual }
    } else {
        let previous: i64 = tx.query_row(
            "SELECT COALESCE(MAX(generation),0) FROM checkpoint_executions WHERE session_id=?",
            [request.binding.identity.session.to_string()],
            |r| r.get(0),
        )?;
        let generation = previous
            .checked_add(1)
            .ok_or_else(|| invalid("execution generation exhausted"))?;
        ClaimResult::Granted {
            grant: Box::new(Grant {
                activation: request.activation,
                generation: generation as u64,
                binding: request.binding.clone(),
                start: request.start,
                permissions: request.permissions.clone(),
                handoff: handoff.clone(),
            }),
            expires_at,
        }
    };
    tx.execute(
        "INSERT INTO checkpoint_execution_claims (activation_id,request_json,receipt_json,handoff_json) VALUES(?,?,?,?)",
        params![
            request.activation.to_string(),
            serde_json::to_string(&request)?,
            serde_json::to_string(&receipt)?,
            handoff.as_ref().map(serde_json::to_string).transpose()?,
        ],
    )?;
    if let ClaimResult::Granted { grant, expires_at } = &receipt {
        if handoff.is_some() {
            tx.execute("INSERT INTO checkpoint_repository_sources(source_key,source_json,session_id) VALUES(?,?,?)",
                params![request.binding.source.key()?,serde_json::to_string(&request.binding.source)?,request.binding.identity.session.to_string()])?;
        }
        tx.execute(
            "INSERT INTO checkpoint_executions (activation_id,session_id,generation,grant_json,state,expires_at) VALUES(?,?,?,?,'active',?)",
            params![
                grant.activation.to_string(),
                grant.binding.identity.session.to_string(),
                i64::try_from(grant.generation).map_err(|_| invalid("execution generation overflow"))?,
                serde_json::to_string(grant)?,
                *expires_at as i64,
            ],
        )?;
    }
    tx.commit().map_err(StoreError::CommitUnknown)?;
    Ok(receipt)
}

impl ExecutionAuthority for FileRepository {
    fn repository_identity(&self) -> &str {
        self.identity()
    }
    fn claim(&self, request: Claim) -> BoxFuture<'_, Result<ClaimResult, StoreError>> {
        self.run(move |db| claim(db, request, None))
    }
    fn claim_handoff(
        &self,
        request: HandoffClaim,
    ) -> BoxFuture<'_, Result<ClaimResult, StoreError>> {
        self.run(move |db| claim(db, request.target, Some(request.source)))
    }
    fn observe(&self, grant: Arc<Grant>) -> BoxFuture<'_, Result<Status, StoreError>> {
        self.run(move |db| {
            let (current, expired) = read_status(db, &grant, now()?)?;
            if !expired {
                return Ok(current);
            }
            // Re-read under the write lock: a concurrent renewal may have won.
            let tx = transaction(db)?;
            let result = status(&tx, &grant, now()?)?;
            tx.commit().map_err(StoreError::CommitUnknown)?;
            Ok(result)
        })
    }
    fn renew(&self, request: Renewal) -> BoxFuture<'_, Result<Status, StoreError>> {
        self.run(move |db| {
            if request.operation.is_nil() {
                return Err(invalid("execution renewal operation is nil"));
            }
            let tx = transaction(db)?;
            let old: Option<(String, String)> = tx.query_row(
                "SELECT request_json,receipt_json FROM checkpoint_execution_renewals WHERE operation_id=?",
                [request.operation.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            ).optional()?;
            if let Some((original, receipt)) = old {
                if serde_json::from_str::<Renewal>(&original)? != request {
                    return Err(invalid("execution renewal identity conflict"));
                }
                return Ok(serde_json::from_str(&receipt)?);
            }
            let at = now()?;
            let proposed = deadline(at, request.lease_millis)?;
            let result = match status(&tx, &request.grant, at)? {
                Status::Active { expires_at } => {
                    let expires_at = expires_at.max(proposed);
                    tx.execute(
                        "UPDATE checkpoint_executions SET expires_at=? WHERE activation_id=?",
                        params![expires_at as i64, request.grant.activation.to_string()],
                    )?;
                    Status::Active { expires_at }
                }
                other => other,
            };
            tx.execute(
                "INSERT INTO checkpoint_execution_renewals (operation_id,request_json,receipt_json) VALUES(?,?,?)",
                params![
                    request.operation.to_string(),
                    serde_json::to_string(&request)?,
                    serde_json::to_string(&result)?,
                ],
            )?;
            tx.commit().map_err(StoreError::CommitUnknown)?;
            Ok(result)
        })
    }
    fn revoke(&self, grant: Arc<Grant>) -> BoxFuture<'_, Result<Status, StoreError>> {
        self.run(move |db| {
            let tx = transaction(db)?;
            let result = match status(&tx, &grant, now()?)? {
                Status::Stopped => Status::Stopped,
                _ => {
                    tx.execute(
                        "UPDATE checkpoint_executions SET state='draining' WHERE activation_id=?",
                        [grant.activation.to_string()],
                    )?;
                    Status::Draining
                }
            };
            tx.commit().map_err(StoreError::CommitUnknown)?;
            Ok(result)
        })
    }
    fn stopped(&self, evidence: Stopped) -> BoxFuture<'_, Result<(), StoreError>> {
        self.run(move |db| {
            let tx = transaction(db)?;
            match status(&tx, evidence.grant(), now()?)? {
                Status::Active { .. } => {
                    return Err(invalid(
                        "execution must be revoked before confirming cleanup",
                    ));
                }
                Status::Draining => {
                    tx.execute(
                        "UPDATE checkpoint_executions SET state='stopped' WHERE activation_id=?",
                        [evidence.grant().activation.to_string()],
                    )?;
                }
                Status::Stopped => {}
            }
            tx.commit().map_err(StoreError::CommitUnknown)
        })
    }
    fn receipt(
        &self,
        grant: Arc<Grant>,
        request: Commit,
    ) -> BoxFuture<'_, Result<Option<CommitReceipt>, StoreError>> {
        self.run(move |db| execution_receipt(db, &grant, &request))
    }
    fn original_commit(
        &self,
        grant: Arc<Grant>,
        operation: Uuid,
    ) -> BoxFuture<'_, Result<Option<CommitReceipt>, StoreError>> {
        self.run(move |db| {
            grant.binding.source.validate()?;
            if operation.is_nil() {
                return Err(invalid("invalid original commit operation"));
            }
            let key = crate::checkpoints::digest(&serde_json::to_vec(&(
                &grant.binding.source,
                operation,
            ))?);
            let original: Option<String> = db
                .query_row(
                    "SELECT request_json FROM checkpoint_repository_receipts WHERE operation_key=?",
                    [key],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(original) = original else {
                return Ok(None);
            };
            let request: Commit = serde_json::from_str(&original)?;
            if request.operation != operation || request.binding != grant.binding {
                return Err(invalid("original commit identity differs"));
            }
            execution_receipt(db, &grant, &request)
        })
    }
    fn commit(
        &self,
        grant: Arc<Grant>,
        request: Commit,
        verified: VerifiedCheckpoint,
    ) -> BoxFuture<'_, Result<CommitReceipt, StoreError>> {
        let objects = self.identity().to_owned();
        self.run(move |db| {
            verified.validate(&objects, &request)?;
            let tx = transaction(db)?;
            if let Some(receipt) = execution_receipt(&tx, &grant, &request)? {
                return Ok(receipt);
            }
            if !matches!(status(&tx, &grant, now()?)?, Status::Active { .. }) {
                // Persist the observed expiration even though publication is refused.
                tx.commit().map_err(StoreError::CommitUnknown)?;
                return Err(invalid("execution generation is not active"));
            }
            let receipt = sql::commit_in(&tx, &request)?;
            tx.execute(
                "INSERT INTO checkpoint_execution_commits (operation_key,grant_json) VALUES(?,?)",
                params![request.key()?, serde_json::to_string(grant.as_ref())?],
            )?;
            tx.commit().map_err(StoreError::CommitUnknown)?;
            Ok(receipt)
        })
    }
}
fn execution_receipt(
    db: &Connection,
    grant: &Grant,
    request: &Commit,
) -> Result<Option<CommitReceipt>, StoreError> {
    if request.binding != grant.binding {
        return Err(invalid("commit belongs to another execution Session"));
    }
    let receipt = sql::receipt(db, request)?;
    let original: Option<String> = db
        .query_row(
            "SELECT grant_json FROM checkpoint_execution_commits WHERE operation_key=?",
            [request.key()?],
            |r| r.get(0),
        )
        .optional()?;
    match (receipt, original) {
        (Some(receipt), Some(original)) if serde_json::from_str::<Grant>(&original)? == *grant => {
            Ok(Some(receipt))
        }
        (None, None) => Ok(None),
        _ => Err(invalid(
            "checkpoint operation belongs to another execution generation",
        )),
    }
}
