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
use maka_agent_facts::history_snapshot::Continuation;
use maka_event_log::{
    root::{RootNamespaces, RootOwner},
    root_store::RootStore,
};
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn checkpoint_continuation_uses_source_relationships_and_keeps_old_captures_inert() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().to_owned();
    let owner = tokio::task::spawn_blocking(move || {
        RootOwner::create(
            &base.join("root"),
            &RootNamespaces {
                ownership: base.join("owners"),
                control: base.join("control"),
            },
        )
    })
    .await
    .unwrap()
    .unwrap();
    let mut migrations = maka_event_log::ROOT_MIGRATIONS.to_vec();
    migrations.extend_from_slice(crate::MIGRATIONS);
    migrations.sort_by_key(|m| m.version);
    let root = RootStore::open(Arc::new(owner), &migrations).await.unwrap();
    let data = root.data_writer().unwrap();
    let state = crate::Store::new(
        data.clone(),
        crate::DriverDomain::new(data.clone()).unwrap(),
    )
    .unwrap();
    let source = root
        .create_session("source", "source", &json!({}), 1)
        .await
        .unwrap();
    let record = source.get::<serde_json::Value>().await.unwrap().unwrap();
    let owner = RunOwner {
        deployment: Uuid::new_v4(),
        namespace: maka_plugin_core::composition::InstanceId(vec![Uuid::new_v4()]),
    };
    let history = state.session_history(owner.clone(), "source".into());
    let request = CaptureRequest {
        id: SnapshotId::new(),
        cut: history.capture().await.unwrap(),
    };
    let captured = history
        .capture_checkpoint_snapshot(
            request,
            source.revision_guard(&data, record.revision).unwrap(),
            Some(
                state
                    .quiescence_guard(owner.clone(), "source".into())
                    .unwrap(),
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        captured.manifest().await.unwrap().continuation,
        Some(Continuation::OrdinaryNativeV1)
    );
    let ordinary = history
        .capture_snapshot(
            CaptureRequest {
                id: SnapshotId::new(),
                cut: history.capture().await.unwrap(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(ordinary.manifest().await.unwrap().continuation, None);
    root.create_session("child", "child", &json!({"parentSessionId":"source"}), 1)
        .await
        .unwrap();
    let dependent = history
        .capture_checkpoint_snapshot(
            CaptureRequest {
                id: SnapshotId::new(),
                cut: history.capture().await.unwrap(),
            },
            source.revision_guard(&data, record.revision).unwrap(),
            Some(state.quiescence_guard(owner, "source".into()).unwrap()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        dependent.manifest().await.unwrap().continuation,
        None,
        "children are absent from the history payload but present in the source authority"
    );
    drop((source, state, data, history, captured, ordinary, dependent));
    root.close().await.unwrap();
}
