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
use maka_event_log::checkpoints::{
    Scope as CheckpointScope,
    execution::{Claim, ClaimResult, Execution, ExecutionAuthority, HandoffClaim, Permissions},
    repository::{SessionRepository, Source, files::FileRepository},
};
use maka_plugin_core::operation::RecoveryRef;

async fn detach(rig: &mut Rig) -> TestResult<()> {
    let errors = rig.finish().await;
    if !errors.is_empty() {
        return Err(format!("cleanup: {errors:?}").into());
    }
    rig.client = None;
    rig.host = None;
    rig.store = None;
    rig.captured.lock().unwrap().take();
    rig.stop_serving = CancellationToken::new();
    Ok(())
}
async fn managed(
    rig: &mut Rig,
    boot: &Boot,
    repository: &Arc<FileRepository>,
    execution: &Execution,
) -> TestResult<()> {
    let base = rig.directory.path().to_owned();
    let root = tokio::task::spawn_blocking(move || {
        RootOwner::open(
            &base.join("root"),
            &RootNamespaces {
                ownership: base.join("owners"),
                control: base.join("control"),
            },
        )
    })
    .await??;
    let storage = repository
        .storage(execution.grant().binding.identity.agent)?
        .with_execution(execution.clone())?;
    let mut migrations = maka_event_log::ROOT_MIGRATIONS.to_vec();
    migrations.extend_from_slice(maka_agent_state::MIGRATIONS);
    migrations.sort_by_key(|m| m.version);
    let store = Arc::new(
        RootStore::open_with_checkpoint_storage(Arc::new(root), &migrations, Some(storage)).await?,
    );
    rig.attach_host(store, boot.deployment, boot.gateway, true, None)
        .await
}
fn source(rig: &Rig, session: String) -> TestResult<Source> {
    let owner = rig
        .captured
        .lock()
        .unwrap()
        .clone()
        .ok_or("owner missing")?;
    Ok(Source {
        root_id: owner.store.owned_root()?.root_id().into(),
        session,
        scope: CheckpointScope {
            state_domain: owner.data_source.state_domain.clone(),
            deployment: owner.data_source.deployment,
            namespace: owner.data_source.namespace.clone(),
        },
    })
}
async fn backend(rig: &Rig, url: &str) -> TestResult<Value> {
    let client = rig.client.as_ref().unwrap();
    let catalog = call(client, "settings", "connections", json!({"kind":"start"})).await?;
    let configured = call(client,"settings","create",json!({"expectedCatalogRevision":catalog["revision"],"connection":{
        "slug":"handoff","name":"Local handoff target","provider":{"packageId":"maka.providers","entryId":"maka.providers","scope":"profile","name":"ollama"},
        "configuration":{"baseUrl":url},"enabled":true,"enabledModelIds":["llama3.2"],
        "modelOverrides":{"llama3.2":{"codeMode":false,"apiProtocol":"openai-chat-completions"}}
    }})).await?;
    assert_eq!(configured["kind"], "committed", "{configured}");
    Ok(
        json!({"kind":"model","connectionId":configured["connection"]["connectionId"],"modelId":"llama3.2"}),
    )
}
async fn recorded(rig: &Rig, method: &str, input: Value) -> TestResult<(RecoveryRef, Value)> {
    let mut pending = rig
        .client
        .as_ref()
        .unwrap()
        .call("agent", method.into(), input, Attachments::new())
        .await?;
    pending.start().await?;
    assert!(matches!(pending.receive().await?, Reply::Prepared { .. }));
    let Reply::Mutation { reply, cleanup } = pending.confirm(json!({"test":"handoff"})).await?
    else {
        return Err("mutation reply missing".into());
    };
    cleanup?;
    let reference = pending
        .reference()
        .ok_or("operation reference missing")?
        .clone();
    let Receipt::Recorded {
        outcome:
            OperationOutcome::Returned {
                result: OperationResult::Success { value },
            },
        ..
    } = reply.receipt
    else {
        return Err(format!("{method}: {:?}", reply.receipt).into());
    };
    pending.close().await?;
    Ok((reference, value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_handoff_prepares_without_execution_and_continues_in_fresh_root() {
    let mut a = Rig::new().unwrap();
    let mut b = Rig::new().unwrap();
    let result = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(150),
        exercise(&mut a, &mut b, CaptureCase::Complete),
    ))
    .catch_unwind()
    .await;
    let a_cleanup = a.finish().await;
    let b_cleanup = b.finish().await;
    assert!(matches!(result, Ok(Ok(Ok(())))), "{result:?}");
    assert!(a_cleanup.is_empty(), "{a_cleanup:?}");
    assert!(b_cleanup.is_empty(), "{b_cleanup:?}");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_handoff_rejects_checklist_state_missing_from_the_archive() {
    let mut a = Rig::new().unwrap();
    let mut b = Rig::new().unwrap();
    let result = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(150),
        exercise(&mut a, &mut b, CaptureCase::Checklist),
    ))
    .catch_unwind()
    .await;
    let a_cleanup = a.finish().await;
    let b_cleanup = b.finish().await;
    assert!(matches!(result, Ok(Ok(Ok(())))), "{result:?}");
    assert!(a_cleanup.is_empty(), "{a_cleanup:?}");
    assert!(b_cleanup.is_empty(), "{b_cleanup:?}");
}
#[derive(Clone, Copy)]
enum CaptureCase {
    Complete,
    Checklist,
    HistoryOnly,
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_handoff_rejects_omitted_workspace() {
    let mut a = Rig::new().unwrap();
    let mut b = Rig::new().unwrap();
    let result = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(150),
        exercise(&mut a, &mut b, CaptureCase::HistoryOnly),
    ))
    .catch_unwind()
    .await;
    let a_cleanup = a.finish().await;
    let b_cleanup = b.finish().await;
    assert!(matches!(result, Ok(Ok(Ok(())))), "{result:?}");
    assert!(a_cleanup.is_empty(), "{a_cleanup:?}");
    assert!(b_cleanup.is_empty(), "{b_cleanup:?}");
}
async fn exercise(a: &mut Rig, b: &mut Rig, capture: CaptureCase) -> TestResult<()> {
    let checklist = matches!(capture, CaptureCase::Checklist);
    let workspace_consistency = if matches!(capture, CaptureCase::HistoryOnly) {
        Value::Null
    } else {
        json!("caller_confirmed_quiescent")
    };
    let mut provider = MockAiBuilder::new().interactive(1).start().await?;
    let mut observed = provider.take_interactive_rx().unwrap();
    let boot_a = bootstrap(a.directory.path(), provider.base_url(), checklist).await?;
    let boot_b = bootstrap(b.directory.path(), provider.base_url(), false).await?;
    attach(a, &boot_a).await?;
    attach(b, &boot_b).await?;
    let backend_a = backend(a, &provider.base_url()).await?;
    let backend_b = backend(b, &provider.base_url()).await?;
    let created = call(a.client.as_ref().unwrap(),"agent","create",json!({"title":"Original","automaticTitle":false,"labels":[],"workspace":{"kind":"directory","path":a.directory.path().join("workspace")}})).await?;
    let session_a = created["session"]["id"]
        .as_str()
        .ok_or("source session missing")?
        .to_owned();
    let session_b = {
        let owner = b.captured.lock().unwrap().clone().unwrap();
        format!("{}{}", owner.prefix, Uuid::new_v4().simple())
    };
    let source_a = source(a, session_a.clone())?;
    let source_b = source(b, session_b.clone())?;
    assert_ne!(source_a.root_id, source_b.root_id);
    assert_ne!(source_a.scope.deployment, source_b.scope.deployment);
    detach(a).await?;
    detach(b).await?;
    let objects = tempfile::tempdir()?;
    let repository = Arc::new(FileRepository::open(objects.path())?);
    let binding = repository.bind(source_a, Uuid::new_v4()).await?;
    let ClaimResult::Granted { grant, .. } = repository
        .claim(Claim {
            activation: Uuid::new_v4(),
            binding: binding.clone(),
            start: None,
            permissions: Permissions {
                sandbox: maka_sandbox::Sandbox::Disabled,
                tools: Some(if checklist {
                    vec!["todo_write".into()]
                } else {
                    vec![]
                }),
            },
            lease_millis: 300_000,
        })
        .await?
    else {
        return Err("source grant missing".into());
    };
    let execution_a = Execution::attach(repository.clone(), Arc::new(*grant)).await?;
    managed(a, &boot_a, &repository, &execution_a).await?;
    std::fs::write(
        a.directory.path().join("workspace/answer.txt"),
        "checkpoint r1",
    )?;
    let sent = call(a.client.as_ref().unwrap(),"agent","send",json!({"sessionId":session_a,"message":"Remember the original-context-token.","backend":backend_a})).await?;
    let response = observed.recv().await.ok_or("source model missing")?;
    if checklist {
        response.send_plan(super::bot_tools::tool_response(
            "todo_write",
            json!({"todos":[{"content":"Retain this checklist","status":"pending"}]}),
        ));
        observed
            .recv()
            .await
            .ok_or("checklist tool result missing")?
            .reply_text("Checklist saved.");
        let source_owner = a.captured.lock().unwrap().clone().unwrap();
        assert!(source_owner.todo.has_items(&session_a).await?);
    } else {
        response.reply_text("I remember original-context-token.");
    }
    quiescent_message(a.client.as_ref().unwrap(), &session_a, &sent["messageId"]).await?;
    let preview = call(
        a.client.as_ref().unwrap(),
        "agent",
        "previewHistoryCheckpoint",
        json!({"sessionId":session_a}),
    )
    .await?;
    let published = call(a.client.as_ref().unwrap(),"agent","publishCheckpoint",json!({"sessionId":session_a,"expectedRevision":null,"confirmedSubtreeDigest":preview["subtreeDigest"],"workspaceConsistency":workspace_consistency})).await?;
    assert_eq!(published["state"]["kind"], "published", "{published}");
    let first = repository
        .read_head(binding.identity.clone())
        .await?
        .ok_or("head missing")?;
    std::fs::write(
        a.directory.path().join("workspace/answer.txt"),
        "later source edit",
    )?;
    a.host.as_ref().unwrap().stop_execution().await?;
    detach(a).await?;
    std::fs::rename(
        a.directory.path().join("root"),
        a.directory.path().join("source-unavailable"),
    )?;
    let ClaimResult::Granted { grant, .. } = repository
        .claim_handoff(HandoffClaim {
            source: binding.clone(),
            target: Claim {
                activation: Uuid::new_v4(),
                binding: maka_event_log::checkpoints::repository::Binding {
                    identity: binding.identity.clone(),
                    source: source_b,
                },
                start: Some(first.head.revision),
                permissions: Permissions {
                    sandbox: maka_sandbox::Sandbox::Disabled,
                    tools: Some(vec![]),
                },
                lease_millis: 300_000,
            },
        })
        .await?
    else {
        return Err("target grant missing".into());
    };
    let execution_b = Execution::attach(repository.clone(), Arc::new(*grant)).await?;
    managed(b, &boot_b, &repository, &execution_b).await?;
    let prepare_input =
        json!({"sessionId":session_b,"parent":b.directory.path().join("workspace")});
    if !matches!(capture, CaptureCase::Complete) {
        let error = call(
            b.client.as_ref().unwrap(),
            "agent",
            "prepareCheckpoint",
            prepare_input,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source_not_independently_resumable"),
            "{error}"
        );
        assert!(
            b.store
                .as_ref()
                .unwrap()
                .get_session::<Value>(&session_b)
                .await?
                .is_none()
        );
        assert!(observed.try_recv().is_err());
        provider.shutdown().await;
        return Ok(());
    }
    let (preparation, prepared) = recorded(b, "prepareCheckpoint", prepare_input.clone()).await?;
    let restored = PathBuf::from(prepared["path"].as_str().ok_or("prepared path missing")?);
    assert_eq!(
        std::fs::read_to_string(restored.join("answer.txt"))?,
        "checkpoint r1"
    );
    assert!(
        b.store
            .as_ref()
            .unwrap()
            .get_session::<Value>(&session_b)
            .await?
            .is_none()
    );
    assert!(
        call(
            b.client.as_ref().unwrap(),
            "agent",
            "send",
            json!({"sessionId":session_b,"message":"must not run","backend":backend_b})
        )
        .await
        .is_err()
    );
    assert!(observed.try_recv().is_err());
    // Restart after files are ready but before any Session/admission is installed.
    detach(b).await?;
    managed(b, &boot_b, &repository, &execution_b).await?;
    let activation = json!({"sessionId":session_b,"preparation":preparation,"preparedInput":prepare_input,"title":"Continued","permissions":{"access":"read_only","requestPermissions":false},"backend":backend_b,"tools":[],"acceptExternalWritersRisk":true});
    let mut fabricated = activation.clone();
    fabricated["preparation"]["operation"] = json!(Uuid::new_v4());
    assert!(
        call(
            b.client.as_ref().unwrap(),
            "agent",
            "activateCheckpoint",
            fabricated
        )
        .await
        .is_err()
    );
    let mut invalid = activation.clone();
    invalid["backend"]["connectionId"] = json!(Uuid::new_v4());
    assert!(
        call(
            b.client.as_ref().unwrap(),
            "agent",
            "activateCheckpoint",
            invalid
        )
        .await
        .is_err()
    );
    std::fs::write(
        restored.join("answer.txt"),
        "target edited before activation",
    )?;
    assert!(
        call(
            b.client.as_ref().unwrap(),
            "agent",
            "activateCheckpoint",
            activation.clone()
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(restored.join("answer.txt"))?,
        "target edited before activation"
    );
    assert!(
        b.store
            .as_ref()
            .unwrap()
            .get_session::<Value>(&session_b)
            .await?
            .is_none()
    );
    std::fs::write(restored.join("answer.txt"), "checkpoint r1")?;
    let (activation_ref, activated) = recorded(b, "activateCheckpoint", activation.clone()).await?;
    assert_eq!(
        activated["logicalSessionId"],
        json!(binding.identity.session)
    );
    assert_eq!(activated["session"]["id"], session_b);
    assert!(observed.try_recv().is_err(), "activation started a model");
    let sent = call(b.client.as_ref().unwrap(),"agent","send",json!({"sessionId":session_b,"message":"Continue from what you remember.","backend":backend_b})).await?;
    let request = observed.recv().await.ok_or("target continuation missing")?;
    assert!(serde_json::to_string(&request.request)?.contains("original-context-token"));
    request.reply_text("Continued in the new environment.");
    quiescent_message(b.client.as_ref().unwrap(), &session_b, &sent["messageId"]).await?;
    let preview = call(
        b.client.as_ref().unwrap(),
        "agent",
        "previewHistoryCheckpoint",
        json!({"sessionId":session_b}),
    )
    .await?;
    let second = call(b.client.as_ref().unwrap(),"agent","publishCheckpoint",json!({"sessionId":session_b,"expectedRevision":first.head.revision,"confirmedSubtreeDigest":preview["subtreeDigest"],"workspaceConsistency":"caller_confirmed_quiescent"})).await?;
    assert_eq!(second["state"]["kind"], "published", "{second}");
    let retained = repository
        .read_head(binding.identity.clone())
        .await?
        .ok_or("second head missing")?;
    assert_ne!(retained.head.revision, first.head.revision);
    assert_eq!(retained.binding, execution_b.grant().binding);
    {
        let checkout = repository
            .storage(binding.identity.agent)?
            .checkout_exact(binding.identity.clone(), retained.head.revision)
            .await?;
        let owner = b.captured.lock().unwrap().clone().unwrap();
        let state = owner.durable.with_data_writer(owner.store.data_writer()?)?;
        let history = state.session_history(owner.run_owner(), session_b.clone());
        let snapshot = history
            .import_snapshot(
                Uuid::new_v4(),
                checkout.manifest.history,
                checkout.snapshot,
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            snapshot.manifest().await?.continuation,
            Some(maka_agent_facts::history_snapshot::Continuation::OrdinaryNativeV1),
            "a continued Session can publish another independently resumable checkpoint"
        );
    }
    detach(b).await?;
    managed(b, &boot_b, &repository, &execution_b).await?;
    assert!(
        observed.try_recv().is_err(),
        "restarting an activated Host started a model"
    );
    // Original activation reply remains in the same transaction as the target Session.
    let receipt = b
        .store
        .as_ref()
        .unwrap()
        .operation(&activation_ref)?
        .operation_observation()
        .await?
        .ok_or("activation receipt missing")?;
    assert!(
        matches!(receipt.outcome,OperationOutcome::Returned { result: OperationResult::Success { ref value } } if value == &activated)
    );
    provider.shutdown().await;
    Ok(())
}
