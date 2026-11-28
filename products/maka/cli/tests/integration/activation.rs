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

//! Actual product CLI processes with an independent persistent repository.
use super::run::{TestEnv, start_mock_ai};
use maka_mock_ai::{FinishReason, TurnPlan, Usage};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;
fn request() -> Value {
    json!({"schemaVersion":1,"activationId":Uuid::new_v4(),"agentId":Uuid::new_v4(),"start":{"kind":"new"},
        "stimulus":{"kind":"message","text":"first turn"},"timeoutMillis":30_000,"tokenBudget":10_000,
        "permissions":{"access":"danger_full_access","requestPermissions":false},
        "ceiling":{"sandbox":{"kind":"disabled"},"tools":[]},"tools":[],"acceptExternalWritersRisk":true})
}
fn command(
    env: &TestEnv,
    state: &Path,
    workspace: &Path,
    config: &Path,
    repository: &Path,
) -> tokio::process::Command {
    let mut cmd = env.maka_command();
    cmd.args(["activation", "run", "--state-root"])
        .arg(state)
        .arg("--workspace-root")
        .arg(workspace)
        .arg("--config-root")
        .arg(config)
        .arg("--repository")
        .arg(repository);
    let mut cmd = tokio::process::Command::from(cmd);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}
async fn run(mut cmd: tokio::process::Command, input: &Value) -> Output {
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().expect("start activation process");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(90), child.wait_with_output())
        .await
        .expect("activation bounded exit")
        .unwrap()
}
fn events(output: &Output) -> Vec<Value> {
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSONL: {e}: {line}"))
        })
        .collect()
}
fn terminal(output: &Output) -> Value {
    let values = events(output);
    let terminals: Vec<_> = values
        .iter()
        .filter(|value| value["type"] == "terminal")
        .collect();
    assert_eq!(
        terminals.len(),
        1,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    terminals[0]["outcome"].clone()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_three_processes_continue_without_source_roots_and_replay_without_resending() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mock = start_mock_ai(move |req, _| {
        let n = observed.fetch_add(1, Ordering::SeqCst);
        let messages = req.raw_body["messages"].to_string();
        let turn = n.min(2);
        if turn > 0 {
            assert!(
                messages.contains("checkpoint reply 0"),
                "missing first history: {messages}"
            );
        }
        if turn > 1 {
            assert!(
                messages.contains("checkpoint reply 1"),
                "missing second history: {messages}"
            );
        }
        if n == 2 {
            assert!(req.raw_body["tools"].as_array().unwrap().iter().any(|t| t["function"]["name"]=="exec"),"expected product Code Mode surface");
            return TurnPlan::new().push_tool_call(format!("read-{turn}"), "exec", json!({"code":"text(await tools.Read({path: \"answer.txt\"}));", "yield_time_ms":15000,"max_output_tokens":2000})).finish(FinishReason::ToolCalls);
        }
        if n == 3 {
            assert!(messages.contains("original checkpoint bytes"), "tool did not read restored workspace: {messages}");
        }
        TurnPlan::new()
            .push_text(format!("checkpoint reply {turn}"))
            .with_usage(Usage::new(100, 20))
            .finish(FinishReason::Stop)
    })
    .await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let repository = env.base_dir().join("shared-repository");
    let mut input = request();
    input["tools"] = json!(["Read"]);
    input["ceiling"]["tools"] = input["tools"].clone();
    let mut previous: Option<(std::path::PathBuf, std::path::PathBuf)> = None;
    let mut identity = None;
    let mut old_revision = Value::Null;
    let mut receipts = Vec::new();
    for index in 0..3 {
        if let Some((state, workspace)) = previous.take() {
            std::fs::remove_dir_all(state).unwrap();
            std::fs::remove_dir_all(workspace).unwrap();
        }
        let state = env.base_dir().join(format!("state-{index}"));
        let workspace = env.base_dir().join(format!("workspace-{index}"));
        std::fs::create_dir(&workspace).unwrap();
        if index == 0 {
            std::fs::write(workspace.join("answer.txt"), "original checkpoint bytes").unwrap();
        }
        let output = run(
            command(&env, &state, &workspace, &config, &repository),
            &input,
        )
        .await;
        let result = terminal(&output);
        assert_eq!(
            result["status"],
            "completed",
            "worker {index}: {result}: stdout={} stderr={} journal={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            std::fs::read_to_string(state.join("activation.json")).unwrap_or_default()
        );
        assert!(output.status.success());
        let ready = events(&output)
            .into_iter()
            .find(|v| v["type"] == "session_ready")
            .unwrap();
        if let Some(identity) = &identity {
            assert_eq!(identity, &ready["binding"]["identity"]);
        }
        identity = Some(ready["binding"]["identity"].clone());
        let query = events(&output)
            .into_iter()
            .find(|v| v["type"] == "publication_prepared")
            .unwrap()["query"]
            .clone();
        receipts.push((query, result["checkpoint"].clone()));
        let revision = result["checkpoint"]["revision"].clone();
        assert_ne!(revision, old_revision);
        if index == 0 {
            std::fs::write(workspace.join("answer.txt"), "later local edit").unwrap();
        } else {
            let restored: Vec<_> = std::fs::read_dir(&workspace)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.is_dir())
                .collect();
            assert_eq!(restored.len(), 1);
            assert_eq!(
                std::fs::read_to_string(restored[0].join("answer.txt")).unwrap(),
                "original checkpoint bytes"
            );
        }
        let replay = run(
            command(&env, &state, &workspace, &config, &repository),
            &input,
        )
        .await;
        assert_eq!(terminal(&replay), result);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if index == 2 { 4 } else { index + 1 },
            "same activation must not send twice"
        );
        let journal = std::fs::read_to_string(state.join("activation.json")).unwrap();
        assert!(!journal.contains("sk-test-run-key"));
        let status = env
            .maka_command()
            .args(["activation", "status", "--state-root"])
            .arg(&state)
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "{}",
            String::from_utf8_lossy(&status.stderr)
        );
        assert_eq!(events(&status)[0]["outcome"], result);
        // A different request with the same activation cannot create another turn.
        let mut changed = input.clone();
        changed["stimulus"]["text"] = json!("changed stimulus");
        let rejected = run(
            command(&env, &state, &workspace, &config, &repository),
            &changed,
        )
        .await;
        assert_eq!(terminal(&rejected)["status"], "fatal_failure");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if index == 2 { 4 } else { index + 1 }
        );
        input["activationId"] = json!(Uuid::new_v4());
        input["start"] = json!({"kind":"resume","identity":identity,"revision":revision});
        input["stimulus"]["text"] = json!(format!("turn {}", index + 1));
        old_revision = revision;
        previous = Some((state, workspace));
    }
    if let Some((state, workspace)) = previous.take() {
        std::fs::remove_dir_all(state).unwrap();
        std::fs::remove_dir_all(workspace).unwrap();
    }
    for (query, expected) in receipts {
        let mut cmd = env.maka_command();
        cmd.args(["activation", "receipt", "--repository"])
            .arg(&repository);
        let output = run(tokio::process::Command::from(cmd), &query).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(events(&output)[0]["publication"]["head"], expected);
    }
    // Executing a Code Mode tool saves discovery preferences. This state is not
    // silently discarded: the next preparation must reject the unsupported closure.
    let mut rejected = input.clone();
    rejected["activationId"] = json!(Uuid::new_v4());
    let workspace = env.base_dir().join("unsupported-parent");
    std::fs::create_dir(&workspace).unwrap();
    let output = run(
        command(
            &env,
            &env.base_dir().join("unsupported"),
            &workspace,
            &config,
            &repository,
        ),
        &rejected,
    )
    .await;
    assert_eq!(terminal(&output)["status"], "blocked");
    assert_eq!(terminal(&output)["reason"], "recovery_required");
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_rejects_invalid_and_overlapping_roots_without_model_calls() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mock = start_mock_ai(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        TurnPlan::new()
            .push_text("unexpected")
            .finish(FinishReason::Stop)
    })
    .await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let state = env.base_dir().join("state");
    let repository = env.base_dir().join("repository");
    for patch in [
        json!({"schemaVersion":2}),
        json!({"timeoutMillis":0}),
        json!({"acceptExternalWritersRisk":false}),
        json!({"stimulus":{"kind":"message","text":""}}),
    ] {
        let mut input = request();
        for (key, value) in patch.as_object().unwrap() {
            input[key] = value.clone();
        }
        let output = run(
            command(&env, &state, &env.workspace, &config, &repository),
            &input,
        )
        .await;
        assert_eq!(terminal(&output)["status"], "fatal_failure");
        assert!(!state.exists());
    }
    let output = run(
        command(&env, &state, &config, &config, &repository),
        &request(),
    )
    .await;
    assert_eq!(terminal(&output)["status"], "fatal_failure");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_timeout_stops_the_generation_without_publishing_or_leaking_credentials() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mock = start_mock_ai(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        TurnPlan::new()
            .with_delay(Duration::from_secs(60))
            .push_text("too late")
            .finish(FinishReason::Stop)
    })
    .await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let state = env.base_dir().join("state-timeout");
    let repository = env.base_dir().join("repository");
    let mut input = request();
    input["timeoutMillis"] = json!(5_000);
    let output = run(
        command(&env, &state, &env.workspace, &config, &repository),
        &input,
    )
    .await;
    let result = terminal(&output);
    assert_eq!(result["status"], "retryable_failure", "{result}");
    assert!(
        result["reason"] == "timeout" || result["reason"] == "execution_failed",
        "{result}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let record: Value =
        serde_json::from_slice(&std::fs::read(state.join("activation.json")).unwrap()).unwrap();
    assert_eq!(record["cleanup_confirmed"], true);
    use maka_event_log::checkpoints::{
        execution::{ExecutionAuthority, Grant, Status},
        repository::{SessionRepository, files::FileRepository},
    };
    let grant: Grant = serde_json::from_value(record["grant"].clone()).unwrap();
    let store = FileRepository::open(&repository).unwrap();
    assert_eq!(
        store.observe(Arc::new(grant.clone())).await.unwrap(),
        Status::Stopped
    );
    assert!(
        store
            .read_head(grant.binding.identity)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("sk-test-run-key"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("sk-test-run-key"));
    fn no_credential(path: &Path) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                no_credential(&path);
            } else if path.is_file() {
                let bytes = std::fs::read(&path).unwrap();
                assert!(
                    !bytes
                        .windows(b"sk-test-run-key".len())
                        .any(|w| w == b"sk-test-run-key"),
                    "credential copied to {}",
                    path.display()
                );
            }
        }
    }
    no_credential(&state);
    let replay = run(
        command(&env, &state, &env.workspace, &config, &repository),
        &input,
    )
    .await;
    assert_eq!(terminal(&replay), result);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_rejects_excess_sandbox_permissions_before_sending() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mock = start_mock_ai(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        TurnPlan::new()
            .push_text("unexpected")
            .finish(FinishReason::Stop)
    })
    .await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let state = env.base_dir().join("state-restricted");
    let repository = env.base_dir().join("repository");
    let mut input = request();
    input["ceiling"]["sandbox"] = json!({"kind":"managed","filesystem":{"default":"read","rules":[],"denyGlobs":[]},"network":"allowed"});
    let output = run(
        command(&env, &state, &env.workspace, &config, &repository),
        &input,
    )
    .await;
    assert_eq!(terminal(&output)["status"], "fatal_failure");
    assert_eq!(terminal(&output)["reason"], "permissions_exceed_grant");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let record: Value =
        serde_json::from_slice(&std::fs::read(state.join("activation.json")).unwrap()).unwrap();
    assert!(record["operations"]["send"].is_null());
    assert_eq!(record["cleanup_confirmed"], true);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_pending_question_returns_blocked_without_publishing() {
    let mock = start_mock_ai(|_,_| TurnPlan::new().push_tool_call("ask","AskUserQuestion",json!({"questions":[{"question":"Choose","options":[{"label":"A"},{"label":"B"}]}]})).with_usage(Usage::new(100,20)).finish(FinishReason::ToolCalls)).await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let state = env.base_dir().join("state-question");
    let repository = env.base_dir().join("repository");
    let mut input = request();
    input["tools"] = json!(["AskUserQuestion"]);
    input["ceiling"]["tools"] = input["tools"].clone();
    let output = run(
        command(&env, &state, &env.workspace, &config, &repository),
        &input,
    )
    .await;
    let result = terminal(&output);
    assert_eq!(
        result["status"],
        "blocked",
        "{result}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(result["reason"], "interaction_required");
    assert!(
        events(&output)
            .iter()
            .all(|v| v["type"] != "publication_prepared")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_killed_worker_never_advances_head_or_authorizes_a_competitor() {
    use maka_event_log::checkpoints::{
        execution::{ExecutionAuthority, Grant, Status},
        repository::{SessionIdentity, SessionRepository, files::FileRepository},
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let notified = started.clone();
    let mock = start_mock_ai(move |_, _| {
        if observed.fetch_add(1, Ordering::SeqCst) == 0 {
            TurnPlan::new()
                .push_text("committed first turn")
                .finish(FinishReason::Stop)
        } else {
            notified.notify_one();
            TurnPlan::new()
                .with_delay(Duration::from_secs(60))
                .push_text("uncommitted second turn")
                .finish(FinishReason::Stop)
        }
    })
    .await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let repository = env.base_dir().join("repository");
    let first_state = env.base_dir().join("first");
    let mut input = request();
    let first = run(
        command(&env, &first_state, &env.workspace, &config, &repository),
        &input,
    )
    .await;
    assert_eq!(
        terminal(&first)["status"],
        "completed",
        "{}",
        String::from_utf8_lossy(&first.stdout)
    );
    let identity: SessionIdentity = serde_json::from_value(
        events(&first)
            .into_iter()
            .find(|v| v["type"] == "session_ready")
            .unwrap()["binding"]["identity"]
            .clone(),
    )
    .unwrap();
    let revision = terminal(&first)["checkpoint"]["revision"].clone();
    input["activationId"] = json!(Uuid::new_v4());
    input["start"] = json!({"kind":"resume","identity":identity,"revision":revision});
    let state = env.base_dir().join("interrupted");
    let mut child = command(&env, &state, &env.workspace, &config, &repository)
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&input).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), started.notified())
        .await
        .expect("second worker running");
    // Status is a read-only observation even while the writer owns its Root.
    let status = env
        .maka_command()
        .args(["activation", "status", "--state-root"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    let record: Value =
        serde_json::from_slice(&std::fs::read(state.join("activation.json")).unwrap()).unwrap();
    assert_ne!(record["cleanup_confirmed"], true);
    let grant: Grant = serde_json::from_value(record["grant"].clone()).unwrap();
    let store = FileRepository::open(&repository).unwrap();
    assert_ne!(
        store.observe(Arc::new(grant.clone())).await.unwrap(),
        Status::Stopped
    );
    assert_eq!(
        serde_json::to_value(
            store
                .read_head(identity.clone())
                .await
                .unwrap()
                .unwrap()
                .head
                .revision
        )
        .unwrap(),
        revision
    );
    // Revocation is not physical stop evidence, even after the process died.
    store.revoke(Arc::new(grant)).await.unwrap();
    input["activationId"] = json!(Uuid::new_v4());
    let competitor = run(
        command(
            &env,
            &env.base_dir().join("competitor"),
            &env.workspace,
            &config,
            &repository,
        ),
        &input,
    )
    .await;
    assert_eq!(terminal(&competitor)["status"], "blocked");
    assert_eq!(terminal(&competitor)["reason"], "execution_busy");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        serde_json::to_value(
            store
                .read_head(identity)
                .await
                .unwrap()
                .unwrap()
                .head
                .revision
        )
        .unwrap(),
        revision
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_lost_commit_response_is_queryable_after_worker_termination() {
    use maka_event_log::checkpoints::{
        execution::{ExecutionAuthority, Grant},
        repository::{CommitReceipt, files::FileRepository},
    };
    use tokio::io::{AsyncBufReadExt, BufReader};
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mock = start_mock_ai(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        TurnPlan::new()
            .push_text("committed but response lost")
            .finish(FinishReason::Stop)
    })
    .await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let state = env.base_dir().join("lost-response");
    let repository = env.base_dir().join("repository");
    let input = request();
    let mut child = command(&env, &state, &env.workspace, &config, &repository)
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&input).unwrap())
        .await
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    let query = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let line = output
                .next_line()
                .await
                .unwrap()
                .expect("publication query before terminal");
            let event: Value = serde_json::from_str(&line).unwrap();
            assert_ne!(event["type"], "terminal", "{event}");
            if event["type"] == "publication_prepared" {
                break event["query"].clone();
            }
        }
    })
    .await
    .unwrap();
    // Stop consuming the response. Observe only the authority's original receipt;
    // process termination must not turn an already committed result into a retry.
    let store = FileRepository::open(&repository).unwrap();
    let grant: Grant = serde_json::from_value(query["grant"].clone()).unwrap();
    let operation = serde_json::from_value(query["operation"].clone()).unwrap();
    let committed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(receipt) = store
                .original_commit(Arc::new(grant.clone()), operation)
                .await
                .unwrap()
            {
                break receipt;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    std::fs::remove_dir_all(state).unwrap();
    let mut receipt = env.maka_command();
    receipt
        .args(["activation", "receipt", "--repository"])
        .arg(&repository);
    let result = run(tokio::process::Command::from(receipt), &query).await;
    assert!(result.status.success());
    let recovered: CommitReceipt =
        serde_json::from_value(events(&result)[0]["publication"].clone()).unwrap();
    assert_eq!(recovered, committed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_managed_tools_cannot_read_the_injected_configuration_root() {
    let config_path = Arc::new(std::sync::Mutex::new(String::new()));
    let captured = config_path.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let mock = start_mock_ai(move |req, _| {
        if counted.fetch_add(1, Ordering::SeqCst) == 0 {
            let path = captured.lock().unwrap().clone();
            let code = format!("const result = await tools.Read({{path:{}}}); if (result.kind === 'completed') throw new Error('configuration root readable'); text('configuration protected');",serde_json::to_string(&path).unwrap());
            TurnPlan::new().push_tool_call("private-read","exec",json!({"code":code,"yield_time_ms":15000,"max_output_tokens":2000})).finish(FinishReason::ToolCalls)
        } else {
            let messages = req.raw_body["messages"].to_string();
            assert!(messages.contains("configuration protected"),"{messages}");
            assert!(!messages.contains("private-fixture-secret"));
            TurnPlan::new().push_text("protected").finish(FinishReason::Stop)
        }
    }).await;
    let mut env = TestEnv::new();
    let config = env.init_root("config", &mock.base_url(), "danger-full-access");
    let private = config.join("private-fixture.txt");
    std::fs::write(&private, "private-fixture-secret").unwrap();
    *config_path.lock().unwrap() = private.to_str().unwrap().into();
    let mut input = request();
    input["permissions"]["access"] = json!("read_only");
    input["ceiling"]["sandbox"] = json!({"kind":"managed","filesystem":{"default":"read","rules":[],"denyGlobs":[]},"network":"allowed"});
    input["tools"] = json!(["Read"]);
    input["ceiling"]["tools"] = input["tools"].clone();
    let output = run(
        command(
            &env,
            &env.base_dir().join("private-read"),
            &env.workspace,
            &config,
            &env.base_dir().join("repository"),
        ),
        &input,
    )
    .await;
    assert_eq!(
        terminal(&output)["status"],
        "completed",
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
