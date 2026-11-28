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

use super::{EnvDirs, build_command};
use maka_mock_ai::{ControlPlaneConfig, FinishReason, MockAiBuilder, ModelEntry, TurnPlan, Usage};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capsule {
    query: Value,
    head: Value,
}

async fn invoke(mut command: tokio::process::Command, input: &Value) -> Vec<Value> {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap())
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(90), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

pub async fn produce(bin: &Path, directory: &Path, dirs: &EnvDirs) -> Capsule {
    let mock = MockAiBuilder::new()
        .control(ControlPlaneConfig {
            models: vec![ModelEntry::simple("test-model")],
            ..Default::default()
        })
        .handler_fn(|_, _| {
            TurnPlan::new()
                .push_text("activation compatibility history")
                .with_usage(Usage::new(100, 20))
                .finish(FinishReason::Stop)
        })
        .start()
        .await
        .unwrap();
    let config = directory.join("activation-config");
    let workspace = directory.join("activation-workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("proof.txt"), "saved workspace").unwrap();
    let mut init = build_command(bin, dirs);
    init.args(["host", "init", "--settings-stdin", "--root"])
        .arg(&config);
    invoke(init,&json!({"chatDefaults":{"sandboxMode":"danger-full-access"},"connections":[{"slug":"activation-test","name":"Activation Test","provider":"openai-compatible","configuration":{"baseUrl":mock.base_url()},"apiKey":"compat-test-key","enabled":true,"enabledModelIds":["test-model"]}],"defaultTarget":{"slug":"activation-test","modelId":"test-model"}})).await;
    let mut run = build_command(bin, dirs);
    run.args(["activation", "run", "--state-root"])
        .arg(directory.join("activation-state"))
        .arg("--workspace-root")
        .arg(workspace)
        .arg("--config-root")
        .arg(config)
        .arg("--repository")
        .arg(directory.join("activation-repository"));
    let output = invoke(run,&json!({"schemaVersion":1,"activationId":Uuid::new_v4(),"agentId":Uuid::new_v4(),"start":{"kind":"new"},"stimulus":{"kind":"message","text":"Save compatibility state"},"timeoutMillis":30_000,"tokenBudget":10_000,"permissions":{"access":"danger_full_access","requestPermissions":false},"ceiling":{"sandbox":{"kind":"disabled"},"tools":[]},"tools":[],"acceptExternalWritersRisk":true})).await;
    let query = output
        .iter()
        .find(|e| e["type"] == "publication_prepared")
        .unwrap()["query"]
        .clone();
    let result = &output.iter().find(|e| e["type"] == "terminal").unwrap()["outcome"];
    assert_eq!(result["status"], "completed");
    mock.shutdown().await;
    Capsule {
        query,
        head: result["checkpoint"].clone(),
    }
}

pub async fn verify(bin: &Path, directory: &Path, dirs: &EnvDirs, saved: &Capsule) {
    let bytes = std::fs::read(directory.join("activation-state/activation.json")).unwrap();
    let record: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(record["request"]["schemaVersion"], 1);
    assert_eq!(record["cleanup_confirmed"], true);
    assert_eq!(record["outcome"]["checkpoint"], saved.head);
    for operation in record["operations"].as_object().unwrap().values() {
        let reference: maka_plugin_core::operation::RecoveryRef =
            serde_json::from_value(operation["reference"].clone()).unwrap();
        reference.validate().unwrap();
    }
    // The old query must work after relocation and lease expiration without
    // opening the source Root or replaying a stimulus against the obsolete model URL.
    let mut receipt = build_command(bin, dirs);
    receipt
        .args(["activation", "receipt", "--repository"])
        .arg(directory.join("activation-repository"));
    let output = invoke(receipt, &saved.query).await;
    assert_eq!(output[0]["publication"]["head"], saved.head);
}
