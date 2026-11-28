<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# maka-event-log

[English](README.md)

`maka-event-log` 管理单个 State Root 的底层存储：操作系统级独占进程锁（`RootOwner`）、SQLite 数据库（`RootStore`）、两阶段 Host 操作账本，以及不可变检查点（Checkpoint）发布。上层 Agent 事实表与索引由 [`maka-agent-state`](../../plugins/agent/state/src/lib.rs) 通过组合迁移挂载到同一个 `RootStore` 上。

## 核心模块

- [`root.rs`](src/root.rs) 与 [`root_store.rs`](src/root_store.rs) — 持有 State Root 独占锁，并在停机时等待已接纳的在途操作结清。
- [`connection.rs`](src/connection.rs) 与 [`writeback.rs`](src/writeback.rs) — 独立线程运行单写者 FIFO 队列与并发只读快照通道，仅在事务确认提交后广播缓存失效信号。
- [`root_store/data.rs`](src/root_store/data.rs) — 提供按 Root 或 Session 隔离的 SQL 执行接口，支持原生服务将业务状态变更与 Host 操作回执放在同一写事务内原子提交。
- [`applications.rs`](src/applications.rs) — 存储部署源、调用方意图（`CallerIntent`）以及区分业务结果与物理清理的操作回执。
- [`checkpoints/`](src/checkpoints.rs) — 工作区与对话历史的内容寻址快照发布、Head CAS 推进以及崩溃后恢复结算。
- [`schema.rs`](src/schema.rs) 与 [`migration.rs`](src/migration.rs) — 基础表结构定义与多领域组合迁移校验。

## 检查点仓库（Checkpoint Repository）

`checkpoints::repository` 将不可变对象存储与逻辑会话绑定、Head CAS 提交解耦：

- **默认本地模式（`RootStore::open`）** — Head 表保存在当前 Root 库内，已校验的私有暂存文件经目录 `fsync` 后直接硬链接晋升，避免重复拷贝。
- **外部仓库模式（`RootStore::open_with_checkpoint_storage`）** — 注入自定义 `Storage` 实现。`FileRepository::open` 提供了独立于工作 Root 的本地多进程参考实现，用独立 SQLite 仲裁并发进程并用文件系统保存不可变对象。

`Storage::checkout_exact` 在检出当前 Head 前会完整校验对象摘要、工作区闭包以及逻辑会话绑定来源，且仅提供只读数据检出，不授予任何执行权限。

## 执行权

`checkpoints::execution::ExecutionAuthority` 是独立于不可变对象库的可信控制面接口。grant 绑定逻辑 Session、稳定 activation ID、执行代次、精确起始 Head 以及沙箱和工具权限上限。`claim`、`renew` 对同一请求返回原始持久结果，拒绝结果也不改变；旧的成功回执不代表当前仍可执行，应通过 `observe` 查询现状。

可信装配层领取 grant、调用 `Execution::attach`，再通过 `Storage::with_execution` 将其传给 `RootStore::open_with_checkpoint_storage`，最后启动 Host。普通 claim 绑定原 Root/deployment/namespace 中的一个 native Session；计划内跨 Root 续作通过下述 `claim_handoff` 完成。Root 持久记录执行配置，不能绕过授权按默认本地模式重开。没有本地 checkpoint 身份绑定或发布记录的 Root，或没有未完成发布的既有共享仓库，可以显式启用；更换已登记的 grant 需要后续激活/迁移流程。

受管模式下，修改类 RPC 的统一入口拒绝其他 Session 的写入及 Root 全局、跨 Session 操作（包括创建/分支、Goal、调度和执行器安装）。Root 全局任务生产者保持关闭；恢复时先过滤其他 Session，再检查已保存权限，然后才预留 Run 或消费排队输入。授权拒绝保留原 Run 事实和队列。Native Owner 在消息/Run 准入及实际模型、工具授权时检查 grant，不在每次配置读取时重复查询。工具选择和沙箱权限（包括单独批准的文件/网络权限）均与 grant 上限取交集；若旧 Run 保存的权限或可批准权限超过该上限，则拒绝恢复。首版拒绝外部执行器后端。Host 每 250 ms 观察一次，每次观察最多等待五秒。撤销、过期或观察失败会不可逆地取消本地工作并启动 Host 关闭。这是对已接收工作的协作式取消，不是瞬时撤销外部副作用。authority 适配器负责调用方认证和可线性化的仲裁，序列化的 grant 字段本身不是访问凭据。

`FileRepository` 同时提供单机持久参考协调器，在同一个 SQL 事务中检查执行代次、执行预期 Head CAS 并保存原始发布回执。Session 一旦受管，普通 Repository 提交就不能绕过执行权检查；撤销后重试已完成的操作只返回原回执，不重新写入。Active 状态的观察只读；观察到过期时，在写事务内重新确认后才持久化执行屏障。该适配器不是分布式租约服务，不承诺依据墙上时钟的过期时间自动接管。

计划内交接由监督方调用 `Host::stop_execution`。Host 关闭传输、取消并排空插件及受管进程，等待 Root/SQL 清理确认后才登记 `Stopped`；重复关闭会保留原清理失败。撤销和最后向控制面登记清理结果的请求各有五秒期限。只有这个确认才允许下一代执行方领取资格；超时、租约过期、失联或清理失败都会阻止接管。外部 `Environment` 适配器必须覆盖原运行环境及所有受管写者，不能只结束父 PID。未受管编辑器或远端第三方副作用仍需自己的隔离/结算机制，本接口不会把未验证的 Workspace 快照变成一致快照。

默认本地 Host 不启用此可选模式。这些 Rust 接口不绑定云厂商或 Kubernetes，也不新增 CLI 配置或自动 checkpoint 触发。

## 计划内跨 Root 续作

1. 受管源端在静止边界发布包含 Workspace 的精确 checkpoint。Native 在持有源端准入时记录续作资格，不将 Goal、Bot、定时任务、待办或持久 Cell 状态、Room、父子关系、未完成工作或外部执行器历史静默丢弃。仅包含历史的存档、旧存档和仅供查看的快照仍可读取，导入不会为其补造续作资格。
2. 监督方停止并排空源端，再以当前 Head 的原发布方绑定及新 Root/deployment/namespace/Session 调用 `ExecutionAuthority::claim_handoff`。逻辑 Session 和 agent 不变。协调器要求精确 Head 仍被保留、此前所有执行代次均已停止，并原子登记目标绑定和 grant；直到目标发布后继版本，Head 仍保留原发布方来源。
3. 可信装配层携 grant 启动目标 Host。`prepareCheckpoint` 导入经过校验的规范历史，并在独立子目录中恢复文件，但不创建可运行 Session、不调用模型、不接收待执行消息。准备记录保存原目录身份和精确 checkpoint。目标重启后会验证并复用原准备；目录替换、文件修改、不完整复制、Session ID 已占用均拒绝覆盖。
4. `activateCheckpoint` 要求已提交的准备操作引用，以及符合 grant 上限的目标模型、工具和权限。目标配置、惰性的历史引用、启动激活标记和原始 Host 操作结果在同一事务中提交；交接 grant 无法启动缺少此标记的已有目标 Session，因此启动恢复不能运行原有的不相关工作，不重放旧工具，也不自动发消息。后续显式 `send` 使用原历史；`publishCheckpoint` 以目标代次推进同一个逻辑 Head。

Workspace 一致性仍是调用方确认或未验证；激活需要明确接受外部写入风险。准备隔离和哈希校验都不能阻止其他应用写入。凭据、依赖安装、全局 Memory、Goal 和跨 Session 协调状态不随之迁移。这条路径继续选定 checkpoint，而不包含其发布后的修改；监督方负责协调计划内交接并提供目标环境。它不实现云调度器、自动故障接管、UI、任意历史回退或覆盖已有目标 Session。

## 测试

```sh
cargo nextest run -p maka-event-log
```
