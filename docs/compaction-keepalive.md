# 远程手动压缩中断：根因与部署验收

## 已确认的证据

2026-10-29，AlphaPro 会话 `session-a20a80f7-e233-444e-8ee7-6975a42d1214` 多次手动 `/compact` 在约 125 秒后失败。会话先记录 `This operation was aborted`，随后模型摘要流记录缺少终止事件。昨天已修复的 `reasoning.effort=none` 错误没有复发，提供商默认 `high` 仍然生效。

用户确认入口是 dsh-manager。Agent 日志出现 `outcome=cancelled stream=false elapsedMs=125003 error=manager cancelled proxy request` 等记录。本机直接调用原生 RPC 则在 258921 毫秒后成功：压缩1107条历史项、约511584 tokens，上下文从77%降到20%。原会话字节前缀已与备份完全一致。

## 原因

1. Manager 对 DSH 控制 API 使用缓冲转发，仅静态资源启用流式响应。
2. 原生手动压缩的 HTTP 响应须等模型摘要全部完成，数分钟内没有响应头/正文。
3. 公网调用链在约125秒关闭连接，Manager 发送 `proxy_cancel`。
4. DSH 将 HTTP 请求的取消信号传入手动压缩，任务和摘要流随后停止。

源码中的 Manager 和 Agent 原生超时都是5分钟，因此不能把125秒直接说成 Manager 的5分钟定时器，也不能在没有公网代理日志时断言具体哪个代理产品施加了该截止时间。确认的设计缺陷是：缓冲长 RPC 没有面向浏览器的保活。

## 修复范围

仅识别原生 `POST /api/commands/execute` 的无附件 `/compact` JSON RPC。最多检查64KiB+1字节，检查后完整重放原始请求体和 Close 操作；大请求、其他命令及上传不改协议。

Manager 等待上游完整结果时，每15秒向公网浏览器刷新一个合法的 JSON 空白，并使用 `Cache-Control: no-store, no-transform` 与 `X-Accel-Buffering: no`。保活由 Manager 本身产生，兼容现有缓冲 Agent，不需要额外 DSH 插件。

最终响应保持原生 `rpcId` 与结果；保活后发现上游状态/JSON/关联错误时返回相关联的 RPC 失败，不冒充成功。真实客户端取消仍向 Agent 传播；总超时会结束上游，避免孤儿请求。

保留原有5分钟任务预算，且从转发开始计时，不被心跳续期。仅对已识别的 compact 响应，把 socket 写入截止设为任务截止后10秒，为 `proxy_cancel` 与完整相关联 RPC 失败预留终结时间；这10秒不是继续执行任务的预算，也不扩大其他 API 的超时。保活不绕过不可续期的外部绝对请求时限；若需要超过5分钟，应同时设计 Manager 与 Agent 一致的长任务预算或异步任务协议，不能只把所有超时无限放大。

Agent WS 断连或同ID连接替换会立即结束绑定旧连接的 pending，释放请求并发槽；流式上传中途断连也会中止阻塞读取。响应与取消均绑定实际连接，不能由新连接承接或重放旧请求，旧连接迟到的清理不影响新连接。

保活之后的成功结果校验真实原生 `result:{ok:true,value:...}`，失败结果必须有对象形式的 `error:{code,message,details}`。HTTP响应头一经保活发出不能追补；若上游之后试图改变 Cookie，Manager 返回明确的关联失败且不修改自身 cookie jar，避免浏览器与Manager认证状态分裂。普通 compact 不应依赖晚到 Cookie 更新。

## 构建与部署

当前改动是未提交的源码修复，不代表远端运行镜像已更新。部署前备份现有 Manager 数据卷及配置，保留原管理员凭据和已注册 Agent 数据，不要删除数据卷。

在部署机器的本项目根目录执行：

```sh
docker compose -f docker-compose.yml -f docker-compose.compaction-fix.yml build dsh-manager
docker compose -f docker-compose.yml -f docker-compose.compaction-fix.yml up -d --no-deps --force-recreate dsh-manager
```

覆盖文件使用显式 `dsh-manager:compaction-fix` 本地镜像和 `pull_policy: never`，避免原配置的 `pull_policy: always` 再拉取未修复的远端 latest。继续使用原有 `.env`/部署环境与数据卷。

## 验收

```sh
go test ./internal/api -run Compact -count=1
go test ./...
go vet ./...
```

必须覆盖请求识别、原始 body 重放、心跳先于 Agent 结果到达、最终 JSON 仍可解析、晚到错误不冒充成功、真实浏览器断开仍发送 `proxy_cancel`。

远程验收必须回到用户原 Manager 公网入口，对耗时超过原故障点的 `/compact` 检查15秒保活和最终结果。短请求、本机直连和加速定时器测试不能替代实际公网验收。部署完成前请把结果标为“会话已恢复、源码修复待部署”，不要称端到端问题已彻底解决。
