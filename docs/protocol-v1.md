# dsh-manager Agent Protocol v1

## Transport

Agent connections use the same manager authority and port:

- HTTP for enrollment and compatible heartbeat APIs;
- WS for the long-lived Agent control channel;
- `Authorization: Bearer <agentToken>`;
- `X-Agent-Id: <agentId>`.

Manager itself does not terminate TLS or use certificate fingerprints. Deploy an external reverse proxy for HTTPS/WSS when traffic crosses an untrusted network.

Agent Token 由一次性配对码换取，manager 只保存 SHA-256 哈希，launcher 使用 Windows DPAPI 保护本地 Token。

## Agent → manager

### register

```json
{
  "type": "register",
  "instances": [
    {
      "instanceId": "local",
      "displayName": "本地",
      "type": "local",
      "state": "running",
      "urlAvailable": true,
      "generation": 1,
      "eventSeq": 1
    }
  ]
}
```

### heartbeat

字段与 `register` 相同。默认由 launcher 每 15 秒发送一次。

### command_result

```json
{
  "type": "command_result",
  "requestId": "cmd-123",
  "instanceId": "ssh:ubuntu",
  "ok": true,
  "error": ""
}
```

## manager → Agent

### hello

Agent 连接建立后发送，用于确认协议通道已建立。

### command

```json
{
  "type": "command",
  "requestId": "cmd-123",
  "instanceId": "local",
  "action": "restart",
  "args": {}
}
```

当前允许的 action：

- `start`
- `stop`
- `restart`
- `sync`
- `update`

## HTTP proxy 消息

manager Dashboard 为当前浏览器设置实例 Cookie 后，会将普通 HTTP 请求封装为 `proxy_request`：

```json
{
  "type": "proxy_request",
  "requestId": "proxy-123",
  "instanceId": "local",
  "method": "GET",
  "path": "/",
  "headers": {},
  "body": ""
}
```

launcher 或 dsh-manager-plugin 使用本地或 SSH 转发后的 dsh URL 执行请求，再返回 `proxy_response`，body 使用 Base64。响应中的多个 `Set-Cookie` 必须通过可选的 `setCookies` 数组逐条返回，不能合并到普通 headers 中；这对 dsh 历史会话等需要会话 Cookie 的接口是必需的。manager 会在浏览器边界按 `/dsh/<session>/` 隔离 Cookie Path，并在对应实例的 HTTP/WS 请求中重放 Cookie；Cookie 值和其他属性保持不变，但 `dsh-session` / `dsh-target` 等 manager Cookie 不会转发给 Agent。

### 可选的二进制流与 WebSocket 帧

支持 `proxy.http-stream-v1` 的 Agent 收到 `proxy_request.streamResponse:true` 时，使用 `proxy_response_start`（JSON，含 status/headers/setCookies）、零个或多个 binary WebSocket 消息 `proxy_response_chunk_binary`，最后使用 JSON `proxy_response_end`。每个 binary 消息的 payload 是 `JSON header + \n + raw bytes`；header 至少包含 `type` 和 `requestId`。`proxy_response_end.error` 表示流失败。manager 对每个 requestId 使用固定大小队列；生产者不得假定无限缓冲，manager 会隔离并中止超出队列的慢浏览器流。

支持 `proxy.binary-websocket-frame-v1` 时，`proxy_ws_open` 带 `binaryFrames:true`，双向使用 binary WebSocket 消息 `proxy_ws_frame_binary`，格式同样是 `JSON header + \n + raw frame bytes`，header 的 `frameType` 为 `text` 或 `binary`。未协商时继续使用 Base64 JSON `proxy_ws_frame`。WebSocket tunnel 仍使用 `proxy_ws_open`、`proxy_ws_open_result` 和 `proxy_ws_close`。

支持 `proxy.cancel-v1` 的 Agent 可接收 manager 的 `{ "type":"proxy_cancel", "requestId":"...", "instanceId":"...", "reason":"client_disconnected" }`。当浏览器 HTTP 请求上下文结束时，manager 尽力发送该消息以取消对应上游工作；不要求确认，未协商时不会发送。

支持 `proxy.http-request-stream-v1` 时，manager 以 JSON `proxy_request_start` 发送请求元数据，以一个或多个 binary `proxy_request_chunk_binary` 发送不大于 64 KiB 的原始请求体块，最后以 JSON `proxy_request_end` 收尾。binary envelope 为 `JSON header + \n + raw bytes`，header 含 `type`、`requestId`、`instanceId`。未协商时继续使用 Base64 `proxy_request.body`。

### Manager 出站调度

manager 对每条 Agent WebSocket 使用有界的 critical、interactive、bulk 队列和单写入器；不改变任何消息 JSON 或二进制 envelope。`proxy_cancel`、`proxy_ws_close` 和 `hello` 为 critical，命令与普通代理控制消息为 interactive，二进制 WebSocket 帧为 bulk。调度器按固定加权轮询服务各队列，既优先控制消息，也保证 bulk 不会无限饥饿。队列满时 manager 会拒绝本次本地操作；Agent 无需新增处理或确认。

## 实例 ID

- 本地实例：`local`；
- SSH 实例：`ssh:<connection-display-name>`。

实例 ID 必须在一个 Agent 内稳定，不能使用临时端口作为身份。

## 状态语义

```text
stopped
starting
running
stopping
failed
offline
```

`generation` 用于区分重启前后的实例生命周期，`eventSeq` 用于后续事件游标和断线恢复。

## Optional agent metadata (backward-compatible)

Protocol version remains `1`. Existing launcher clients may omit all fields below and retain their previous behavior.

Enrollment and register/heartbeat messages may include:

```json
{
  "agentType": "dsh-plugin",
  "agentVersion": "v0.1.0",
  "pluginVersion": "0.1.0",
  "capabilities": ["proxy.http", "proxy.websocket", "settings.host", "plugin.config"]
}
```

Supported capability names currently include:

- `command` — lifecycle commands;
- `proxy.http` — HTTP proxy;
- `proxy.websocket` — WebSocket proxy;
- `proxy.http-stream-v1` — binary, chunked HTTP proxy responses;
- `proxy.binary-websocket-frame-v1` — raw binary envelopes for WebSocket tunnel frames;
- `proxy.cancel-v1` — best-effort cancellation of a browser-abandoned HTTP proxy request;
- `proxy.http-request-stream-v1` — binary, chunked HTTP proxy request bodies;
- `settings.host` — host-backed settings persistence;
- `plugin.config` — plugin-owned settings.

Unknown capability names are ignored. If an existing agent omits capabilities, manager treats it as a legacy launcher with the original HTTP/WebSocket behavior. A dsh-plugin must explicitly advertise the proxy capabilities it supports.

## 兼容性约束

- 未知消息类型必须记录并忽略，不得导致 Agent 退出；
- 未知命令必须返回 `ok:false`；
- 命令必须带 requestId；
- manager 不能通过该协议下发任意 shell；
- 协议版本升级时必须保留 `protocolVersion` 协商。
