# dsh-manager Agent Protocol v1

## Transport

Agent 连接使用：

- HTTPS：配对和兼容的 heartbeat API；
- WSS：长连接 Agent 控制通道；
- `Authorization: Bearer <agentToken>`；
- `X-Agent-Id: <agentId>`。

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

launcher 使用本地或 SSH 转发后的 dsh URL 执行请求，再返回 `proxy_response`，body 使用 Base64。WebSocket tunnel 使用 `proxy_ws_open`、`proxy_ws_open_result`、`proxy_ws_frame` 和 `proxy_ws_close`，帧 body 使用 Base64，FrameType 区分 text 与 binary。

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
- `settings.host` — host-backed settings persistence;
- `plugin.config` — plugin-owned settings.

Unknown capability names are ignored. If an existing agent omits capabilities, manager treats it as a legacy launcher with the original HTTP/WebSocket behavior. A dsh-plugin must explicitly advertise the proxy capabilities it supports.

## 兼容性约束

- 未知消息类型必须记录并忽略，不得导致 Agent 退出；
- 未知命令必须返回 `ok:false`；
- 命令必须带 requestId；
- manager 不能通过该协议下发任意 shell；
- 协议版本升级时必须保留 `protocolVersion` 协商。
