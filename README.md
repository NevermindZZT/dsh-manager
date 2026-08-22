# dsh-manager

服务器端 dsh 实例管理服务（M0）。当前版本提供：

- 独立 Go 工程和 Git 仓库；
- SQLite Agent / instance registry；
- 一次性 Agent 配对码；
- Agent token 认证；
- Agent 心跳和实例状态上报；
- 管理员实例查询 API；
- 健康检查；
- HTTP 服务优雅退出。

## 快速运行

需要 Go 1.26+：

```powershell
$env:DSH_MANAGER_HTTP_ADDR = ":8080"
$env:DSH_MANAGER_DATA_DIR = "./data"
$env:DSH_MANAGER_PAIRING_CODE = "paste-a-one-time-code"
$env:DSH_MANAGER_ADMIN_TOKEN = "keep-this-private"
go run ./cmd/dsh-manager
```

如果没有设置配对码或管理员 Token，服务会在启动日志中生成并显示临时值。正式部署应通过环境变量或 Secret 注入，不要把密钥提交到 Git。

## API

健康检查：

```text
GET /healthz
```

注册 Agent：

```http
POST /api/v1/agents/enroll
Content-Type: application/json

{"pairingCode":"...","name":"Office-PC","platform":"windows","launcherVersion":"0.1.3"}
```

响应中的 agentToken 只返回一次，launcher 应使用 DPAPI 等本机安全存储保护它。

Agent 心跳：

```http
POST /api/v1/agent/heartbeat
Authorization: Bearer <agentToken>
X-Agent-Id: <agentId>
Content-Type: application/json

{"instances":[{"instanceId":"local","displayName":"本地","type":"local","state":"running","urlAvailable":true,"generation":1,"eventSeq":3}]}
```

管理员查询：

```http
GET /api/v1/agents
GET /api/v1/instances
Authorization: Bearer <adminToken>
```

## 安全说明

当前 M0 是 HTTP 控制面原型，尚未实现 TLS/WSS、应用层加密、用户登录、RBAC 和远程 UI tunnel。不要直接将它暴露到公网。下一阶段先加入 HTTPS/WSS（支持自签名证书和 launcher 指纹固定），再实现控制命令和授权隧道。
