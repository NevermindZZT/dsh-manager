# dsh-manager

![Version](https://img.shields.io/badge/version-v0.3.2-blue)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)
![Docker](https://img.shields.io/badge/Docker-Hub-2496ED)
![License](https://img.shields.io/badge/license-MIT-green)

服务器端 dsh 实例管理服务，使用 Go 编写，可直接运行或通过 Docker 部署。Docker 镜像发布到 `nevermindzzt/dsh-manager`。

> **Transport:** manager 只监听一个 plain HTTP 端口，同时承载 Dashboard、Agent API 和 Agent WebSocket。manager 不生成私有证书、不监听第二个 HTTPS 端口、不使用 TLS 指纹。公网部署请在该 HTTP upstream 前使用 Cloudflare Tunnel 或其他反向代理终止 HTTPS/WSS。

配套 Windows Agent / launcher：[github.com/NevermindZZT/dsh-launcher](https://github.com/NevermindZZT/dsh-launcher)

## 功能

- SQLite Agent / instance registry；
- 每次启动自动生成一次性 Agent 注册配对码；
- Agent Token 只在 enrollment 响应中返回一次；
- Agent 注册、心跳、多实例状态同步；
- 管理员查询 Agent 与 dsh 实例；
- 管理员通过 Agent WebSocket 下发启动、停止、重启、同步和更新命令；
- 按浏览器会话代理目标 dsh HTTP 与 WebSocket，并以有界优先级队列调度 Agent 出站消息；
- 兼容 DSH 0.1.2-rc.1 startup token 的一次性 bootstrap；
- 内置 Dashboard 登录和配对管理；
- Docker / docker-compose 部署。

## 快速运行

需要 Go 1.26+。环境变量优先于 `config.yaml`：

```powershell
$env:DSH_MANAGER_HTTP_ADDR = ":8080"
$env:DSH_MANAGER_DATA_DIR = "./data"
$env:DSH_MANAGER_ADMIN_USERNAME = "admin"
$env:DSH_MANAGER_ADMIN_PASSWORD = "change-this-password"
$env:DSH_MANAGER_ADMIN_TOKEN = "keep-this-private"
go run ./cmd/dsh-manager
```

manager 启动时会打印本次临时 pairing code。已有 Agent Token 不会因为 pairing code 刷新或 manager 重启而失效。

Dashboard：

```text
http://服务器:8080/manager
```

HTTP 只适合 loopback 或可信私有网络。公网访问必须通过外部 HTTPS 反向代理，并将请求转发到 manager 的 HTTP 端口。

## Docker

仓库中的 `docker-compose.yml` 使用 `10090` 作为唯一 manager 端口：

```powershell
$env:DSH_MANAGER_ADMIN_USERNAME = "admin"
$env:DSH_MANAGER_ADMIN_PASSWORD = "change-this-password"
$env:DSH_MANAGER_ADMIN_TOKEN = "long-random-admin-token"
docker compose pull
docker compose up -d
```

直接运行：

```powershell
docker run -d --name dsh-manager `
  -p 10090:10090 `
  -v ${PWD}/data:/data `
  -e DSH_MANAGER_HTTP_ADDR=:10090 `
  -e DSH_MANAGER_ADMIN_USERNAME=admin `
  -e DSH_MANAGER_ADMIN_PASSWORD=change-this-password `
  -e DSH_MANAGER_ADMIN_TOKEN=change-this-api-token `
  nevermindzzt/dsh-manager:latest
```

容器不要求宿主机 bind mount 的 `/data` 预先设置固定 UID/GID；只需要确保容器进程对该目录具有读写权限。

## Cloudflare Tunnel

Cloudflare 负责边缘 HTTPS，源站只需要指向 manager 的单一 HTTP 端口：

```yaml
ingress:
  - hostname: dsh.nevermindzzt.top
    service: http://127.0.0.1:10090
  - hostname: dshserver.nevermindzzt.top
    service: http://127.0.0.1:10090
  - service: http_status:404
```

如果 cloudflared 在 Docker 中运行，使用同一 Docker 网络中的服务名：

```yaml
ingress:
  - hostname: dshserver.nevermindzzt.top
    service: http://dsh-manager:10090
  - service: http_status:404
```

不再需要 `noTLSVerify`、第二个 10091 端口或源站私有证书。修改后执行：

```text
cloudflared tunnel ingress validate
```

使用域名之前确认：

```text
GET  /healthz                         -> dsh-manager JSON
POST /api/v1/agents/enroll            -> manager JSON
GET  /api/v1/agent/connect (Upgrade)  -> Agent authorization response or WS
```

## Dashboard 登录 API

```http
POST /api/v1/auth/login
Content-Type: application/json

{"username":"admin","password":"..."}
```

成功后返回 HttpOnly session cookie。旧版静态 Admin Token 仍可用于自动化 API：

```http
Authorization: Bearer <adminToken>
```

## Agent Protocol v1

Agent enrollment：

```http
POST http://manager.example.com:8080/api/v1/agents/enroll
Content-Type: application/json

{"pairingCode":"...","name":"Office-PC","platform":"windows","launcherVersion":"0.2.2"}
```

Agent WebSocket 使用同一 authority 和端口：

```text
ws://manager.example.com:8080/api/v1/agent/connect
Authorization: Bearer <agentToken>
X-Agent-Id: <agentId>
```

通过外部 HTTPS 反向代理时，客户端 URL 使用代理提供的 `https://` / `wss://`，但 proxy upstream 仍是 manager 的 `http://` / `ws://` 单端口。

协议保留 enrollment、register、heartbeat、command_result、proxy_request、proxy_response、proxy_ws_open、proxy_ws_frame 和 proxy_ws_close。Agent 可声明可选能力 `proxy.binary-response-v1`；manager 会在双方支持时通过二进制响应帧传输 HTTP body，旧 Agent 继续使用兼容的 Base64 JSON。未知可选字段必须被旧 Agent 忽略。

## DSH 0.1.2-rc.1 startup token

新版 dsh 的启动 URL 形如：

```text
http://127.0.0.1:<port>/?token=<one-time-token>
```

manager、launcher 和 plugin 的 bootstrap 约定如下：

1. Agent register/heartbeat 可在实例元数据中提供临时 `startupUrl`；
2. manager 只在 live Agent session 内记录“该实例支持 bootstrap”这一布尔状态，不保存 token；
3. `/dsh/<session>/` 的首个无 query `GET /` 才会下发 `bootstrap:true`；
4. Agent 使用自己内存中的 startup URL 请求本地 dsh；
5. dsh 返回的 `Location: /` 会被 manager 改写回 `/dsh/<session>/`；
6. dsh 的 Set-Cookie 和浏览器后续 Cookie 会继续通过 HTTP/WS tunnel 转发；
7. startup URL 不写入 SQLite、Dashboard API、持久化配置或日志。

## dsh UI 代理

管理员点击「打开 dsh」后得到：

```text
/dsh/<session-id>/
```

manager 代理 HTML、静态资源、REST API、上传下载和 WebSocket。目标 dsh 的 Cookie 在 Agent HTTP 与 WebSocket 请求中保持可用。

## 远程访问传输优化

- Agent 请求 dsh 时会根据浏览器的 `Accept-Encoding` 保留 gzip 响应；
- 带内容 hash 的 `/assets/*` 静态资源返回长期 immutable 缓存头；
- 新版 Agent 与 manager 协商 `proxy.binary-response-v1` 后，HTTP body 不再经过 Base64 JSON，而是使用二进制 WebSocket 响应帧；
- 旧 Agent 不声明该能力时自动回退到 Base64 JSON 协议；
- bootstrap、API、上传下载和 WebSocket 不会套用静态资源 immutable 缓存策略。


可选实例字段：

```json
{
  "instanceId": "local",
  "displayName": "本地",
  "state": "running",
  "urlAvailable": true,
  "startupUrl": "http://127.0.0.1:3080/?token=..."
}
```

`startupUrl` 是瞬时 bearer 信息，只能在内存中的 Agent WebSocket 消息中使用，manager 数据库和实例列表 API 不会返回它。

## 安全边界

- Agent Token 只在 enrollment 响应中返回一次；
- pairing code 只用于 enrollment，不会使已有 Agent Token 失效；
- manager 不生成私有 TLS 证书，也不做证书 pinning；
- plain HTTP 不提供传输加密，公网必须使用外部 HTTPS/WSS 反向代理；
- manager 不保存 SSH 私钥、SSH 密码或 dsh credentials；
- manager 不提供任意 shell 执行接口；
- Dashboard 使用 bcrypt 密码哈希和 HttpOnly Session Cookie；
- startup token 不持久化、不写入日志；
- 不要把 manager 的 plain HTTP upstream 直接暴露到不可信公网。
