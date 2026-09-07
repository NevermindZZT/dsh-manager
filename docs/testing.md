# dsh-manager / dsh-launcher 测试说明

## 1. 环境

manager：Go 1.26+、PowerShell、可选 Docker Desktop。
launcher：Windows 10/11、.NET 8 SDK、WebView2、Node.js、dsh；SSH 测试需要系统 OpenSSH。

manager 自身只监听一个 HTTP 端口。HTTPS/WSS 仅由外部反向代理提供，测试 manager 本地服务时使用 HTTP/WS。

## 2. 自动测试

### manager

```powershell
cd D:\code\dsh-launcher\dsh-manager
$env:GOMODCACHE = "$PWD\.tools\gomodcache"
$env:GOCACHE = "$PWD\.tools\gocache"
gofmt -w cmd internal
go test ./...
go vet ./...
go build -trimpath -o .\bin\dsh-manager.exe .\cmd\dsh-manager
```

覆盖：SQLite registry、HTTP enrollment、heartbeat、Dashboard 登录、Agent WS 命令、浏览器 HTTP/WS tunnel、startup bootstrap redirect、Cookie forwarding。

### plugin

```powershell
cd D:\code\dsh-launcher\dsh-manager-plugin
npm test
```

覆盖：HTTP manager transport、enrollment 生命周期、startup token bootstrap、Set-Cookie、authenticated WebSocket Cookie forwarding。

### launcher

```powershell
cd D:\code\dsh-launcher\dsh-launcher
$env:NUGET_PACKAGES = "$PWD\.tools\packages"
& .\.tools\dotnet\dotnet.exe restore .\src\DshLauncher.csproj
& .\.tools\dotnet\dotnet.exe build .\src\DshLauncher.csproj -c Release --no-restore
```

成功标准：0 errors。现有 WebView2 WindowsBase 警告不影响构建。

## 3. 启动隔离 manager

```powershell
cd D:\code\dsh-launcher\dsh-manager
$env:DSH_MANAGER_HTTP_ADDR = "127.0.0.1:18080"
$env:DSH_MANAGER_DATA_DIR = "$PWD\test-data"
$env:DSH_MANAGER_ADMIN_USERNAME = "admin"
$env:DSH_MANAGER_ADMIN_PASSWORD = "test-password"
$env:DSH_MANAGER_ADMIN_TOKEN = "legacy-api-token"
go run .\cmd\dsh-manager
```

从启动日志复制本次 pairing code。不要把 pairing code 固定写入环境变量或提交到 Git。

Dashboard：

```text
http://127.0.0.1:18080/manager
```

Dashboard 不应显示 TLS fingerprint，也不应要求接受私有证书警告。

## 4. launcher Agent 配置

```text
启用 dsh-manager Agent：勾选
服务器地址：http://127.0.0.1:18080
Agent 名称：test-pc
配对码：复制本次 manager 启动日志中的 pairing code
TLS 指纹：不存在，不填写
```

日志应出现：

```text
[Manager] Agent 配对成功: agent-...
[Manager] Agent 通道已连接
```

## 5. plugin Agent 配置

```powershell
cd D:\code\dsh-launcher\dsh-manager-plugin
npm install
$env:DSH_MANAGER_URL = "http://127.0.0.1:18080"
$env:DSH_MANAGER_PAIRING_CODE = "复制当前 manager 启动日志中的配对码"
$env:DSH_MANAGER_NAME = "plugin-dsh"
```

不要设置任何 TLS fingerprint 环境变量。插件会从 DSH 0.1.2-rc.1 `connection.authenticatedUrl()` 获得 startup URL，并只在内存中保存 token。

## 6. startup token / WebSocket tunnel 验证

1. launcher 或 plugin 上的本地 dsh 使用 0.1.2-rc.1 启动；
2. manager Dashboard 点击「打开 dsh」；
3. 首个根请求通过 Agent 使用 startup token；
4. dsh 返回的 303 `Location: /` 被改写回 `/dsh/<session>/`；
5. 浏览器收到 dsh Set-Cookie；
6. 后续 HTTP 请求不再 bootstrap，但携带 Cookie；
7. dsh WebSocket 请求携带同一 Cookie 并保持实时通信；
8. launcher/plugin 日志和 manager 数据库中都不应出现 startup token。

## 7. Docker 测试

```powershell
cd D:\code\dsh-launcher\dsh-manager
$env:DSH_MANAGER_ADMIN_USERNAME = "admin"
$env:DSH_MANAGER_ADMIN_PASSWORD = "docker-password"
$env:DSH_MANAGER_ADMIN_TOKEN = "docker-api-token"
docker compose up -d --build
docker compose logs -f dsh-manager
```

Compose 只发布一个 manager 端口（默认 `10090`），数据目录不要求固定 UID/GID。公网使用 Cloudflare Tunnel 时，源站配置为：

```yaml
ingress:
  - hostname: dsh.example.com
    service: http://dsh-manager:10090
  - service: http_status:404
```

## 8. 常见连接失败

### invalid agent credentials

launcher 保存了旧 manager 的 Agent ID/Token。只有 manager 数据库被替换、Agent 被取消配对，或需要注册新 Agent 时，才填写当前 pairing code；刷新 pairing code 本身不会使已有 Token 失效。

### Agent 离线

检查 manager HTTP 地址、单端口防火墙、launcher/plugin 进程和日志中的 `agent connected`。

### Dashboard 仍显示 TLS fingerprint

说明访问的是旧 manager 二进制或旧 Docker 镜像。停止旧进程，重建并启动当前版本；当前源码和新构建 Dashboard 不包含 TLS fingerprint 区块。

### 打开 dsh 后 401

确认目标 Agent 是 plugin 0.1.8 或包含 startupUrl 元数据的 launcher，并确认 dsh 是 0.1.2-rc.1。旧 Agent 无法为新 dsh 提供 startup token bootstrap。
