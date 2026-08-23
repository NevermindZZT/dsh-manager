# dsh-manager / dsh-launcher 测试说明

## 1. 环境

manager：Go 1.26+、PowerShell、可选 Docker Desktop。
launcher：Windows 10/11、.NET 8 SDK、WebView2、Node.js、dsh；SSH 测试需要系统 OpenSSH。

## 2. 自动测试

### manager

```powershell
cd D:\\code\\dsh-launcher\\dsh-manager
$env:GOMODCACHE = "$PWD\\.tools\\gomodcache"
$env:GOCACHE = "$PWD\\.tools\\gocache"
gofmt -w cmd internal
go test ./...
go vet ./...
go build -trimpath -o .\\bin\\dsh-manager.exe .\\cmd\\dsh-manager
```

覆盖：SQLite registry、Agent 配对、heartbeat、管理员 API、Dashboard 首页、登录 Session、WSS 命令分发、浏览器 WebSocket tunnel。

### launcher

```powershell
cd D:\\code\\dsh-launcher\\dsh-launcher
$env:NUGET_PACKAGES = "$PWD\\.tools\\packages"
& .\\.tools\\dotnet\\dotnet.exe restore .\\src\\DshLauncher.csproj
& .\\.tools\\dotnet\\dotnet.exe build .\\src\\DshLauncher.csproj -c Release --no-restore
```

成功标准：0 errors。现有 nullable / WebView2 WindowsBase 警告不影响构建。

## 3. 启动 manager

```powershell
cd D:\\code\\dsh-launcher\\dsh-manager
$env:DSH_MANAGER_HTTP_ADDR = "127.0.0.1:18080"
$env:DSH_MANAGER_AGENT_HTTPS_ADDR = "127.0.0.1:18443"
$env:DSH_MANAGER_DATA_DIR = "$PWD\\test-data"
$env:DSH_MANAGER_PAIRING_CODE = "pair-test-001"
$env:DSH_MANAGER_ADMIN_USERNAME = "admin"
$env:DSH_MANAGER_ADMIN_PASSWORD = "test-password"
$env:DSH_MANAGER_ADMIN_TOKEN = "legacy-api-token"
go run .\\cmd\\dsh-manager
```

日志中应出现证书指纹、配对码和 dashboard login loaded from environment。

浏览器访问：https://127.0.0.1:18443/

首次访问自签名证书时接受浏览器警告，然后使用：用户名：admin，密码：test-password。
HTTP 模式也可以登录，但用户名、密码和 Agent 数据会以明文传输，只建议在可信内网使用；公网请使用 HTTPS/WSS。

## 4. launcher Agent 配置

在启动器设置中填写：

```text
启用 dsh-manager Agent：勾选
服务器地址：https://127.0.0.1:18443
Agent 名称：test-pc
配对码：pair-test-001
TLS 指纹：复制 manager 日志中的 fingerprintSha256
```

注意：HTTP 模式填写 http://127.0.0.1:18080，不需要 TLS 指纹；HTTPS 模式填写 https://127.0.0.1:18443，并填写 64 位 SHA-256 TLS 指纹。修改服务器地址或指纹后，旧 Agent 凭证会自动清除，需要重新配对。

launcher 日志应出现：

```text
[Manager] Agent 配对成功: agent-...
[Manager] Agent 通道已连接
```

## 5. 生命周期测试

在 Dashboard 依次点击：启动、重启、停止、启动、同步。

兼容的静态 Admin Token API：

```powershell
$headers = @{ Authorization = "Bearer legacy-api-token" }
Invoke-RestMethod -Uri "https://127.0.0.1:18443/api/v1/instances/<agentId>/local/commands" `
  -Method Post -Headers $headers -SkipCertificateCheck `
  -ContentType "application/json" -Body '{"action":"restart"}'
```

## 6. WebSocket tunnel 测试

1. launcher 中本地 dsh 处于 running；
2. Dashboard 点击目标实例的“打开 dsh”；
3. 页面加载 dsh 原生 Web UI；
4. 打开或创建 dsh session；
5. 发送消息并验证实时响应；
6. 对 SSH 实例重复测试；
7. 关闭 launcher，确认页面断开并释放 tunnel。

## 7. Docker 测试

```powershell
cd D:\\code\\dsh-launcher\\dsh-manager
$env:DSH_MANAGER_PAIRING_CODE = "pair-docker-001"
$env:DSH_MANAGER_ADMIN_USERNAME = "admin"
$env:DSH_MANAGER_ADMIN_PASSWORD = "docker-password"
$env:DSH_MANAGER_ADMIN_TOKEN = "docker-api-token"
docker compose up -d --build
docker compose logs -f dsh-manager
```

访问 https://服务器地址:8443/，停止服务：

```powershell
docker compose down
```

## 8. 常见连接失败

### Agent 通道连接失败

HTTP 和 HTTPS 都支持。HTTP 模式填写 http://服务器:8080；HTTPS 模式填写 https://服务器:8443，并在 launcher 中配置 TLS 指纹。

### TLS 指纹错误

复制 manager 启动日志中的 fingerprintSha256，输入完整 64 位 SHA-256 指纹。

### invalid agent credentials

launcher 保存了旧 manager 的 Agent ID/Token。修改服务器地址或 TLS 指纹并保存，再填入新的配对码。

### Agent 离线

检查 launcher 是否运行、8443 是否放行、防火墙、服务器地址、TLS 指纹，以及 launcher 日志中的 [Manager] 错误。

### 登录成功但实例操作失败

刷新 Dashboard，确认 Agent 在线；再检查 manager 日志中的 agent connected 和 agent command result。
