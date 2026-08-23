(() => {
  const $ = (id) => document.getElementById(id);
  async function api(path, options) {
    const opt = options || {};
    opt.credentials = "same-origin";
    opt.headers = Object.assign({}, opt.headers || {});
    if (opt.body) opt.headers["Content-Type"] = "application/json";
    const response = await fetch(path, opt);
    const text = await response.text();
    if (!response.ok) throw new Error(text || ("HTTP " + response.status));
    return text ? JSON.parse(text) : {};
  }
  function showApp(username) { $("loginView").classList.add("hidden"); $("appView").classList.remove("hidden"); $("welcome").textContent = "已登录：" + username; }
  function showLogin() { $("appView").classList.add("hidden"); $("loginView").classList.remove("hidden"); }
  function esc(value) { return String(value || "").replace(/[&<>]/g, function (c) { return {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]; }); }
  function escAttr(value) { return String(value || "").replace(/[^a-zA-Z0-9_.:-]/g, "_"); }
  async function login(event) {
    event.preventDefault();
    $("loginError").textContent = "登录中…";
    try {
      const result = await api("/api/v1/auth/login", { method: "POST", body: JSON.stringify({ username: $("username").value, password: $("password").value }) });
      $("password").value = "";
      showApp(result.username);
      await loadData();
    } catch (error) { $("loginError").textContent = error.message; }
  }
  async function logout() { try { await api("/api/v1/auth/logout", { method: "POST" }); } finally { showLogin(); } }
  async function loadPairing() {
    const info = await api("/api/v1/admin/pairing");
    $("pairingCode").textContent = info.pairingCode || "";
    $("tlsFingerprint").textContent = info.tlsFingerprint || "HTTP 模式无需 TLS 指纹";
  }
  async function refreshPairing() { try { const info = await api("/api/v1/admin/pairing/refresh", { method: "POST" }); $("pairingCode").textContent = info.pairingCode || ""; $("tlsFingerprint").textContent = info.tlsFingerprint || "HTTP 模式无需 TLS 指纹"; } catch (error) { $("error").textContent = error.message; } }
  async function copyText(id) {
    const text = $(id).textContent || "";
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
        await navigator.clipboard.writeText(text);
      } else {
        const area = document.createElement("textarea");
        area.value = text;
        area.setAttribute("readonly", "");
        area.style.position = "fixed";
        area.style.opacity = "0";
        document.body.appendChild(area);
        area.select();
        area.setSelectionRange(0, area.value.length);
        const copied = document.execCommand("copy");
        document.body.removeChild(area);
        if (!copied) throw new Error("copy command failed");
      }
      $("error").textContent = "已复制";
    } catch (error) {
      $("error").textContent = "复制失败，请手动选择文本";
    }
  }
  async function loadData() {
    try {
      const values = await Promise.all([api("/api/v1/agents"), api("/api/v1/instances"), loadPairing()]);
      const agents = values[0].agents || [], instances = values[1].instances || [];
      $("agentCount").textContent = agents.filter(function (x) { return x.online; }).length;
      $("instanceCount").textContent = instances.length;
      $("runningCount").textContent = instances.filter(function (x) { return x.state === "running"; }).length;
      $("agents").innerHTML = agents.length ? agents.map(function (x) { return '<div class="instance"><div class="instance-head"><h3>' + esc(x.name) + '</h3><span class="pill ' + (x.online ? 'online' : 'offline') + '">' + (x.online ? '在线' : '离线') + '</span></div><code>' + esc(x.id) + '</code><span class="muted">' + esc(x.platform) + ' · ' + esc(x.launcherVersion) + '</span></div>'; }).join("") : '<span class="muted">暂无 Agent</span>';
      $("instances").innerHTML = instances.length ? instances.map(function (x) { return '<div class="instance"><div class="instance-head"><h3>' + esc(x.displayName) + '</h3><span class="pill ' + escAttr(x.state) + '">' + esc(x.state) + '</span></div><code>' + esc(x.agentId + '/' + x.instanceId) + '</code><span class="muted">' + (x.urlAvailable ? '服务地址可用' : '服务未就绪') + '</span><div class="actions"><button data-action="start" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">启动</button><button class="danger" data-action="stop" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">停止</button><button data-action="restart" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">重启</button><button class="secondary" data-action="open" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">打开 dsh</button></div></div>'; }).join("") : '<span class="muted">暂无实例</span>';
      $("error").textContent = "";
    } catch (error) { if (error.message.indexOf("authorization") >= 0) showLogin(); else $("error").textContent = error.message; }
  }
  async function command(action, agent, instance) {
    try { await api("/api/v1/instances/" + encodeURIComponent(agent) + "/" + encodeURIComponent(instance) + "/commands", { method: "POST", body: JSON.stringify({ action: action }) }); setTimeout(loadData, 500); }
    catch (error) { $("error").textContent = error.message; }
  }
  async function openDsh(agent, instance) {
    try { const result = await api("/api/v1/instances/" + encodeURIComponent(agent) + "/" + encodeURIComponent(instance) + "/open", { method: "POST" }); window.location.href = result.url || "/"; }
    catch (error) { $("error").textContent = error.message; }
  }
  $("loginForm").addEventListener("submit", login);
  $("refreshButton").addEventListener("click", loadData);
  $("logoutButton").addEventListener("click", logout);
  $("refreshPairing").addEventListener("click", refreshPairing);
  $("copyPairing").addEventListener("click", function () { copyText("pairingCode"); });
  $("copyFingerprint").addEventListener("click", function () { copyText("tlsFingerprint"); });
  $("instances").addEventListener("click", function (event) { const button = event.target.closest("button[data-action]"); if (!button) return; const action = button.dataset.action; if (action === "open") openDsh(button.dataset.agent, button.dataset.instance); else command(action, button.dataset.agent, button.dataset.instance); });
  api("/api/v1/auth/me").then(function (result) { showApp(result.username); return loadData(); }).catch(showLogin);
})();
