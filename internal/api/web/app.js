(() => {
  const $ = (id) => document.getElementById(id);
  async function api(path, options) {
    const opt = options || {};
    opt.credentials = "same-origin";
    opt.cache = "no-store";
    opt.headers = Object.assign({}, opt.headers || {});
    if (opt.body) opt.headers["Content-Type"] = "application/json";
    const response = await fetch(path, opt);
    const text = await response.text();
    if (!response.ok) {
      const error = new Error(text || ("HTTP " + response.status));
      error.status = response.status;
      throw error;
    }
    if (!text) return {};
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.toLowerCase().includes("application/json"))
      throw new Error("Manager 返回了非 JSON 响应（HTTP " + response.status + "），请确认使用的是最新 manager 服务");
    try { return JSON.parse(text); } catch { throw new Error("Manager 返回了无效 JSON（HTTP " + response.status + "）"); }
  }
  function showApp(username, managerVersion) { $("loginView").classList.add("hidden"); $("appView").classList.remove("hidden"); $("welcome").textContent = "已登录：" + username; if (managerVersion) $("version").textContent = "v" + managerVersion; }
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
  function setFingerprint(value) {
    const node = $("tlsFingerprint");
    const fingerprint = value || "HTTP 模式无需 TLS 指纹";
    node.dataset.value = fingerprint;
    node.textContent = node.dataset.hidden === "true" && value ? "••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••" : fingerprint;
  }
  async function loadPairing() {
    const info = await api("/api/v1/admin/pairing");
    $("pairingCode").textContent = info.pairingCode || "";
    setFingerprint(info.tlsFingerprint);
  }
  async function refreshPairing() { try { const info = await api("/api/v1/admin/pairing/refresh", { method: "POST" }); $("pairingCode").textContent = info.pairingCode || ""; setFingerprint(info.tlsFingerprint); } catch (error) { $("error").textContent = error.message; } }
  async function copyText(id) {
    const node = $(id);
    const text = node.dataset.value || node.textContent || "";
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
      $("agents").innerHTML = agents.length ? agents.map(function (x) { var id = x.id || x.agentId || ""; return '<div class="instance"><div class="instance-head"><h3>' + esc(x.name) + '</h3><span class="pill ' + (x.online ? 'online' : 'offline') + '">' + (x.online ? '在线' : '离线') + '</span></div><code>' + esc(id) + '</code><span class="muted">' + esc(x.platform) + ' · ' + esc(x.agentType || 'launcher') + ' · ' + esc(x.launcherVersion || x.pluginVersion || '') + '</span><div class=actions><button class=danger data-action=revoke data-agent="' + escAttr(id) + '">取消配对</button></div></div>'; }).join("") : '<span class="muted">暂无 Agent</span>';
      $("instances").innerHTML = instances.length ? instances.map(function (x) { return '<div class="instance"><div class="instance-head"><h3>' + esc(x.displayName) + '</h3><span class="pill ' + escAttr(x.state) + '">' + esc(x.state) + '</span></div><div class=muted>电脑：' + esc(x.agentName || x.agentId) + '</div><code>' + esc(x.agentId + '/' + x.instanceId) + '</code><span class="muted">' + (x.urlAvailable ? '服务地址可用' : '服务未就绪') + '</span><div class="actions"><button data-action="start" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">启动</button><button class="danger" data-action="stop" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">停止</button><button data-action="restart" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">重启</button><button class="secondary" data-action="open" data-agent="' + escAttr(x.agentId) + '" data-instance="' + escAttr(x.instanceId) + '">打开 dsh</button></div></div>'; }).join("") : '<span class="muted">暂无实例</span>';
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
  function confirmRevoke(name) {
    return new Promise(function (resolve) {
      const dialog = document.createElement("dialog");
      dialog.setAttribute("aria-labelledby", "dsh-revoke-title");
      dialog.style.border = "0";
      dialog.style.padding = "0";
      dialog.style.background = "transparent";
      dialog.style.borderRadius = "14px";
      dialog.style.boxShadow = "none";
      dialog.style.overflow = "visible";
      dialog.innerHTML = '<form method="dialog" style="min-width:min(420px,calc(100vw - 48px));padding:24px;border:0;border-radius:14px;background:#141c31;color:#edf4ff;box-shadow:0 20px 80px #0008"><h3 id="dsh-revoke-title" style="margin:0 0 12px;font-size:18px">确认取消配对</h3><p style="margin:0 0 20px;color:#b8c6df">确定取消电脑「' + esc(name) + '」的配对吗？其关联 dsh 实例也会被移除。</p><div style="display:flex;justify-content:flex-end;gap:8px"><button value="cancel" type="button">取消</button><button id="dsh-revoke-confirm" value="confirm" type="button" style="background:#a83b56;color:#fff">确认取消配对</button></div></form>';
      document.body.appendChild(dialog);
      const finish = function (value) { dialog.close(); dialog.remove(); resolve(value); };
      dialog.querySelector("button[value=cancel]").onclick = function () { finish(false); };
      dialog.querySelector("#dsh-revoke-confirm").onclick = function () { finish(true); };
      dialog.addEventListener("cancel", function () { finish(false); }, { once: true });
      dialog.showModal();
    });
  }
  async function revokeAgent(agent, button) {
    const card = button?.closest(".instance");
    const name = card?.querySelector("h3")?.textContent || agent;
    if (!(await confirmRevoke(name))) return;
    if (button) { button.disabled = true; button.textContent = "取消中…"; }
    $("error").textContent = "正在取消配对…";
    try {
      try {
        const result = await api("/api/v1/admin/agents/" + encodeURIComponent(agent) + "/revoke", { method: "POST" });
        if (result.deleted !== true)
          throw new Error("Manager 未确认删除数据库记录");
      } catch (error) {
        // Older v0.2.x managers only expose the DELETE endpoint.
        if (error.status !== 404) throw error;
        await api("/api/v1/admin/agents/" + encodeURIComponent(agent), { method: "DELETE" });
      }
      card?.remove();
      try { await loadData(); } catch {}
      $("error").textContent = "已取消配对";
    } catch (error) {
      if (button) { button.disabled = false; button.textContent = "取消配对"; }
      $("error").textContent = error.message;
    }
  }
  $("loginForm").addEventListener("submit", login);
  $("refreshButton").addEventListener("click", loadData);
  $("logoutButton").addEventListener("click", logout);
  $("refreshPairing").addEventListener("click", refreshPairing);
  $("copyPairing").addEventListener("click", function () { copyText("pairingCode"); });
  $("copyFingerprint").addEventListener("click", function () { copyText("tlsFingerprint"); });
  $("toggleFingerprint").addEventListener("click", function () {
    const node = $("tlsFingerprint");
    const hidden = node.dataset.hidden === "true";
    node.dataset.hidden = hidden ? "false" : "true";
    node.classList.toggle("revealed", hidden);
    $("toggleFingerprint").textContent = hidden ? "隐藏 TLS 指纹" : "显示 TLS 指纹";
    setFingerprint(node.dataset.value || "");
  });
  $("instances").addEventListener("click", function (event) { const button = event.target.closest("button[data-action]"); if (!button) return; const action = button.dataset.action; if (action === "open") openDsh(button.dataset.agent, button.dataset.instance); else command(action, button.dataset.agent, button.dataset.instance); });
  $("agents").addEventListener("click", function (event) { const button = event.target.closest("button[data-action=revoke]"); if (button) revokeAgent(button.dataset.agent, button); });
  api("/api/v1/auth/me").then(function (result) { showApp(result.username, result.version); return loadData(); }).catch(showLogin);
})();
