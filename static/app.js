"use strict";

// ---------- 内置示例 ----------
const EXAMPLES = {
  echo: [
    "import sys",
    "",
    'print("你好！输入一行我就回显，输入 quit 结束。")',
    "for line in sys.stdin:",
    '    line = line.rstrip("\\n")',
    '    if line == "quit":',
    '        print("再见！")',
    "        break",
    '    print("你说：" + line)',
    "",
  ].join("\n"),

  dual: [
    "import sys, time",
    "",
    "# 两路输出都不带换行，观察增量呈现",
    "for i in range(6):",
    '    sys.stdout.write(f"[out-{i}]")',
    '    sys.stderr.write(f"{{err-{i}}}")',
    "    time.sleep(0.4)",
    'sys.stdout.write("\\n")',
    'sys.stderr.write("\\n")',
    'print("完毕")',
    "",
  ].join("\n"),

  eof: [
    "import sys",
    "",
    'print("请随便输入几行，然后点击「关闭标准输入 (EOF)」。")',
    "data = sys.stdin.read()  # 一直读到 EOF",
    'print(f"收到 {len(data.splitlines())} 行、{len(data)} 个字符。")',
    'sys.stderr.write("你输入的内容：\\n" + data + "-- EOF --\\n")',
    "",
  ].join("\n"),

  loop: [
    "import signal, time",
    "",
    "signal.signal(signal.SIGTERM, signal.SIG_IGN)  # 故意无视 SIGTERM",
    'print("我忽略了 SIGTERM，只能被 SIGKILL 终止。点「终止」试试。")',
    "i = 0",
    "while True:",
    '    print(f"tick {i}")',
    "    i += 1",
    "    time.sleep(0.5)",
    "",
  ].join("\n"),
};

// ---------- 页面状态 ----------
const $ = (id) => document.getElementById(id);
let runId = null;      // 本轮运行标识；响应里的 run_id 不匹配就丢弃（迟到回包）
let outPos = 0;
let errPos = 0;
let pollTimer = null;
let pollInFlight = false;

function setStatus(text, kind) {
  const el = $("status");
  el.textContent = text;
  el.className = "status " + (kind || "idle");
}

function setRunningUI(running) {
  $("runBtn").disabled = running;
  $("termBtn").disabled = !running;
  $("stdinLine").disabled = !running;
  $("sendBtn").disabled = !running;
  $("eofBtn").disabled = !running;
}

function setStdinClosedUI() {
  $("stdinLine").disabled = true;
  $("sendBtn").disabled = true;
  $("eofBtn").disabled = true;
  $("stdinLine").placeholder = "标准输入已关闭 (EOF)";
}

function appendText(paneId, text) {
  const pane = $(paneId);
  const nearBottom =
    pane.scrollTop + pane.clientHeight >= pane.scrollHeight - 40;
  pane.appendChild(document.createTextNode(text));
  if (nearBottom) pane.scrollTop = pane.scrollHeight;
}

function appendMeta(paneId, text, cls) {
  const pane = $(paneId);
  const span = document.createElement("span");
  span.className = cls || "meta";
  span.textContent = text;
  pane.appendChild(span);
  pane.scrollTop = pane.scrollHeight;
}

// ---------- 轮询 ----------
async function poll() {
  if (!runId || pollInFlight) return;
  pollInFlight = true;
  try {
    const resp = await fetch(
      `/api/runs/${runId}/poll?out_pos=${outPos}&err_pos=${errPos}`
    );
    if (!resp.ok) {
      if (resp.status === 404) {
        stopPolling();
        runId = null;
        setRunningUI(false);
        setStatus("运行信息已丢失（服务可能已重启）", "error");
      }
      return;
    }
    const data = await resp.json();
    if (data.run_id !== runId) return; // 迟到回包，不得混入本轮页面
    if (data.out) appendText("stdoutPane", data.out);
    outPos = data.out_pos;
    if (data.err) appendText("stderrPane", data.err);
    errPos = data.err_pos;
    if (data.stdin_closed) setStdinClosedUI();
    if (data.state === "finished") {
      stopPolling();
      setRunningUI(false);
      setStdinClosedUI();
      const how = data.terminated ? "被终止" : "正常退出";
      setStatus(`已结束（${how}）· 退出码 = ${data.exit_code}`, "finished");
      appendMeta("stdoutPane", `\n── 进程${how}，退出码 ${data.exit_code} ──\n`);
      runId = null; // 本轮结束，允许重跑
    } else {
      setStatus(`运行中 · id=${data.run_id.slice(0, 8)}…`, "running");
    }
  } catch (e) {
    // 网络抖动，下一轮再试
  } finally {
    pollInFlight = false;
  }
}

function startPolling() {
  stopPolling();
  pollTimer = setInterval(poll, 250);
  poll();
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

// ---------- 操作 ----------
async function startRun() {
  const code = $("code").value;
  if (!code.trim()) {
    setStatus("代码不能为空", "error");
    return;
  }
  $("runBtn").disabled = true;
  try {
    const resp = await fetch("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code }),
    });
    const data = await resp.json();
    if (resp.status === 201) {
      // 新一轮：清空两栏与位置，旧 run 的迟到回包因 run_id 不符被丢弃
      runId = data.run_id;
      outPos = 0;
      errPos = 0;
      $("stdoutPane").textContent = "";
      $("stderrPane").textContent = "";
      $("stdinLine").placeholder = "输入一行文本，回车发送";
      setRunningUI(true);
      setStatus(`运行中 · id=${runId.slice(0, 8)}…`, "running");
      startPolling();
    } else {
      $("runBtn").disabled = false;
      setStatus(`无法启动：${data.error || resp.status}`, "error");
    }
  } catch (e) {
    $("runBtn").disabled = false;
    setStatus("无法连接服务器", "error");
  }
}

async function sendLine() {
  if (!runId) return;
  const input = $("stdinLine");
  const line = input.value;
  input.value = "";
  const resp = await fetch(`/api/runs/${runId}/stdin`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ line }),
  });
  const data = await resp.json();
  if (resp.ok) {
    appendMeta("stdoutPane", `» ${line}\n`, "echo");
  } else {
    setStatus(`写入被拒绝：${data.error || resp.status}`, "error");
  }
  input.focus();
}

async function closeStdin() {
  if (!runId) return;
  const resp = await fetch(`/api/runs/${runId}/stdin/close`, { method: "POST" });
  if (resp.ok) {
    setStdinClosedUI();
    appendMeta("stdoutPane", "── 已关闭标准输入 (EOF) ──\n");
  } else {
    const data = await resp.json();
    setStatus(`关闭失败：${data.error || resp.status}`, "error");
  }
}

async function terminateRun() {
  if (!runId) return;
  $("termBtn").disabled = true;
  const resp = await fetch(`/api/runs/${runId}/terminate`, { method: "POST" });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({}));
    setStatus(`终止失败：${data.error || resp.status}`, "error");
  }
  // 终止接口返回时进程已被回收、输出已排空；立刻拉一次最终结果
  poll();
}

// ---------- 事件绑定 ----------
$("runBtn").addEventListener("click", startRun);
$("termBtn").addEventListener("click", terminateRun);
$("sendBtn").addEventListener("click", sendLine);
$("eofBtn").addEventListener("click", closeStdin);
$("stdinLine").addEventListener("keydown", (e) => {
  if (e.key === "Enter") sendLine();
});
$("loadExampleBtn").addEventListener("click", () => {
  $("code").value = EXAMPLES[$("examples").value] || "";
});
$("examples").addEventListener("change", () => {
  $("code").value = EXAMPLES[$("examples").value] || "";
});

// 默认载入第一个示例
$("code").value = EXAMPLES.echo;
