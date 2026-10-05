"""与 Python 程序实时对话的网页后端。

- 粘贴的代码写入临时文件，在独立子进程中执行，启动后立即返回运行标识。
- stdout / stderr 各由一个线程读取，按 UTF-8 增量解码（跨读边界的
  多字节字符会被补齐，不丢字、不乱码），未换行的片段也立即入队。
- 全站同时仅一个未结束运行；旧 run_id 对新进程一律无效。
- 正常退出与主动终止（SIGKILL，不等程序自行退出）都会先排空两路输出、
  回收进程，之后才公布退出码并允许重跑。
- 状态全部在内存中，进程重启即清空。仅供本机可信代码使用，无安全沙箱。
"""

import codecs
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static", static_url_path="")

STATE_RUNNING = "running"
STATE_FINISHED = "finished"

CHUNK = 4096               # 每次从管道读取的原始字节数
LONG_POLL_SECONDS = 20     # /events 长轮询最长等待

_state_lock = threading.Lock()
_current_run = None        # 当前（可能是唯一未结束的）运行


class Run:
    """一次子进程运行：两路输出增量解码成事件流，退出码最后公布。"""

    def __init__(self, code):
        self.id = uuid.uuid4().hex
        self.events = []            # [{seq, stream, data}]，seq 从 1 递增
        self.seq = 0
        self.cond = threading.Condition()
        self.state = STATE_RUNNING
        self.exit_code = None
        self.stdin_closed = False

        self.tmpdir = tempfile.mkdtemp(prefix="webrun_")
        self.script = os.path.join(self.tmpdir, "main.py")
        with open(self.script, "w", encoding="utf-8") as f:
            f.write(code)

        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-u", self.script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            cwd=self.tmpdir,
            env=env,
        )
        self.pumps = [
            threading.Thread(target=self._pump,
                             args=("stdout", self.proc.stdout), daemon=True),
            threading.Thread(target=self._pump,
                             args=("stderr", self.proc.stderr), daemon=True),
        ]
        for t in self.pumps:
            t.start()
        threading.Thread(target=self._reaper, daemon=True).start()

    def _emit(self, stream, data):
        with self.cond:
            self.seq += 1
            self.events.append({"seq": self.seq, "stream": stream, "data": data})
            self.cond.notify_all()

    def _pump(self, stream, pipe):
        # 增量解码器：多字节 UTF-8 字符被两次 read 切开时会先缓存、
        # 等后半截到达后再输出，因此不丢字也不乱码；非法字节替换为 U+FFFD。
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            while True:
                chunk = os.read(pipe.fileno(), CHUNK)
                if not chunk:          # EOF：子进程关闭了这一路
                    break
                text = decoder.decode(chunk)
                if text:
                    self._emit(stream, text)
            tail = decoder.decode(b"", final=True)
            if tail:
                self._emit(stream, tail)
        except OSError:
            pass
        finally:
            try:
                pipe.close()
            except OSError:
                pass

    def _reaper(self):
        # 先等两路输出都读到 EOF（排空），再 wait() 回收进程，
        # 全部完成后才公布退出码——保证退出码之后不会再有迟到的输出。
        for t in self.pumps:
            t.join()
        code = self.proc.wait()
        with self.cond:
            self.exit_code = code
            self.state = STATE_FINISHED
            self.seq += 1
            self.events.append({"seq": self.seq, "stream": "meta",
                                "data": {"exit_code": code}})
            self.cond.notify_all()
        shutil.rmtree(self.tmpdir, ignore_errors=True)


def _get_run(run_id):
    """只有与当前运行匹配的 run_id 才有效；旧标识控制不了新进程。"""
    with _state_lock:
        run = _current_run
    if run is None or run.id != run_id:
        return None
    return run


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.post("/run")
def start_run():
    global _current_run
    code = (request.get_json(force=True, silent=True) or {}).get("code", "")
    if not code.strip():
        return jsonify({"error": "代码为空"}), 400
    with _state_lock:
        if _current_run is not None and _current_run.state == STATE_RUNNING:
            return jsonify({"error": "已有运行未结束，请先终止"}), 409
        run = Run(code)
        _current_run = run
    # 子进程已启动，立即返回运行标识
    return jsonify({"run_id": run.id})


@app.get("/events")
def events():
    """长轮询：返回 seq > after 的新事件；run_id 不匹配即返回 409。"""
    run_id = request.args.get("run_id", "")
    try:
        after = int(request.args.get("after", "0"))
    except ValueError:
        after = 0
    run = _get_run(run_id)
    if run is None:
        return jsonify({"error": "运行标识已失效", "status": "stale"}), 409
    deadline = time.monotonic() + LONG_POLL_SECONDS
    with run.cond:
        while (run.seq <= after and run.state == STATE_RUNNING
               and time.monotonic() < deadline):
            run.cond.wait(timeout=0.5)
        fresh = [e for e in run.events if e["seq"] > after]
        return jsonify({
            "run_id": run.id,
            "status": run.state,
            "exit_code": run.exit_code,
            "stdin_closed": run.stdin_closed,
            "events": fresh,
        })


@app.post("/input")
def send_input():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    line = data.get("line", "")
    with run.cond:
        if run.state != STATE_RUNNING:
            return jsonify({"error": "运行已结束"}), 409
        if run.stdin_closed:
            return jsonify({"error": "标准输入已关闭，写入被拒绝"}), 409
        try:
            run.proc.stdin.write((line + "\n").encode("utf-8"))
            run.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            return jsonify({"error": "子进程已关闭标准输入"}), 409
    return jsonify({"ok": True})


@app.post("/stdin/close")
def close_stdin():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    with run.cond:
        if not run.stdin_closed:
            run.stdin_closed = True
            try:
                run.proc.stdin.close()   # 向子进程发送 EOF
            except OSError:
                pass
    return jsonify({"ok": True})


@app.post("/terminate")
def terminate():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    try:
        run.proc.kill()   # SIGKILL：立即终止，不等程序自行退出
    except OSError:
        pass
    return jsonify({"ok": True})


@app.get("/status")
def status():
    with _state_lock:
        run = _current_run
    if run is None:
        return jsonify({"state": "idle"})
    return jsonify({"state": run.state, "run_id": run.id,
                    "exit_code": run.exit_code})


if __name__ == "__main__":
    # threaded=True：等待输入或一路无输出时，其余请求仍可并发处理
    app.run(host="0.0.0.0", port=8000, threaded=True)
