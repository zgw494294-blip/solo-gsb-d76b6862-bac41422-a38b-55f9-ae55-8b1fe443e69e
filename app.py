"""与 Python 程序实时对话的网页（Flask 后端）。

设计要点：
- 全站同一时刻最多一个未结束运行；状态全部在内存中，重启即清空。
- 粘贴的代码在独立子进程中执行，启动后立即返回运行标识（run id）。
- stdout / stderr 各由一个守护线程读取原始字节，并分别用 UTF-8
  增量解码器解码：未换行的内容能即时显示，跨片段的多字节字符
  不会丢字也不会乱码。
- 等待输入或一路无输出时，另一路输出、状态查询和终止请求照常处理。
- 正常退出与主动终止都会先排空两路输出并回收进程，之后才公布
  退出码、允许下一次运行；终止使用 SIGTERM -> SIGKILL，绝不等
  程序自行退出。
- 旧 run id 对新进程没有任何控制权；轮询响应都带 run id，
  前端据此丢弃迟到回包。
- 仅供本机可信、不派生进程的标准库代码使用，不做安全沙箱。
"""

import codecs
import os
import subprocess
import sys
import threading
import uuid

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

state_lock = threading.Lock()
current_run = None  # 当前（可能是唯一未结束的）运行；重启后清空


class Run:
    """一次子进程运行：进程句柄 + 两路已解码输出 + 生命周期状态。"""

    def __init__(self, code: str):
        self.id = uuid.uuid4().hex
        self.lock = threading.Lock()
        self.out_chunks = []  # 已解码的 stdout 文本片段
        self.err_chunks = []  # 已解码的 stderr 文本片段
        self.stdin_closed = False
        self.terminated = False
        self.finished = False  # 进程已回收且两路输出均已排空
        self.exit_code = None
        # -u：不缓冲，输出立即可见；-I：隔离模式，忽略环境变量与用户站点
        self.proc = subprocess.Popen(
            [sys.executable, "-u", "-I", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._readers = [
            threading.Thread(
                target=self._read_stream,
                args=(self.proc.stdout, self.out_chunks),
                daemon=True,
            ),
            threading.Thread(
                target=self._read_stream,
                args=(self.proc.stderr, self.err_chunks),
                daemon=True,
            ),
        ]
        for t in self._readers:
            t.start()
        self._monitor = threading.Thread(target=self._watch, daemon=True)
        self._monitor.start()

    def _read_stream(self, stream, chunks):
        """读原始字节并增量解码；一路阻塞不影响另一路。"""
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        fd = stream.fileno()
        while True:
            try:
                data = os.read(fd, 65536)
            except OSError:
                data = b""
            if not data:  # EOF：进程关闭了这一路
                break
            text = decoder.decode(data)
            if text:
                with self.lock:
                    chunks.append(text)
        tail = decoder.decode(b"", final=True)  # 冲刷解码器尾部残留
        if tail:
            with self.lock:
                chunks.append(tail)

    def _watch(self):
        """回收进程并等两路输出排空，之后才公布退出码。"""
        self.proc.wait()  # 回收，避免僵尸进程
        for t in self._readers:
            t.join()  # 等两路都读到 EOF 并排入缓冲
        with self.lock:
            self.exit_code = self.proc.returncode
            self.finished = True

    def terminate(self):
        """主动终止：先 SIGTERM，1 秒内不退就 SIGKILL，然后等排空与回收。"""
        with self.lock:
            self.terminated = True
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
            except OSError:
                pass
            try:
                self.proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                try:
                    self.proc.kill()
                except OSError:
                    pass
        self._monitor.join(timeout=5.0)

    def write_line(self, line: str):
        """向标准输入写一行；关闭后或进程退出后写入被拒绝。"""
        with self.lock:
            if self.finished:
                return "运行已结束，写入被拒绝"
            if self.stdin_closed:
                return "标准输入已关闭，写入被拒绝"
        try:
            self.proc.stdin.write((line + "\n").encode("utf-8"))
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            return "子进程已退出，写入被拒绝"
        return None

    def close_stdin(self):
        """关闭标准输入（向子进程发送 EOF）；幂等。"""
        with self.lock:
            if self.stdin_closed:
                return False
            self.stdin_closed = True
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        return True

    def snapshot(self, out_pos: int, err_pos: int):
        """按客户端已消费的字符位置，返回两路增量与当前状态。"""
        with self.lock:
            out_text = "".join(self.out_chunks)
            err_text = "".join(self.err_chunks)
            out_pos = min(max(out_pos, 0), len(out_text))
            err_pos = min(max(err_pos, 0), len(err_text))
            return {
                "run_id": self.id,
                "state": "finished" if self.finished else "running",
                "exit_code": self.exit_code,
                "terminated": self.terminated,
                "stdin_closed": self.stdin_closed,
                "out": out_text[out_pos:],
                "out_pos": len(out_text),
                "err": err_text[err_pos:],
                "err_pos": len(err_text),
            }


def get_run(run_id):
    """只有当前运行的 id 才有效；旧 id 一律查无此运行。"""
    with state_lock:
        run = current_run
    if run is None or run.id != run_id:
        return None
    return run


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/runs")
def create_run():
    """启动一次运行，立即返回运行标识；已有未结束运行时拒绝。"""
    global current_run
    data = request.get_json(force=True, silent=True) or {}
    code = data.get("code", "")
    if not isinstance(code, str) or not code.strip():
        return jsonify(error="代码不能为空"), 400
    with state_lock:
        if current_run is not None and not current_run.finished:
            return jsonify(error="已有未结束的运行，请先终止或等待其结束",
                           run_id=current_run.id), 409
        run = Run(code)
        current_run = run
    return jsonify(run_id=run.id), 201


@app.get("/api/runs/<run_id>/poll")
def poll_run(run_id):
    """增量轮询：返回两路新输出与状态；旧 id 返回 404。"""
    run = get_run(run_id)
    if run is None:
        return jsonify(error="运行不存在或已被新一轮取代"), 404
    out_pos = request.args.get("out_pos", 0, type=int)
    err_pos = request.args.get("err_pos", 0, type=int)
    return jsonify(run.snapshot(out_pos, err_pos))


@app.post("/api/runs/<run_id>/stdin")
def write_stdin(run_id):
    """向子进程标准输入写入一行文本。"""
    run = get_run(run_id)
    if run is None:
        return jsonify(error="运行不存在或已被新一轮取代"), 404
    data = request.get_json(force=True, silent=True) or {}
    line = data.get("line", "")
    if not isinstance(line, str):
        return jsonify(error="line 必须是字符串"), 400
    line = line.replace("\r", "").replace("\n", "")  # 只接受一行
    error = run.write_line(line)
    if error:
        return jsonify(error=error), 409
    return jsonify(ok=True)


@app.post("/api/runs/<run_id>/stdin/close")
def close_stdin(run_id):
    """关闭子进程标准输入（EOF）；之后写入会被拒绝。"""
    run = get_run(run_id)
    if run is None:
        return jsonify(error="运行不存在或已被新一轮取代"), 404
    run.close_stdin()
    return jsonify(ok=True, stdin_closed=True)


@app.post("/api/runs/<run_id>/terminate")
def terminate_run(run_id):
    """终止运行：SIGTERM -> SIGKILL，排空两路输出并回收后才返回。"""
    run = get_run(run_id)
    if run is None:
        return jsonify(error="运行不存在或已被新一轮取代"), 404
    run.terminate()
    with run.lock:
        return jsonify(ok=True,
                       state="finished" if run.finished else "running",
                       exit_code=run.exit_code)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
