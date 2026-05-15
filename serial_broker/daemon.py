from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import select
import selectors
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import serial

from .logs import SerialLogs, ensure_socket_removed
from .protocol import DEFAULT_SOCKET, recv_json_line, result, send_json_line
from .usb import reset_usb_device, wait_for_path


IDLE = "IDLE"


class Broker:
    def __init__(self, serial_path: str, baud: int, socket_path: str, log_dir: str, ring_lines: int) -> None:
        self.serial_path = serial_path
        self.baud = baud
        self.socket_path = socket_path
        self.logs = SerialLogs(log_dir, ring_lines)
        self.started = time.time()
        self.state = IDLE
        self.current_job: dict[str, Any] | None = None
        self.lock_owner: str | None = None
        self.last_error: str | None = None
        self._job_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._cancel = threading.Event()
        self._stop = threading.Event()
        self._bridge_active = threading.Event()
        self._serial_lock = threading.Lock()
        self.ser: serial.Serial | None = None
        self.reader_thread: threading.Thread | None = None

    def start(self) -> None:
        self._open_serial()
        self.reader_thread = threading.Thread(target=self._reader_loop, name="serial-reader", daemon=True)
        self.reader_thread.start()
        self._serve()

    def _open_serial(self) -> None:
        with self._serial_lock:
            self.ser = serial.Serial(
                self.serial_path,
                self.baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.1,
                xonxoff=False,
                rtscts=False,
                dsrdtr=False,
            )
            self.ser.reset_input_buffer()
            self.last_error = None

    def _close_serial(self) -> None:
        with self._serial_lock:
            if self.ser:
                try:
                    self.ser.close()
                finally:
                    self.ser = None

    def _reader_loop(self) -> None:
        while not self._stop.is_set():
            if self._bridge_active.is_set():
                time.sleep(0.02)
                continue
            ser = self.ser
            if not ser or not ser.is_open:
                time.sleep(0.1)
                continue
            try:
                data = ser.read(4096)
                if data:
                    self.logs.write_raw(data, text=True)
            except Exception as exc:
                self.last_error = str(exc)
                time.sleep(0.2)

    def _serve(self) -> None:
        ensure_socket_removed(self.socket_path)
        Path(self.socket_path).parent.mkdir(parents=True, exist_ok=True)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(self.socket_path)
        os.chmod(self.socket_path, 0o660)
        srv.listen(20)
        srv.settimeout(0.5)
        self.logs.event(f"[BROKER_START] serial={self.serial_path} baud={self.baud}")
        try:
            while not self._stop.is_set():
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()
        finally:
            srv.close()
            ensure_socket_removed(self.socket_path)
            self._close_serial()
            self.logs.close()

    def _handle_client(self, conn: socket.socket) -> None:
        with conn:
            fp = conn.makefile("rb")
            try:
                req = recv_json_line(fp)
                resp = self.dispatch(req)
            except Exception as exc:
                self.last_error = str(exc)
                resp = result(ok=False, operation="unknown", state=self.state, error=str(exc))
            send_json_line(conn, resp)

    def dispatch(self, req: dict[str, Any]) -> dict[str, Any]:
        op = req.get("op")
        if op == "status":
            return self.status()
        if op == "tail":
            return result(ok=True, operation=op, state=self.state, lines=self.logs.tail(int(req.get("n", 200))))
        if op == "grep":
            return result(ok=True, operation=op, state=self.state, matches=self.logs.grep(req.get("pattern", ""), full=bool(req.get("full"))))
        if op == "wait":
            return self.wait(req.get("pattern", ""), float(req.get("timeout", 60)))
        if op == "run":
            return self.with_job("CMD_JOB", op, lambda: self.run_command(req.get("cmd", ""), float(req.get("timeout", 30))))
        if op == "send":
            return self.with_job("CMD_JOB", op, lambda: self.send_line(req.get("text", ""), bool(req.get("newline", True))))
        if op == "upload":
            method = req.get("method", "zmodem")
            state = "UPLOAD_BASE64" if method == "base64" else "UPLOAD_ZMODEM"
            timeout = self._upload_timeout(req["local"], req.get("timeout"))
            return self.with_job(state, op, lambda: self.upload(req["local"], req["remote"], method, timeout))
        if op == "reset-board":
            return self.with_job("RESET_BOARD", op, self.reset_board)
        if op == "reset-usb":
            return self.with_job("RESET_USB", op, lambda: self.reset_usb(float(req.get("timeout", 20))))
        if op == "recover":
            return self.with_job("RECOVERY", op, lambda: self.recover(req))
        if op == "cancel":
            self._cancel.set()
            return result(ok=True, operation=op, state=self.state)
        if op == "force-unlock":
            self.logs.event("[WARN] force-unlock requested")
            self._cancel.set()
            if self._job_lock.locked():
                try:
                    self._job_lock.release()
                except RuntimeError:
                    pass
            self._set_state(IDLE, None, None)
            return result(ok=True, operation=op, state=self.state)
        return result(ok=False, operation=str(op), state=self.state, error="unknown operation")

    def status(self) -> dict[str, Any]:
        return result(
            ok=True,
            operation="status",
            state=self.state,
            serial_path=self.serial_path,
            baud=self.baud,
            current_job=self.current_job,
            lock_owner=self.lock_owner,
            log_file=str(self.logs.text_path),
            raw_log_file=str(self.logs.raw_path),
            ring_buffer_line_count=self.logs.line_count,
            daemon_uptime=round(time.time() - self.started, 3),
            last_error=self.last_error,
        )

    def _set_state(self, state: str, job: dict[str, Any] | None, owner: str | None) -> None:
        with self._state_lock:
            self.state = state
            self.current_job = job
            self.lock_owner = owner

    def with_job(self, state: str, op: str, fn) -> dict[str, Any]:
        owner = str(uuid.uuid4())
        if not self._job_lock.acquire(blocking=False):
            return result(ok=False, operation=op, state=self.state, error=f"BUSY: current state is {self.state}")
        self._cancel.clear()
        self._set_state(state, {"operation": op, "owner": owner, "started": time.time()}, owner)
        begin = self.logs.raw_offset
        start = time.monotonic()
        try:
            out = fn()
            out.setdefault("ok", True)
            out.setdefault("operation", op)
            out["state"] = self.state
            out.setdefault("error", None)
            out["log_file"] = str(self.logs.text_path)
            out["log_offset_begin"] = begin
            out["log_offset_end"] = self.logs.raw_offset
            out["duration_ms"] = int((time.monotonic() - start) * 1000)
            return out
        except Exception as exc:
            self.last_error = str(exc)
            return result(
                ok=False,
                operation=op,
                state=self.state,
                error=str(exc),
                log_file=str(self.logs.text_path),
                log_offset_begin=begin,
                log_offset_end=self.logs.raw_offset,
                duration_ms=int((time.monotonic() - start) * 1000),
                recent_log="\n".join(x["text"] for x in self.logs.tail(30)),
            )
        finally:
            self._set_state(IDLE, None, None)
            self._cancel.clear()
            try:
                self._job_lock.release()
            except RuntimeError:
                pass

    def _write_serial(self, data: bytes) -> None:
        ser = self.ser
        if not ser or not ser.is_open:
            raise RuntimeError("serial is not open")
        with self._serial_lock:
            ser.write(data)
            ser.flush()

    def wait(self, pattern: str, timeout: float) -> dict[str, Any]:
        seq = self.logs.seq
        ok, text = self.logs.wait_for(pattern, seq, timeout, self._cancel)
        return result(ok=ok, operation="wait", state=self.state, error=None if ok else "timeout", output=text)

    def run_command(self, cmd: str, timeout: float) -> dict[str, Any]:
        token = uuid.uuid4().hex
        begin_marker = f"__SB_BEGIN_{token}__"
        end_marker = f"__SB_END_{token}__:"
        self._write_serial(b"stty -echo 2>/dev/null || true\r")
        time.sleep(0.15)
        seq = self.logs.seq
        script = "\r".join(
            [
                f"printf '%s\\n' {shlex.quote(begin_marker)}",
                f"( {cmd} )",
                "__sb_ec=$?",
                f"printf '%s%s\\n' {shlex.quote(end_marker)} \"$__sb_ec\"",
                "stty echo 2>/dev/null || true",
                "",
            ]
        )
        self._write_serial(script.encode("utf-8"))
        ok, text = self._wait_for_regex(re.escape(end_marker) + r"\d+", seq, timeout)
        if not ok:
            return {"ok": False, "operation": "run", "error": "timeout", "output": text, "exit_code": None}
        return self._parse_run_output(text, begin_marker, end_marker)

    def _wait_for_regex(self, pattern: str, seq: int, timeout: float) -> tuple[bool, str]:
        deadline = time.monotonic() + timeout
        compiled = re.compile(pattern)
        while True:
            text = self.logs.text_since(seq)
            if compiled.search(text):
                return True, text
            if self._cancel.is_set():
                return False, text
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, text
            time.sleep(min(0.1, remaining))

    def send_line(self, text: str, newline: bool = True) -> dict[str, Any]:
        data = text.encode("utf-8") + (b"\r" if newline else b"")
        self._write_serial(data)
        return {"ok": True, "operation": "send", "bytes": len(data)}

    def _parse_run_output(self, text: str, begin_marker: str, end_marker: str) -> dict[str, Any]:
        begin_idx = text.find(begin_marker)
        end_idx = text.find(end_marker, begin_idx + len(begin_marker))
        if begin_idx < 0 or end_idx < 0:
            return {"ok": False, "operation": "run", "error": "missing marker", "output": text, "exit_code": None}
        after_end = text[end_idx + len(end_marker) :].splitlines()[0].strip()
        try:
            exit_code = int(after_end)
        except ValueError:
            exit_code = None
        body = text[begin_idx + len(begin_marker) : end_idx]
        lines = [self._clean_output_line(line) for line in body.splitlines() if line.strip() and not line.startswith("__SB_")]
        lines = [line for line in lines if line]
        return {"ok": exit_code == 0, "operation": "run", "error": None if exit_code == 0 else f"exit_code={exit_code}", "output": "\n".join(lines), "exit_code": exit_code}

    def _clean_output_line(self, line: str) -> str:
        line = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", line)
        line = re.sub(r"\x1b\][^\x07]*(?:\x07|\x1b\\)", "", line)
        line = "".join(ch for ch in line if ch == "\t" or ch >= " ")
        stripped = line.strip()
        if re.match(r"^[^@\s]+@[^:\s]+:.*[$#]$", stripped):
            return ""
        return stripped

    def upload(self, local: str, remote: str, method: str, timeout: float) -> dict[str, Any]:
        shell = self.ensure_target_shell()
        if not shell.get("ok"):
            return {
                "ok": False,
                "operation": "upload",
                "method": method,
                "error": shell["error"],
                "output": shell.get("output", ""),
                "suggestion": "target is not at a Linux shell; use sbctl wait/send to reach a shell, or run sbctl reset-usb / sbctl recover --board --usb --wait \"login:\" if the target is stuck",
            }
        if method == "base64":
            return self.upload_base64(local, remote, timeout)
        if method != "zmodem":
            raise RuntimeError(f"unknown upload method: {method}")
        return self.upload_zmodem(local, remote, timeout)

    def _upload_timeout(self, local: str, requested: Any) -> float:
        if requested is not None:
            return float(requested)
        try:
            size = os.path.getsize(local)
        except OSError:
            return 300.0
        bytes_per_second = max(self.baud / 12.0, 1.0)
        return max(300.0, (size / bytes_per_second) + 90.0)

    def ensure_target_shell(self) -> dict[str, Any]:
        probe = self.run_command("printf SB_SHELL_READY; uname -s", 8)
        output = probe.get("output", "")
        if probe.get("ok") and "SB_SHELL_READY" in output and "Linux" in output:
            return {"ok": True, "output": output}
        return {
            "ok": False,
            "error": "target is not at a working Linux shell",
            "output": output,
        }

    def _sha256_file(self, path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def upload_base64(self, local: str, remote: str, timeout: float) -> dict[str, Any]:
        local_sha = self._sha256_file(local)
        remote_q = shlex.quote(remote)
        b64_remote = shlex.quote(remote + ".b64")
        remote_dir = shlex.quote(str(Path(remote).parent))
        data = base64.b64encode(Path(local).read_bytes()).decode("ascii")
        self.run_command(f"mkdir -p {remote_dir} && : > {b64_remote}", timeout)
        chunk_size = 4096
        for i in range(0, len(data), chunk_size):
            if self._cancel.is_set():
                raise RuntimeError("cancelled")
            chunk = data[i : i + chunk_size]
            resp = self.run_command(f"printf %s {shlex.quote(chunk)} >> {b64_remote}", timeout)
            if not resp.get("ok"):
                raise RuntimeError(resp.get("error") or "base64 chunk failed")
        cmd = f"base64 -d {b64_remote} > {remote_q} && sha256sum {remote_q} && rm -f {b64_remote}"
        resp = self.run_command(cmd, timeout)
        remote_sha = ""
        for part in resp.get("output", "").split():
            if len(part) == 64:
                remote_sha = part
                break
        ok = bool(resp.get("ok")) and remote_sha == local_sha
        return {"ok": ok, "operation": "upload", "method": "base64", "error": None if ok else "sha256 mismatch or upload failed", "sha256": local_sha, "remote_sha256": remote_sha, "size": os.path.getsize(local), "output": resp.get("output", "")}

    def upload_zmodem(self, local: str, remote: str, timeout: float) -> dict[str, Any]:
        local_sha = self._sha256_file(local)
        remote_path = Path(remote)
        remote_dir = str(remote_path.parent)
        remote_dir_q = shlex.quote(remote_dir)
        basename = remote_path.name
        basename_q = shlex.quote(basename)
        remote_tmp_dir = f"/tmp/sbup-{uuid.uuid4().hex[:8]}"
        remote_tmp_dir_q = shlex.quote(remote_tmp_dir)
        remote_tmp_path_q = shlex.quote(f"{remote_tmp_dir}/{basename}")
        self.logs.event(f"[UPLOAD_ZMODEM_BEGIN] local={local} remote={remote}")
        probe = self.run_command("command -v rz && command -v sha256sum", 10)
        if not probe.get("ok"):
            self.logs.event(f"[UPLOAD_ZMODEM_END] ok=false error=target shell/rz probe failed")
            return {
                "ok": False,
                "operation": "upload",
                "method": "zmodem",
                "error": "target is not at a working shell or rz/sha256sum is missing",
                "output": probe.get("output", ""),
            }
        prep = self.run_command(f"mkdir -p {remote_dir_q} {remote_tmp_dir_q}", 20)
        if not prep.get("ok"):
            self.logs.event(f"[UPLOAD_ZMODEM_END] ok=false error=mkdir failed")
            return {
                "ok": False,
                "operation": "upload",
                "method": "zmodem",
                "error": "failed to create remote directory",
                "output": prep.get("output", ""),
            }
        seq = self.logs.seq
        self._write_serial(f"cd {remote_tmp_dir_q} && rz -y -b -e\r".encode("utf-8"))
        ready, ready_text = self._wait_for_regex(r"rz waiting to receive|\*\*\x18B", seq, 15)
        if not ready:
            self.logs.event("[UPLOAD_ZMODEM_END] ok=false error=rz did not become ready")
            self.run_command(f"rm -rf {remote_tmp_dir_q}", 5)
            return {
                "ok": False,
                "operation": "upload",
                "method": "zmodem",
                "error": "target rz did not become ready; target may not be at shell",
                "output": ready_text,
            }
        send_path = local
        temp_dir = None
        if Path(local).name != basename:
            temp_dir = tempfile.TemporaryDirectory()
            send_path = str(Path(temp_dir.name) / basename)
            os.symlink(os.path.abspath(local), send_path)
        try:
            sz = subprocess.Popen(["sz", "-b", "-e", "-y", send_path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError:
            if temp_dir:
                temp_dir.cleanup()
            raise RuntimeError("sz not found on host")
        try:
            self._bridge_active.set()
            bridge_ok = self._bridge_process(sz, timeout)
        finally:
            self._bridge_active.clear()
            if temp_dir:
                temp_dir.cleanup()
        if not bridge_ok or sz.returncode not in (0, None):
            err = sz.stderr.read().decode("utf-8", "replace") if sz.stderr else ""
            self.logs.event(f"[UPLOAD_ZMODEM_END] ok=false error={err.strip()}")
            self.run_command(f"rm -rf {remote_tmp_dir_q}", 5)
            return {"ok": False, "operation": "upload", "method": "zmodem", "error": err or "zmodem failed"}
        tmp_verify = self.run_command(f"cd {remote_tmp_dir_q} && sha256sum {basename_q}", 20)
        tmp_sha = ""
        for part in tmp_verify.get("output", "").split():
            if len(part) == 64:
                tmp_sha = part
                break
        if not tmp_verify.get("ok") or tmp_sha != local_sha:
            self.run_command(f"rm -rf {remote_tmp_dir_q}", 5)
            self.logs.event("[UPLOAD_ZMODEM_END] ok=false error=temp sha256 mismatch")
            return {
                "ok": False,
                "operation": "upload",
                "method": "zmodem",
                "error": "temporary upload sha256 mismatch",
                "sha256": local_sha,
                "remote_sha256": tmp_sha,
                "size": os.path.getsize(local),
                "output": tmp_verify.get("output", ""),
            }
        move = self.run_command(f"mv -f {remote_tmp_path_q} {remote_dir_q}/", 20)
        if not move.get("ok"):
            self.run_command(f"rm -rf {remote_tmp_dir_q}", 5)
            self.logs.event("[UPLOAD_ZMODEM_END] ok=false error=move failed")
            return {
                "ok": False,
                "operation": "upload",
                "method": "zmodem",
                "error": "failed to move uploaded file to remote path",
                "sha256": local_sha,
                "remote_sha256": tmp_sha,
                "size": os.path.getsize(local),
                "output": move.get("output", ""),
            }
        self.run_command(f"rmdir {remote_tmp_dir_q} 2>/dev/null || true", 5)
        verify = self.run_command(f"cd {remote_dir_q} && sha256sum {basename_q}", 20)
        remote_sha = ""
        for part in verify.get("output", "").split():
            if len(part) == 64:
                remote_sha = part
                break
        ok = bool(verify.get("ok")) and remote_sha == local_sha
        self.logs.event(f"[UPLOAD_ZMODEM_END] ok={str(ok).lower()} size={os.path.getsize(local)} sha256={local_sha}")
        return {"ok": ok, "operation": "upload", "method": "zmodem", "error": None if ok else "sha256 mismatch or zmodem failed", "sha256": local_sha, "remote_sha256": remote_sha, "size": os.path.getsize(local), "output": verify.get("output", "")}

    def _bridge_process(self, proc: subprocess.Popen, timeout: float) -> bool:
        ser = self.ser
        if not ser or not ser.is_open or not proc.stdin or not proc.stdout:
            raise RuntimeError("bridge endpoints unavailable")
        sel = selectors.DefaultSelector()
        sel.register(ser.fileno(), selectors.EVENT_READ, "serial")
        sel.register(proc.stdout.fileno(), selectors.EVENT_READ, "sz")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._cancel.is_set():
                proc.terminate()
                return False
            if proc.poll() is not None:
                return proc.returncode == 0
            for key, _ in sel.select(0.1):
                if key.data == "serial":
                    try:
                        data = os.read(ser.fileno(), 4096)
                    except BlockingIOError:
                        continue
                    if data:
                        self.logs.write_raw(data, text=False)
                        proc.stdin.write(data)
                        proc.stdin.flush()
                else:
                    try:
                        data = os.read(proc.stdout.fileno(), 4096)
                    except BlockingIOError:
                        continue
                    if data:
                        self._write_fd_all(ser.fileno(), data)
                        self.logs.write_raw(data, text=False)
        proc.terminate()
        return False

    def _write_fd_all(self, fd: int, data: bytes) -> None:
        view = memoryview(data)
        total = 0
        while total < len(data):
            try:
                written = os.write(fd, view[total:])
            except BlockingIOError:
                time.sleep(0.01)
                continue
            if written <= 0:
                raise RuntimeError("short write to serial fd")
            total += written

    def reset_board(self) -> dict[str, Any]:
        audio = Path(__file__).parent / "assets" / "reset-reminder.wav"
        msg = "PLEASE PRESS THE TARGET BOARD RESET BUTTON NOW. Press Enter here after reset is done."
        self.logs.event(f"[RESET_BOARD_BEGIN] audio={audio}")
        if not sys.stdin.isatty():
            self.logs.event("[RESET_BOARD_END] ok=false error=no interactive daemon stdin")
            return {"ok": False, "operation": "reset-board", "error": "serial-broker stdin is not interactive; cannot confirm manual reset"}
        print("\n" + "=" * 72, flush=True)
        print(msg, flush=True)
        print("=" * 72, flush=True)
        player = self._audio_player_cmd(audio)
        volume_state = self._capture_volume_state()
        self._set_max_volume(volume_state)
        last_print = 0.0
        try:
            while not self._cancel.is_set():
                now = time.monotonic()
                if now - last_print >= 2.0:
                    print("\a[RESET_BOARD] Press the target board reset button, then press Enter here.", flush=True)
                    self.logs.event("[RESET_BOARD] waiting for manual confirmation")
                    last_print = now
                proc = None
                if player:
                    try:
                        proc = subprocess.Popen(player, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    except Exception as exc:
                        self.logs.event(f"[RESET_BOARD_AUDIO_ERROR] {exc}")
                        player = None
                wait_until = time.monotonic() + 2.0
                while time.monotonic() < wait_until and not self._cancel.is_set():
                    if proc and proc.poll() is not None:
                        proc = None
                    readable, _, _ = select.select([sys.stdin], [], [], 0.2)
                    if readable:
                        sys.stdin.readline()
                        if proc and proc.poll() is None:
                            proc.terminate()
                        self.logs.event("[RESET_BOARD_END] ok=true")
                        return {"ok": True, "operation": "reset-board", "message": "manual reset confirmed"}
                if proc and proc.poll() is None:
                    proc.terminate()
            self.logs.event("[RESET_BOARD_END] ok=false error=cancelled")
            return {"ok": False, "operation": "reset-board", "error": "cancelled"}
        finally:
            self._restore_volume_state(volume_state)

    def _audio_player_cmd(self, audio: Path) -> list[str] | None:
        if not audio.exists():
            self.logs.event(f"[RESET_BOARD_AUDIO_MISSING] {audio}")
            return None
        candidates = [
            ["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", str(audio)],
            ["mpg123", "-q", str(audio)],
            ["mpv", "--no-video", "--really-quiet", str(audio)],
            ["play", "-q", str(audio)],
            ["paplay", str(audio)],
            ["aplay", "-q", str(audio)],
        ]
        for cmd in candidates:
            if shutil.which(cmd[0]):
                return cmd
        self.logs.event("[RESET_BOARD_AUDIO_MISSING_PLAYER]")
        return None

    def _capture_volume_state(self) -> dict[str, str] | None:
        if shutil.which("pactl"):
            try:
                volume = subprocess.check_output(
                    ["pactl", "get-sink-volume", "@DEFAULT_SINK@"],
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                mute = subprocess.check_output(
                    ["pactl", "get-sink-mute", "@DEFAULT_SINK@"],
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                match = re.search(r"(\d+)%", volume)
                mute_match = re.search(r"\b(yes|no)\b", mute)
                if match:
                    return {
                        "tool": "pactl",
                        "volume": f"{match.group(1)}%",
                        "mute": mute_match.group(1) if mute_match else "no",
                    }
            except Exception as exc:
                self.logs.event(f"[RESET_BOARD_VOLUME_CAPTURE_ERROR] pactl: {exc}")
        if shutil.which("amixer"):
            try:
                output = subprocess.check_output(
                    ["amixer", "get", "Master"],
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                volume_match = re.search(r"\[(\d+)%\]", output)
                mute_match = re.search(r"\[(on|off)\]", output)
                if volume_match:
                    return {
                        "tool": "amixer",
                        "volume": f"{volume_match.group(1)}%",
                        "mute": mute_match.group(1) if mute_match else "on",
                    }
            except Exception as exc:
                self.logs.event(f"[RESET_BOARD_VOLUME_CAPTURE_ERROR] amixer: {exc}")
        self.logs.event("[RESET_BOARD_VOLUME_UNAVAILABLE]")
        return None

    def _set_max_volume(self, state: dict[str, str] | None) -> None:
        if not state:
            return
        try:
            if state["tool"] == "pactl":
                subprocess.run(["pactl", "set-sink-mute", "@DEFAULT_SINK@", "0"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.run(["pactl", "set-sink-volume", "@DEFAULT_SINK@", "100%"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.logs.event("[RESET_BOARD_VOLUME_MAX] tool=pactl")
            elif state["tool"] == "amixer":
                subprocess.run(["amixer", "sset", "Master", "100%", "unmute"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.logs.event("[RESET_BOARD_VOLUME_MAX] tool=amixer")
        except Exception as exc:
            self.logs.event(f"[RESET_BOARD_VOLUME_MAX_ERROR] {exc}")

    def _restore_volume_state(self, state: dict[str, str] | None) -> None:
        if not state:
            return
        try:
            if state["tool"] == "pactl":
                subprocess.run(["pactl", "set-sink-volume", "@DEFAULT_SINK@", state["volume"]], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.run(["pactl", "set-sink-mute", "@DEFAULT_SINK@", "1" if state.get("mute") == "yes" else "0"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.logs.event(f"[RESET_BOARD_VOLUME_RESTORE] tool=pactl volume={state['volume']} mute={state.get('mute')}")
            elif state["tool"] == "amixer":
                mute_arg = "mute" if state.get("mute") == "off" else "unmute"
                subprocess.run(["amixer", "sset", "Master", state["volume"], mute_arg], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.logs.event(f"[RESET_BOARD_VOLUME_RESTORE] tool=amixer volume={state['volume']} mute={state.get('mute')}")
        except Exception as exc:
            self.logs.event(f"[RESET_BOARD_VOLUME_RESTORE_ERROR] {exc}")

    def reset_usb(self, timeout: float) -> dict[str, Any]:
        self.logs.event(f"[RESET_USB_BEGIN] serial={self.serial_path}")
        self._close_serial()
        reset_usb_device(self.serial_path)
        if not wait_for_path(self.serial_path, timeout):
            raise RuntimeError(f"serial path did not reappear: {self.serial_path}")
        self._open_serial()
        self.logs.event("[RESET_USB_END] ok=true")
        return {"ok": True, "operation": "reset-usb"}

    def recover(self, req: dict[str, Any]) -> dict[str, Any]:
        if req.get("board"):
            self.reset_board()
        if req.get("usb"):
            self.reset_usb(float(req.get("timeout", 80)))
        pattern = req.get("wait")
        if pattern:
            seq = self.logs.seq
            ok, text = self.logs.wait_for(pattern, seq, float(req.get("timeout", 80)), self._cancel)
            return {"ok": ok, "operation": "recover", "error": None if ok else "timeout", "output": text}
        return {"ok": True, "operation": "recover"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serial", required=True)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--socket", default=DEFAULT_SOCKET)
    parser.add_argument("--log-dir", default="./logs")
    parser.add_argument("--ring-lines", type=int, default=2000)
    args = parser.parse_args()
    broker = Broker(args.serial, args.baud, args.socket, args.log_dir, args.ring_lines)
    signal.signal(signal.SIGTERM, lambda *_: broker._stop.set())
    signal.signal(signal.SIGINT, lambda *_: broker._stop.set())
    broker.start()


if __name__ == "__main__":
    main()
