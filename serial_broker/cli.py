from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
import time
from typing import Any

from .protocol import DEFAULT_SOCKET, recv_json_line, send_json_line


def _send_cancel(socket_path: str) -> None:
    print("\n[Cancelling daemon job...]", flush=True, file=sys.stderr)
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect(socket_path)
        send_json_line(s, {"op": "cancel"})
        s.close()
    except Exception:
        pass


def request(socket_path: str, payload: dict[str, Any], heartbeat: bool = False) -> dict[str, Any]:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(socket_path)
        send_json_line(sock, payload)
        fp = sock.makefile("rb")
        if heartbeat:
            stop_event = threading.Event()
            def _heartbeat():
                if stop_event.wait(3):
                    return
                elapsed = 3
                while not stop_event.wait(5):
                    elapsed += 5
                    print(f"... still waiting, {elapsed}s elapsed ...", flush=True)
            t = threading.Thread(target=_heartbeat, daemon=True)
            t.start()
            try:
                line = fp.readline()
            except KeyboardInterrupt:
                stop_event.set()
                _send_cancel(socket_path)
                raise
            finally:
                stop_event.set()
        else:
            try:
                line = fp.readline()
            except KeyboardInterrupt:
                _send_cancel(socket_path)
                raise
        if not line:
            raise EOFError("socket closed")
        return json.loads(line.decode("utf-8"))
    finally:
        sock.close()


def _upload_estimate(local_path: str, baud: int) -> dict[str, Any]:
    try:
        size = os.path.getsize(local_path)
    except OSError:
        return {"error": "cannot stat local file"}
    effective_bps = max(baud * 0.30, 1.0) / 8
    estimated_seconds = size / effective_bps
    return {
        "file_size": size,
        "effective_bps": int(effective_bps),
        "estimated_seconds": round(estimated_seconds, 1),
    }


def print_human(resp: dict[str, Any]) -> None:
    op = resp.get("operation")
    if op == "status":
        print(f"state: {resp.get('state')}")
        print(f"serial: {resp.get('serial_path')} @ {resp.get('baud')}")
        print(f"job: {resp.get('current_job')}")
        print(f"lock_owner: {resp.get('lock_owner')}")
        print(f"log: {resp.get('log_file')}")
        print(f"raw_log: {resp.get('raw_log_file')}")
        print(f"ring_lines: {resp.get('ring_buffer_line_count')}")
        print(f"uptime: {resp.get('daemon_uptime')}s")
        print(f"last_error: {resp.get('last_error')}")
        return
    if op == "tail":
        for row in resp.get("lines", []):
            print(row.get("text", ""))
        return
    if op == "grep":
        for line in resp.get("matches", []):
            print(line)
        return
    if resp.get("output"):
        print(resp["output"])
    if not resp.get("ok"):
        print(f"error: {resp.get('error')}", file=sys.stderr)
    elif op in {"upload", "reset-usb", "reset-board", "recover", "wait"}:
        bits = [f"ok: {resp.get('ok')}", f"state: {resp.get('state')}"]
        if resp.get("method"):
            bits.append(f"method: {resp.get('method')}")
        if resp.get("sha256"):
            bits.append(f"sha256: {resp.get('sha256')}")
        est = resp.get("estimate")
        if est and "error" not in est:
            bits.append(f"file_size: {est.get('file_size')} bytes")
            bits.append(f"effective_bps: {est.get('effective_bps')}")
            bits.append(f"estimated_time: {est.get('estimated_seconds')}s")
            bits.append(f"note: {est.get('note')}")
        print("\n".join(bits))


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    json_output = False
    if "--json" in argv:
        json_output = True
        argv = [item for item in argv if item != "--json"]
    parser = argparse.ArgumentParser(prog="sbctl")
    parser.add_argument("--socket", default=DEFAULT_SOCKET)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status")
    tail = sub.add_parser("tail")
    tail.add_argument("n", nargs="?", type=int, default=200)
    grep = sub.add_parser("grep")
    grep.add_argument("pattern")
    grep.add_argument("--full", action="store_true")
    wait = sub.add_parser("wait")
    wait.add_argument("pattern")
    wait.add_argument("--timeout", type=float, default=60)
    run = sub.add_parser("run")
    run.add_argument("command")
    run.add_argument("--timeout", type=float, default=30)
    send = sub.add_parser("send")
    send.add_argument("text")
    send.add_argument("--no-newline", action="store_true")
    upload = sub.add_parser("upload")
    upload.add_argument("--method", choices=["zmodem", "base64"], default="zmodem")
    upload.add_argument("--timeout", type=float, default=None)
    upload.add_argument("local")
    upload.add_argument("remote")
    sub.add_parser("reset-board")
    reset_usb = sub.add_parser("reset-usb")
    reset_usb.add_argument("--timeout", type=float, default=20)
    recover = sub.add_parser("recover")
    recover.add_argument("--board", action="store_true")
    recover.add_argument("--usb", action="store_true")
    recover.add_argument("--wait")
    recover.add_argument("--timeout", type=float, default=80)
    sub.add_parser("cancel")
    sub.add_parser("force-unlock")
    monitor = sub.add_parser("monitor")
    monitor.add_argument("n", nargs="?", type=int, default=50)
    monitor.add_argument("--interval", type=float, default=1.0)

    args = parser.parse_args(argv)
    payload: dict[str, Any] = {"op": args.cmd}
    if args.cmd == "tail":
        payload["n"] = args.n
    elif args.cmd == "grep":
        payload.update(pattern=args.pattern, full=args.full)
    elif args.cmd == "wait":
        payload.update(pattern=args.pattern, timeout=args.timeout)
    elif args.cmd == "run":
        payload.update(cmd=args.command, timeout=args.timeout)
    elif args.cmd == "send":
        payload.update(text=args.text, newline=not args.no_newline)
    elif args.cmd == "upload":
        # Print estimate before starting so AI/user knows how long to wait
        try:
            status_resp = request(args.socket, {"op": "status"})
            baud = status_resp.get("baud", 115200)
        except Exception:
            baud = 115200
        est = _upload_estimate(args.local, baud)
        if "error" not in est:
            print(
                f"Upload estimate: {est['file_size']} bytes, effective ~{est['effective_bps']} B/s, "
                f"estimated ~{est['estimated_seconds']}s. DO NOT INTERRUPT.",
                flush=True,
            )
        else:
            print("Upload starting (unable to estimate time). DO NOT INTERRUPT.", flush=True)
        payload.update(method=args.method, local=args.local, remote=args.remote)
        if args.timeout is not None:
            payload["timeout"] = args.timeout
    elif args.cmd == "reset-usb":
        payload["timeout"] = args.timeout
    elif args.cmd == "recover":
        payload.update(board=args.board, usb=args.usb, wait=args.wait, timeout=args.timeout)

    if args.cmd == "monitor":
        return cmd_monitor(args.socket, args.n, args.interval)

    # Determine if this command deserves a heartbeat to show it's alive
    heartbeat = False
    if args.cmd == "upload":
        heartbeat = True
    elif args.cmd in ("run", "wait") and args.timeout >= 15:
        heartbeat = True
    elif args.cmd in ("reset-board", "recover"):
        heartbeat = True

    try:
        resp = request(args.socket, payload, heartbeat=heartbeat)
    except FileNotFoundError:
        print(f"error: socket not found: {args.socket}", file=sys.stderr)
        return 2
    except ConnectionRefusedError:
        print(f"error: cannot connect to broker socket: {args.socket}", file=sys.stderr)
        return 2
    if json_output:
        print(json.dumps(resp, ensure_ascii=False, indent=2))
    else:
        print_human(resp)
    return 0 if resp.get("ok") else 1


def cmd_monitor(socket_path: str, n: int, interval: float) -> int:
    last_seq = 0
    last_state = None
    try:
        while True:
            status = request(socket_path, {"op": "status"})
            tail = request(socket_path, {"op": "tail", "n": n})
            state = status.get("state")
            if state != last_state:
                uptime = status.get("daemon_uptime", "?")
                ring = status.get("ring_buffer_line_count", "?")
                print(f"--- state: {state} | uptime: {uptime}s | ring: {ring} ---")
                last_state = state
            lines = tail.get("lines", [])
            new_lines = [line for line in lines if line.get("seq", 0) > last_seq]
            if new_lines:
                for row in new_lines:
                    print(row.get("text", ""))
                last_seq = new_lines[-1].get("seq", last_seq)
            elif last_seq == 0 and lines:
                for row in lines:
                    print(row.get("text", ""))
                last_seq = lines[-1].get("seq", 0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nmonitor stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
