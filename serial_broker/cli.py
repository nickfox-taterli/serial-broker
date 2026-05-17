from __future__ import annotations

import argparse
import json
import socket
import sys
from typing import Any

from .protocol import DEFAULT_SOCKET, recv_json_line, send_json_line


def request(socket_path: str, payload: dict[str, Any]) -> dict[str, Any]:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(socket_path)
        send_json_line(sock, payload)
        return recv_json_line(sock.makefile("rb"))
    finally:
        sock.close()


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
        payload.update(method=args.method, local=args.local, remote=args.remote)
        if args.timeout is not None:
            payload["timeout"] = args.timeout
    elif args.cmd == "reset-usb":
        payload["timeout"] = args.timeout
    elif args.cmd == "recover":
        payload.update(board=args.board, usb=args.usb, wait=args.wait, timeout=args.timeout)

    try:
        resp = request(args.socket, payload)
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


if __name__ == "__main__":
    raise SystemExit(main())
