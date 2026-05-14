from __future__ import annotations

import json
import socket
from typing import Any


DEFAULT_SOCKET = "/run/serial-broker.sock"


def send_json_line(sock: socket.socket, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    sock.sendall(data + b"\n")


def recv_json_line(sock_file) -> dict[str, Any]:
    line = sock_file.readline()
    if not line:
        raise EOFError("socket closed")
    return json.loads(line.decode("utf-8"))


def result(
    *,
    ok: bool,
    operation: str,
    state: str,
    error: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": ok,
        "operation": operation,
        "state": state,
        "error": error,
    }
    payload.update(extra)
    return payload
