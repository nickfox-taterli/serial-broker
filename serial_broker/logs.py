from __future__ import annotations

import codecs
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07]*(?:\x07|\x1b\\)")


def _strip_ansi(line: str) -> str:
    return _ANSI_ESCAPE.sub("", line)


@dataclass
class LogLine:
    seq: int
    ts: float
    text: str


class SerialLogs:
    def __init__(self, log_dir: str, ring_lines: int = 2000) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y-%m-%d")
        self.raw_path = self.log_dir / f"serial-{stamp}.raw"
        self.text_path = self.log_dir / f"serial-{stamp}.txt"
        self.current_raw = self.log_dir / "current.raw"
        self.current_text = self.log_dir / "current.txt"
        self._raw = self.raw_path.open("ab", buffering=0)
        self._text = self.text_path.open("a", encoding="utf-8", buffering=1)
        self._cur_raw = self.current_raw.open("ab", buffering=0)
        self._cur_text = self.current_text.open("a", encoding="utf-8", buffering=1)
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")
        self._partial = ""
        self._ring: deque[LogLine] = deque(maxlen=ring_lines)
        self._seq = 0
        self._raw_offset = self.raw_path.stat().st_size if self.raw_path.exists() else 0
        self._cond = threading.Condition()

    @property
    def raw_offset(self) -> int:
        with self._cond:
            return self._raw_offset

    @property
    def seq(self) -> int:
        with self._cond:
            return self._seq

    @property
    def line_count(self) -> int:
        with self._cond:
            return len(self._ring)

    def close(self) -> None:
        for fp in (self._raw, self._text, self._cur_raw, self._cur_text):
            try:
                fp.close()
            except Exception:
                pass

    def write_raw(self, data: bytes, *, text: bool = True) -> None:
        if not data:
            return
        with self._cond:
            self._raw.write(data)
            self._cur_raw.write(data)
            self._raw_offset += len(data)
            if text:
                decoded = self._decoder.decode(data)
                self._append_decoded_locked(decoded)
            self._cond.notify_all()

    def event(self, text: str) -> None:
        with self._cond:
            self._append_line_locked(text)
            self._cond.notify_all()

    def _append_decoded_locked(self, decoded: str) -> None:
        self._partial += decoded.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._partial:
            line, self._partial = self._partial.split("\n", 1)
            self._append_line_locked(line)

    def _append_line_locked(self, line: str) -> None:
        self._seq += 1
        ts = time.time()
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        millis = int((ts % 1) * 1000)
        row = f"{stamp}.{millis:03d} {line}\n"
        self._text.write(row)
        self._cur_text.write(row)
        self._ring.append(LogLine(self._seq, ts, line))

    def tail(self, n: int) -> list[dict]:
        with self._cond:
            rows = [r for r in self._ring if not self._is_marker_noise(r.text)]
            rows = rows[-max(0, n) :]
            return [{"seq": r.seq, "ts": r.ts, "text": _strip_ansi(r.text)} for r in rows]

    def _is_marker_noise(self, text: str) -> bool:
        return bool(re.search(r"__SB_(?:BEGIN|END)_[0-9a-fA-F]+__(?::\d+)?", text))

    def grep(self, pattern: str, *, full: bool = False) -> list[str]:
        if full and self.text_path.exists():
            matches: list[str] = []
            with self.text_path.open("r", encoding="utf-8", errors="replace") as fp:
                for line in fp:
                    if pattern in line:
                        matches.append(line.rstrip("\n"))
            return matches
        with self._cond:
            return [_strip_ansi(r.text) for r in self._ring if pattern in r.text]

    def text_since(self, seq: int) -> str:
        with self._cond:
            text = "\n".join(r.text for r in self._ring if r.seq > seq)
            if self._partial:
                text = f"{text}\n{self._partial}" if text else self._partial
            return text

    def wait_for(self, pattern: str, seq: int, timeout: float, cancel) -> tuple[bool, str]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                text = "\n".join(r.text for r in self._ring if r.seq > seq)
                if self._partial:
                    text = f"{text}\n{self._partial}" if text else self._partial
                if pattern in text:
                    return True, text
                if cancel.is_set():
                    return False, text
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False, text
                self._cond.wait(min(0.2, remaining))


def ensure_socket_removed(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
