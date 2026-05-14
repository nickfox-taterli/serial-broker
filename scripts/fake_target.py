#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import pty
import select
import subprocess
import sys
import tempfile
import time
import tty


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", default=None)
    args = parser.parse_args()
    master, slave = pty.openpty()
    tty.setraw(slave)
    slave_name = os.ttyname(slave)
    workdir = args.workdir or tempfile.mkdtemp(prefix="fake-target-")
    shell = subprocess.Popen(
        ["/bin/sh"],
        cwd=workdir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
    )
    print(slave_name, flush=True)
    os.write(master, b"Fake Linux target\nfake login: ")
    echo = True
    pending = b""
    last_prompt = time.monotonic()
    try:
        while True:
            fds = [master]
            if shell.stdout:
                fds.append(shell.stdout.fileno())
            readable, _, _ = select.select(fds, [], [], 0.1)
            if time.monotonic() - last_prompt > 0.5:
                os.write(master, b"fake login: ")
                last_prompt = time.monotonic()
            if master in readable:
                data = os.read(master, 4096)
                if not data:
                    break
                if echo:
                    os.write(master, data.replace(b"\n", b"\r\n"))
                pending += data
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    stripped = line.strip()
                    if stripped.startswith(b"stty -echo"):
                        echo = False
                        continue
                    if stripped.startswith(b"stty echo"):
                        echo = True
                        continue
                    if shell.stdin:
                        shell.stdin.write(line + b"\n")
                        shell.stdin.flush()
            if shell.stdout and shell.stdout.fileno() in readable:
                out = os.read(shell.stdout.fileno(), 4096)
                if out:
                    os.write(master, out.replace(b"\n", b"\r\n"))
                elif shell.poll() is not None:
                    break
            if shell.poll() is not None:
                break
    except KeyboardInterrupt:
        pass
    finally:
        try:
            shell.terminate()
        except Exception:
            pass
        os.close(master)
        os.close(slave)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
