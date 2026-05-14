from __future__ import annotations

import fcntl
import os
import time
from pathlib import Path


USBDEVFS_RESET = ord("U") << (4 * 2) | 20


def canonical_tty(path: str) -> str:
    return os.path.realpath(path)


def find_usb_busdev_for_tty(serial_path: str) -> tuple[int, int] | None:
    tty = Path(canonical_tty(serial_path)).name
    sys_path = Path("/sys/class/tty") / tty / "device"
    try:
        cur = sys_path.resolve()
    except FileNotFoundError:
        return None
    for parent in [cur, *cur.parents]:
        busnum = parent / "busnum"
        devnum = parent / "devnum"
        if busnum.exists() and devnum.exists():
            return int(busnum.read_text().strip()), int(devnum.read_text().strip())
    return None


def reset_usb_device(serial_path: str) -> None:
    busdev = find_usb_busdev_for_tty(serial_path)
    if not busdev:
        raise RuntimeError(f"cannot find USB device for {serial_path}")
    bus, dev = busdev
    node = f"/dev/bus/usb/{bus:03d}/{dev:03d}"
    with open(node, "wb", buffering=0) as fp:
        fcntl.ioctl(fp, USBDEVFS_RESET, 0)


def wait_for_path(path: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.2)
    return False
