# serial-broker

`serial-broker` 是我为了解决非常麻烦的无网络Linux板子AI自动化问题做的工具,该工具大约有70%代码由AI写,并且没有充分review,因此可能有各种问题.

- `serial-broker`:daemon,由人类启动和看护,独占串口,持续记录日志,处理串口事务.
- `sbctl`:CLI,给 AI/Coding Agent 调用,每次执行一个明确动作.

本项目同时提供了一个 Skill.

这个 skill 的核心规则是:AI 只调用 `sbctl`,不要启动,停止或直接操作 `serial-broker`,也不要直接打开 `/dev/ttyACM*` 或 `/dev/serial/by-id/*`.** `serial-broker` 由人类负责启动,观察终端,并在需要人工复位时确认.

## 安装

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

如果要使用 zmodem 上传,目标板和宿主机都要安装rz.

```bash
apt install lrzsz
```

## 启动 daemon

推荐使用稳定的 `/dev/serial/by-id/...` 路径.USB reset 后 `/dev/ttyACM1` 这类编号可能变化.

```bash
./serial-broker \
  --serial /dev/serial/by-id/usb-xxx \
  --baud 115200 \
  --socket ./broker.sock \
  --log-dir ./logs
```

日志会写入:

```text
logs/current.raw
logs/current.txt
logs/serial-YYYY-MM-DD.raw
logs/serial-YYYY-MM-DD.txt
```

## sbctl 示例

```bash
./sbctl --socket ./broker.sock status
./sbctl --socket ./broker.sock status --json

./sbctl --socket ./broker.sock tail 200
./sbctl --socket ./broker.sock grep "Oops"

./sbctl --socket ./broker.sock wait "login:" --timeout 60
./sbctl --socket ./broker.sock send "root"
./sbctl --socket ./broker.sock send ""

./sbctl --socket ./broker.sock run "uname -a" --timeout 10
./sbctl --socket ./broker.sock run "dmesg | tail -100" --timeout 10 --json

./sbctl --socket ./broker.sock upload ./test.ko /tmp/test.ko
./sbctl --socket ./broker.sock upload --method zmodem ./test.ko /tmp/test.ko
./sbctl --socket ./broker.sock upload --method base64 ./small.sh /tmp/small.sh

./sbctl --socket ./broker.sock reset-board
./sbctl --socket ./broker.sock reset-usb
./sbctl --socket ./broker.sock recover --board --usb --wait "login:" --timeout 80

./sbctl --socket ./broker.sock cancel
./sbctl --socket ./broker.sock force-unlock
```

## 上传说明

默认上传方式是 zmodem.传输期间 daemon 会在 Host `sz` 和串口 fd 之间做 raw byte bridge,并在 text log 中记录上传事件.

如果 zmodem 在某些 USB 串口适配器或目标板状态下失败,使用 base64 fallback:

```bash
./sbctl --socket ./broker.sock upload --method base64 ./file /tmp/file
```

base64 上传较慢,但更容易调试,适合小脚本和配置文件.

## reset-board 和 reset-usb

`reset-usb` 复位的是 Host 侧 USB 串口设备,不是目标板 CPU.它使用 `USBDEVFS_RESET`,可能需要 root 或 `/dev/bus/usb/*` 的 udev 权限.

`reset-board` 用于提醒人类按目标板复位键.它会循环播放声音和提示.
